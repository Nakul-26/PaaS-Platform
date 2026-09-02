# Phase 5 — Networking + Ingress: Detailed Task Plan

Parent scope: `ARCHITECTURE.md` §10 (Phase 5 entry), §2.5 (Load Balancer / Ingress — TLS termination, host/path routing), `database-schema.md` §2 (`domains`, migration 0009 slot) and §4, `docs/modularity-and-extensibility.md` (the `CertProvider` seam, already fixed at `services/loadbalancer/internal/tls`), `docs/rbac-multitenancy.md` §2 (the already-defined `domains.manage` permission row), `docs/api-conventions.md`. This doc breaks that scope into implementable, orderable tasks with acceptance criteria, mirroring `docs/phases/phase-2-multi-node.md` through `phase-4-service-discovery-lb.md`'s format.

Exit criteria for the whole phase (restated from `ARCHITECTURE.md` §10):

```text
two different applications reachable via two different hostnames through the same load balancer
```

Phase 4 closed the internal service-discovery loop, but its load balancer only routes on the `X-Platform-Service: <dns_name>` header — an internal testing convenience, explicitly documented as a placeholder until "real hostname/path routing is Phase 5's job." No real client sends that header; real traffic arrives with a `Host` header (and, for TLS, SNI) naming a domain the tenant actually owns. Phase 5's job is the two things ARCHITECTURE.md §2.5 still names undone: routing tenant-registered hostnames (and optionally paths) to the right application, and terminating TLS — both without ever touching Postgres or the API server on the request hot path (§2.5, carried forward from Phase 4).

The `domains` table itself is not new design — `database-schema.md` §2/§4 already specifies it (`id, project_id fk, hostname unique, application_id fk→applications, tls_status enum(pending|active|failed), created_at`) at migration slot `0009_domains.sql`, and `docs/rbac-multitenancy.md` §2 already lists `domains.manage` in the permission matrix. Phase 5 is building the thing those docs already reserved room for, not inventing new schema/permissions from scratch.

## Task sequence

Tasks are ordered by dependency — each assumes the ones above it exist.

### 1. DB: `domains` migration + repository
- `0009_domains.sql` per `database-schema.md` §2: `domains(id pk, org_id, project_id fk, hostname unique, application_id fk→applications, tls_status enum(pending|active|failed), created_at)`. `org_id` is denormalized onto the table (not in the original ERD sketch's column list, but required by `database-schema.md` §3's own rule: "denormalize `org_id` onto every tenant-scoped table" — `domains` is tenant-scoped exactly like `applications`/`projects`) so RLS stays a flat check, same two-branch policy shape (`database-schema.md` §3) as `applications`.
- `DomainRepository` in `internal/db` (ADR-0011): `Create(orgID, projectID, applicationID, hostname) (Domain, error)` (unique-violation on `hostname` mapped to `ErrConflict`, matching `ProjectRepository.Create`'s own pattern), `ListByProject(projectID)`, `Delete(id)`, `OrgID(id)` (deep-by-ID resolution, mirrors `ProjectRepository.OrgID` for `api-conventions.md` §2's addressing rule) — all RLS-bound, run over the `platform_app` connection exactly like every other tenant-CRUD repository.
- Unlike `services`/`service_instances` (Phase 4), `domains` **does** carry RLS (it's tenant-authored config, not cluster-observed runtime state), which creates a real gap: the load balancer's resync (Task 4) needs to read domains across every tenant with no per-request session, but it currently only holds the RLS-scoped `platform_app` connection (a deliberate Phase 4 choice). Resolved the same way `internal/db.ReconcileRepository` already resolves this for controller-manager: a second, narrowly-scoped repository — `DomainRoutingRepository` — built against a second, superuser-role connection the load balancer now also holds, used *only* for this one cross-tenant query. `platform_app` stays untouched for everything else the load balancer already does (services/service_instances resync). Package location: `internal/db` alongside `DomainRepository`, doc-commented the same way `ReconcileRepository` explains its own RLS-bypass need.
- Acceptance: repository-level integration tests against real Postgres, including the duplicate-hostname conflict case and an application-in-a-different-project rejection (FK alone won't catch that — needs an explicit check, either a composite FK/trigger or an application-layer validation in the API handler, Task 2).

### 2. API: CRUD routes for domains
- Add `PermDomainsManage Permission = "domains.manage"` to `internal/auth/rbac.go`'s matrix (`{RoleOwner: true, RoleAdmin: true, RoleDeveloper: true, RoleViewer: false}`, exactly `rbac-multitenancy.md` §2's existing row — the permission was already specified, just not wired to any route yet).
- `POST /v1/projects/:projectId/domains` `{hostname, application_id}` — `requirePermission(..., auth.PermDomainsManage)`; validates `application_id` actually belongs to `projectId` before insert (404 if not, same not-leaking-existence posture as everywhere else); `tls_status` set to `active` synchronously at creation (open decision 3 below).
- `GET /v1/projects/:projectId/domains` — `requireMembership` only (read-only, same posture `logs.view`/`application` reads already use — every role including `viewer` can see what's registered).
- `DELETE /v1/domains/:domainId` — `requirePermission(..., auth.PermDomainsManage)`, org resolved via `DomainRepository.OrgID` (api-conventions.md §2's deep-by-ID pattern).
- Acceptance: integration tests covering the full CRUD path, the cross-tenant denial case (404, not 403 — rbac-multitenancy.md §5), the duplicate-hostname 409, and a permission-matrix check (`developer` allowed, `viewer` denied).

### 3. CLI: surface domain management
- `platform create domain <hostname> --app <app-name>`, `platform get domains`, `platform delete domain <hostname>` — mirrors `create_project.go`/`deploy.go`'s existing conventions, resolving `<app-name>` to an ID via `cfg.Applications` the same way `deploy`/`scale` already do.
- Acceptance: manual/integration check against a real apiserver.

*(Can run in parallel with Task 4 — no dependency between them; both depend only on Task 1/2.)*

### 4. `services/loadbalancer`: real Host-header routing
- Extend the existing periodic resync (Phase 4 Task 4) to also pull a `hostname → dns_name` mapping (join `domains` + `services` on `application_id`) into the registry — reusing the same resync ticker and interval, no new NATS subject (open decision 2 below). This join runs over `DomainRoutingRepository` (Task 1) against the load balancer's new second, superuser-role connection (`LOADBALANCER_ADMIN_DATABASE_URL` — naming mirrors controller-manager's plain `DATABASE_URL` for the same concept); the existing `APP_DATABASE_URL`/`platform_app` connection is untouched and keeps doing Phase 4's services/service_instances resync exactly as before.
- `proxy.ServeHTTP` matches primarily on `r.Host` (port suffix stripped) against the resolved map; falls back to the existing `X-Platform-Service` header when the Host isn't a registered domain, so Phase 4's own integration tests and this internal testing path keep working unchanged. No match on either → `404`.
- Acceptance: integration test — two applications, two registered domains (no path prefix — the exit criteria only requires hostname-based routing, per `ARCHITECTURE.md` §10's literal wording), one load balancer instance, confirm each `Host` header reaches its own application's backend and an unregistered `Host` gets `404`.

### 5. TLS termination
- `CertProvider` interface + a self-signed adapter in `services/loadbalancer/internal/tls` — package path and interface name are already fixed by `docs/modularity-and-extensibility.md`'s registry row (`Manual/self-managed` is the named Phase 5 adapter; real ACME/Let's Encrypt/Cloudflare are documented future alternatives, not built now).
- Load balancer gains a second listener (`LOADBALANCER_TLS_LISTEN_ADDR`) whose `tls.Config.GetCertificate` resolves the SNI hostname through `CertProvider`, generating (and caching in memory) a self-signed cert on first use per hostname — never touches Postgres on the request hot path (§2.5), consistent with how the registry itself already works. Plain HTTP keeps working unchanged alongside it (open decision 4 below — no forced redirect).
- Acceptance: integration test — a TLS client dialing the HTTPS listener with `InsecureSkipVerify` (the only honest way to test a self-signed cert, and exactly what a real client would need to do against one) and a registered hostname's SNI reaches the right backend.

*(Depends on Task 4's routing map; independent of Task 3.)*

### 6. End-to-end test automation
- Automate this phase's exit-criteria script as `e2e/phase5_test.go` (`-tags=e2e`), following the exact real-binaries-as-real-OS-processes pattern `e2e/phase1_test.go` through `e2e/phase4_test.go` established.
- Script: deploy two applications → register one domain per application → send requests through the load balancer with each distinct `Host` header (both plain HTTP and HTTPS with `InsecureSkipVerify`) → confirm each reaches its own application, not the other's.
- Acceptance: this test is what actually certifies Phase 5 done, exactly as Phase 1 Task 8 through Phase 4 Task 7 did for their phases.

## Explicitly deferred out of Phase 5

- **Path-based routing.** `ARCHITECTURE.md` §10 names "host/path-based routing" together, but the exit criteria only exercises hostnames, and a real path implementation needs schema work this doc originally underestimated (a hostname can't stay simply `UNIQUE` once more than one domain row may share it at different path prefixes, and matching needs longest-prefix-wins semantics) — exactly the kind of speculative complexity `modularity-and-extensibility.md` §6 says not to build without a concrete driver. Host-based routing (Task 4) is the actual Phase 5 scope; path-based routing is a follow-up task whenever a real multi-path-per-domain use case shows up.
- **Real ACME/Let's Encrypt/Cloudflare cert issuance** — the self-signed `CertProvider` adapter is the whole Phase 5 TLS deliverable; a real adapter is `modularity-and-extensibility.md`'s documented future alternative, not built now.
- **DNS-level domain-ownership verification** (TXT record challenge, etc.) — Phase 5 trusts whatever hostname a tenant registers; nothing here proves they actually control that DNS name. Needs a real registrar/DNS integration design before it's in scope.
- **Load balancer HA / multiple instances** — R8 (`ARCHITECTURE.md` §9) explicitly defers this to Phase 11; single-instance LB is still the documented posture.
- **Phase 6's full RBAC role-based middleware build-out** — `rbac-multitenancy.md`'s permission matrix is explicitly flagged as "the acceptance criteria for the Phase 6 RBAC middleware." Phase 5 only adds the one already-specified `domains.manage` row to the existing (already-shipped, Phase 1) `requirePermission` mechanism — it isn't building new RBAC infrastructure.
- **Wildcard domains, apex/`www` redirect helpers, and any DNS-provider integration** — not named anywhere in `ARCHITECTURE.md` §10's Phase 5 scope; a tenant provisions DNS for their own hostname to point at the platform's load balancer entirely outside this system.

## Open implementation decisions (resolved here, confirm before coding)

1. **Cross-tenant read for the load balancer's domain resync**: `domains` carries RLS (it's tenant-authored config), but the load balancer's resync needs every tenant's domains with no per-request session. Resolved as a second, narrowly-scoped superuser connection (`DomainRoutingRepository`, Task 1/4) — mirrors `internal/db.ReconcileRepository`'s already-established pattern for controller-manager exactly, rather than widening the load balancer's existing `platform_app` connection's privileges or dropping RLS from `domains` itself.
2. **Hostname→service propagation**: periodic resync only (reusing Phase 4 Task 4's existing `LOADBALANCER_RESYNC_INTERVAL` ticker), no new `domain.updated` NATS subject. Domain registration is an infrequent, administrative action (unlike container churn, which is what justified `service.updated`'s push path in Phase 4) — the existing resync-fallback machinery is sufficient and consistent with §2.6's own "pub/sub is an optimization, never the sole source of truth" reasoning applied to a case that doesn't even need the optimization yet.
3. **`tls_status` lifecycle**: set to `active` synchronously in the API handler at creation time, not reconciled asynchronously. The self-signed adapter's cert generation is deterministic and effectively instantaneous — there's no real latency or failure mode to model until a real ACME adapter (with real network calls and real failure modes) exists. Revisit this the moment that adapter is built.
4. **No forced HTTPS**: the load balancer serves plain HTTP and TLS side by side indefinitely; it does not redirect or reject HTTP once a domain has an active cert. Not named in the exit criteria, and forcing it is a product decision (some tenants may want plain HTTP for internal/dev domains) better made with real usage data, not guessed now.

---

**Next step:** review this task plan (or flag changes) before any Phase 5 code is written, per the same working agreement Phases 1-4 followed. Once approved, flip Phase 5 to 🟨 in [`ROADMAP.md`](../../ROADMAP.md) and start from Task 1.

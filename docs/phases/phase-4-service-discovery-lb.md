# Phase 4 — Service Discovery + Load Balancer: Detailed Task Plan

Parent scope: `ARCHITECTURE.md` §10 (Phase 4 entry), §2.5 (Load Balancer / Ingress), §2.6 (Service Registry / Discovery), `database-schema.md` §2 (`services`/`service_instances`, migration 0008) and §4's `0008_services_service_instances.sql` slot, ADR-0012. This doc breaks that scope into implementable, orderable tasks with acceptance criteria, mirroring `docs/phases/phase-2-multi-node.md`/`docs/phases/phase-3-controllers.md`'s format.

Exit criteria for the whole phase (restated from `ARCHITECTURE.md` §10):

```text
scale an app to 3 replicas, send repeated requests through the LB, observe distribution across all 3
stop one replica, confirm it's removed from rotation within the health-check interval
```

Phase 3 closed the desired-vs-actual replica loop but built nothing that lets a client reach a replica without knowing its node/port directly — `services`/`service_instances` don't exist yet (`database-schema.md` §4 lists them at migration 0008, still unapplied), and `services/loadbalancer/main.go` is still the Phase 0 stub. Phase 4's job is the two PATTERN sections §2.5/§2.6 describe: a durable registry of "which instances are healthy for this application" and a reverse proxy that reads only that registry — never Postgres or the API server on the request hot path (§2.5) — to round-robin across them.

Two gaps this phase's work surfaces and must close along the way, not deferred:
- **No reachable address exists for a running container today.** `ContainerRuntime.ContainerStatus`/`ContainerStatusInfo` (`internal/runtime/runtime.go`) reports only ID/status/exit code — never the actual Docker-assigned host port, even though `docker.go`'s `ContainerInspect` call already has it in `NetworkSettings`. Without it, `service_instances.port` has nothing real to store. Task 2 below closes this.
- **Nothing currently turns a `containers` row into a `service_instances` row.** The scheduler writes `containers` from `node.<id>.status`; nothing downstream of that projects "this deployment has these reachable instances." Task 3 closes this as a new controller-manager reconcile loop — the pattern §2.3 explicitly names ("one loop per resource type"), not a bolt-on to the scheduler or a special case in the API server.

## Task sequence

Tasks are ordered by dependency — each assumes the ones above it exist. Parallelizable pairs are noted.

### 1. DB: `services` / `service_instances` migration + repositories
- `0008_services_service_instances.sql` per `database-schema.md` §2: `services(id pk, application_id fk, dns_name unique, created_at)`, `service_instances(id pk, service_id fk, container_id fk, ip, port, healthy boolean, last_seen_at)`, index on `service_instances(service_id, healthy)` (the load balancer's resync query).
- No RLS, matching `nodes`/`containers`' Phase 2 precedent — this is cluster runtime state, not tenant-scoped, and the load balancer's hot path can't depend on an RLS session being set up.
- `ServiceRepository`/`ServiceInstanceRepository` in `internal/db`, following the existing interface-not-raw-SQL convention (ADR-0011): create-or-get service by `application_id`, upsert instance, list healthy instances by `service_id`, mark instance unhealthy/delete by `container_id`.
- Acceptance: repository-level integration tests against real Postgres.

### 2. `internal/runtime` + worker: surface the actual bound host port
- Extend `ContainerStatusInfo` (`internal/runtime/runtime.go`) with the container's actual port bindings, populated in `DockerRuntime.ContainerStatus` from the same `ContainerInspect` call's `NetworkSettings` it already makes — no new Docker API call.
- Extend `node.<id>.status`'s payload (`docs/nats-contract.md`) with the resolved port(s) so a consumer downstream of the worker can learn the real reachable address without querying Docker itself — `nodes.ip` (already captured at `node.<id>.register`, Phase 2 Task 3) plus this port is the full address.
- Acceptance: integration test starts a real container with `host_port: 0` (ephemeral), confirms the status message/`ContainerStatus` call reports the actual assigned port, not 0.

*(Can run in parallel with Task 1 — no dependency between them.)*

### 3. `controller-manager`: Service Instance controller (new reconcile loop)
- A second loop alongside Task 4's-from-Phase-3 Deployment controller, per §2.3's "one loop per resource type" — same binary, same single-instance-only posture (R3), no new service.
- On each tick: join `containers`+`nodes` for every running deployment, lazily create a `services` row on an application's first healthy instance (`dns_name` format — see "Open implementation decisions" below), and upsert/remove `service_instances` rows to match (`healthy = true` for `running` containers, removed/`healthy = false` otherwise), using Task 2's reported port and the container's node's `ip`.
- Publishes `service.updated` (core NATS, not JetStream — §2.6's own reasoning: "never trust pub/sub alone for critical state, always pair with reconciliation," and Task 4's periodic full resync is that pairing, so losing an occasional event is acceptable, unlike `node.<id>.assign`'s JetStream guarantee).
- Acceptance: integration test against real Postgres + NATS + a real worker: deploy at 3 replicas, confirm 3 `service_instances` rows appear healthy; stop one (Phase 3's unassign path), confirm its row is marked unhealthy/removed and a `service.updated` event fires.

### 4. `services/loadbalancer`: real reverse proxy + in-memory registry
- Replace the stub with §2.5's shape: in-memory registry cache, populated via NATS `service.updated` subscribe (Task 3) plus a periodic full resync pull from Postgres (`ServiceInstanceRepository`, Task 1) as the fallback if an event is missed — the registry, not Postgres or the API server, is what the request hot path reads.
- Round-robin strategy behind a small pluggable interface (§2.5: "round robin first, pluggable strategy interface for weighted/least-connections later" — only round-robin needs implementing now, the interface just needs to leave room).
- Reverse-proxies matched requests to a healthy instance's `ip:port`.
- Acceptance: integration test against real Postgres + NATS + ≥3 real backend containers: confirm repeated proxied requests are distributed across all registered healthy instances.

*(Depends on Tasks 1 and 3; can start once Task 3's registry is populated, in parallel with Task 5.)*

### 5. Health-aware backend removal
- TTL-based instance expiry (§2.6): an instance not refreshed within a configurable window ages out of the load balancer's in-memory rotation even before a node-health controller would ever declare the node itself dead — this is what the exit criteria's "within the health-check interval" literally names, and it's satisfied primarily by Task 3's reconcile tick already removing/marking-unhealthy a stopped instance, refreshed here with an explicit TTL rather than relying solely on the next scheduled tick.
- Passive removal in the load balancer itself (§2.5): a backend that errors on a proxied request is temporarily ejected from rotation, independent of what the registry says, so a single bad request doesn't have to wait for the next reconcile tick or registry push.
- Active health checks (§2.5): the load balancer periodically probes each instance directly (configurable interval/path), the "is the app actually responding" layer that's independent of container-process-alive (Phase 3's concern) — this is the piece that would eventually consume `applications.health_check_path` (flagged, unused, in `phase-3-controllers.md`'s deferred list).
- Acceptance: integration test stops a replica's container process directly (not via scale-down) and confirms the load balancer stops routing to it within one health-check interval, without waiting on Task 3's next full tick.

*(Depends on Task 4. Active health checks can be built in parallel with passive/TTL — independent code paths within the same service.)*

### 6. API + CLI: surface service endpoint
- The exit-criteria demo needs somewhere to actually send requests. Extend `get deployments` (or a new `platform get services`) to show the service's `dns_name`/load-balancer-reachable address, mirroring Task 7 from Phase 3's precedent of extending the read path rather than inventing a parallel command where it isn't needed.
- Acceptance: manual/integration check — deploy an app, confirm the CLI surfaces an address that actually reaches it through the load balancer.

*(Depends on Tasks 3 and 4 only — can run in parallel with Task 5.)*

### 7. End-to-end test automation
- Automate this phase's exit-criteria script (top of this doc) as `e2e/phase4_test.go` (`-tags=e2e`), following the exact real-binaries-as-real-OS-processes pattern established by `e2e/phase1_test.go` through `e2e/phase3_test.go` — now also starting `loadbalancer` as a real process alongside `apiserver`/`scheduler`/`worker`/`controller-manager`/`platform`.
- Script: deploy at `--replicas 3` (or deploy then `scale --replicas 3`) → poll until `3/3` → send repeated HTTP requests through the load balancer and confirm all 3 backends are hit → `scale --replicas 2` (or stop one replica directly, matching Task 5's acceptance) → confirm subsequent requests never land on the removed instance within one health-check interval.
- Acceptance: this test is what actually certifies Phase 4 done, exactly as Phase 1 Task 8, Phase 2 Task 9, and Phase 3 Task 8 did for their phases.

## Explicitly deferred out of Phase 4

Per `ARCHITECTURE.md` §10 — do not build any of this now, even if it looks small:
- Custom domains, host/path-based routing, TLS termination (Phase 5) — Phase 4's load balancer routes by the internal `dns_name` only (see open decision 1 below), not a real external hostname.
- Weighted / least-connections load-balancing strategies — §2.5 explicitly names these as "later," round-robin is the whole Phase 4 deliverable.
- Leader election for multiple `controller-manager` instances — still the same Phase 2/3 single-instance constraint (R3); Task 3's new reconcile loop lives in the same single instance, it doesn't change this posture. (The load balancer itself is stateless/read-only and needs no such election — multiple instances are fine from day one.)
- `applications.health_check_path`-driven automatic rollback (Phase 8) — Task 5's active health checks *read* that field once it's wired up, but reacting to a failing check by rolling back a revision is out of scope here; Phase 4 only removes a failing instance from LB rotation.

## Open implementation decisions to resolve at the start of coding

1. `services.dns_name` format — needs to be unique per application and stable across redeploys, but doesn't need to be externally resolvable yet (that's Phase 5's job). A `<project-slug>-<app-slug>.internal`-style value, generated once at Task 3's lazy-create time, is the working default; confirm no collision/uniqueness concern before implementing.
2. Exact request-routing key the load balancer matches on for Phase 4 (Task 4) — since real hostnames are Phase 5's concern, the simplest option is an internal routing header (e.g. `X-Platform-Service: <dns_name>`) or a path prefix, purely so the e2e test (Task 7) and any interim manual testing have something deterministic to target. Finalize during Task 4.
3. TTL window for Task 5's instance expiry, and the load balancer's active health-check interval/path default — both placeholder values now (same R9 posture Phases 2/3 applied to their own interval tuning), revisited under real load in Phase 10.
4. Whether Task 3's controller reconcile tick and Task 5's TTL expiry are the same interval or intentionally decoupled (reconcile correctness vs. rotation freshness are different concerns) — decide during Task 3/5 implementation, not up front.

---

**Next step:** review this task plan (or flag changes) before any Phase 4 code is written, per the same working agreement Phases 1-3 followed. Once approved, flip Phase 4 to 🟨 in [`ROADMAP.md`](../../ROADMAP.md) and start from Task 1.

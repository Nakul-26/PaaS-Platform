# Modularity & Extensibility Registry

This is the living, practical companion to ADR-0011 (interface-bound external dependencies) and ADR-0012 (network-only service boundaries). Those ADRs record *why* the project is built this way; this doc tracks *what the current seams actually are* and *how to use them* — update it whenever a new interface or service is added, in the same PR as the code.

The governing goal, stated plainly: swapping a component, or moving a co-deployed service onto its own infrastructure, should be additive and local — never require touching or understanding code outside the thing being changed.

## 1. The two rules, restated

1. **Every external dependency sits behind a small internal interface** (ADR-0011). Exactly one adapter exists until a second one is actually needed.
2. **A service depends only on domain-free shared code and other services' published network contracts — never on another service's internal package tree** (ADR-0012).

## 2. Interface (port/adapter) registry

| Component | Interface | Package (planned) | Current adapter | Documented future alternatives | Consumers |
|---|---|---|---|---|---|
| Container runtime | `ContainerRuntime` | `internal/runtime` | Docker Engine SDK | containerd, Firecracker/gVisor (ADR-0006) | `worker` |
| Event bus (pub/sub + work assignment) | `EventBus` | `internal/eventbus` | NATS (core + JetStream) | Kafka, RabbitMQ, Redis Streams (ADR-0005) | `apiserver`, `scheduler`, `controller-manager`, `worker`, `loadbalancer` |
| Data access, per aggregate | `OrganizationRepository`, `UserRepository`, `MembershipRepository`, `ProjectRepository`, `ApplicationRepository`, `DeploymentRepository`, `RefreshTokenRepository`, `NodeRepository`, `ContainerRepository`, etc. | `internal/db` (one file per aggregate) | `pgx`, hand-written queries (no `sqlc` codegen wired up — not warranted at Phase 1's query volume; revisit if that changes) | A different store for a specific aggregate if one ever outgrows Postgres (e.g. `resource_usage_samples` → Timescale, flagged in `ARCHITECTURE.md` §9) | `apiserver`, `scheduler`, `controller-manager` |
| Scheduling algorithm | `SchedulingStrategy` | `services/scheduler/internal/strategy` | Filter-then-score (`ARCHITECTURE.md` §2.2) | Bin-packing, spread, affinity/anti-affinity aware, quota-weighted | `scheduler` only |
| Load-balancing algorithm | `BalancingStrategy` | `services/loadbalancer/internal/strategy` | Round robin | Weighted round robin, least connections, latency-aware | `loadbalancer` only |
| Auth / identity | `IdentityProvider` | `internal/auth` | Local password + JWT (ADR-0008) | OAuth/OIDC SSO, SAML (enterprise tenants) | `apiserver` |
| Secrets encryption | `SecretsProvider` | `internal/secrets` | Local envelope-encryption key | Cloud KMS, HashiCorp Vault | `apiserver`, `worker` (env var delivery) |
| Image build | `ImageBuilder` | `services/image-builder/internal` | Docker build (Phase 7) | Buildpacks, Nixpacks, Kaniko | `image-builder` only |
| TLS/certificate provisioning | `CertProvider` | `services/loadbalancer/internal/tls` | Manual/self-managed (Phase 5) | ACME/Let's Encrypt, Cloudflare | `loadbalancer` only |
| Metrics export | `MetricsSink` | `internal/metrics` | Prometheus client (Phase 9) | OpenTelemetry, a vendor APM | all services |

Notes:
- This table is the actual scope of ADR-0011 — it is not an invitation to wrap *everything*. A dependency earns a row here because a concrete future alternative is already anticipated somewhere in `ARCHITECTURE.md` or the ADRs; anything else stays a direct call until a real reason to abstract it shows up.
- Interfaces are defined at the point of consumption (idiomatic Go), living inside the consuming service's package tree except where a dependency is genuinely shared across multiple services (`ContainerRuntime`, `EventBus`), in which case it lives in `internal/` per ADR-0012's "domain-free shared code" carve-out — an interface definition itself is not domain logic.
- Per-aggregate repositories (not one giant `internal/db` package with every query) so that `apiserver` and `controller-manager` each depend only on the repositories they actually use — this also keeps RLS session-variable handling (ADR-0010) localized to one place per aggregate rather than scattered.

## 3. Service network-contract registry

Per ADR-0012 — what each `services/` binary exposes, and therefore what any other service is allowed to depend on.

| Service | Exposes | Contract doc |
|---|---|---|
| `apiserver` | REST API (`/v1/...`) | `api-conventions.md` |
| `scheduler` | NATS: consumes `placement.requested`, publishes `node.<id>.assign`; consumes `node.<id>.register`/`heartbeat`/`status` | `nats-contract.md` |
| `controller-manager` | No inbound contract (reads desired/actual state, acts); publishes reconciliation events to NATS | *(Phase 3)* |
| `worker` | HTTP: container lifecycle (start/stop/status/logs), called directly by `apiserver` (Phase 1 shape — single worker, no scheduler yet). Phase 2 adds NATS: subscribes to its own assignment subject, publishes registration/heartbeat/status, as an additive transport alongside the HTTP contract, not a replacement. | `worker-agent-contract.md` (HTTP); `nats-contract.md` (NATS) |
| `loadbalancer` | HTTP/WS on tenant-facing ports; reads service registry (Postgres + NATS `service.updated`) | `ARCHITECTURE.md` §2.5–2.6 |
| `image-builder` | NATS or REST trigger (build requested), publishes build-complete events | *(Phase 7)* |

If a service ever needs something from another service that isn't on this list, the fix is to extend that service's published contract — not to reach into its package tree.

## 4. Playbook: swapping an adapter

1. Confirm the dependency has a row in §2 (if not, and there's now a concrete reason to swap it, add the row and the interface first, as its own change).
2. Write the new adapter implementing the existing interface — no changes to the interface itself unless the new adapter reveals the contract was wrong, in which case fix the interface and update all adapters together, deliberately.
3. Wire selection via config (e.g. `RUNTIME=docker|containerd`), not a code branch scattered through business logic.
4. Add adapter-level tests (integration tier, `testing-strategy.md`) for the new adapter; existing unit tests against the interface (using a fake) shouldn't need to change at all — if they do, that's a sign the interface leaked adapter-specific details.

## 5. Playbook: extracting a co-deployed service onto its own infrastructure

Because of ADR-0012, this should require **no code changes** — if it does, that's a bug (a network-boundary violation slipped in somewhere) worth fixing before proceeding, not working around.

1. Build and ship the service's binary/image independently of the others.
2. Point its config at the shared NATS/Postgres endpoints (or its own, if the split is deep enough to warrant separate infra per service — e.g. a dedicated Postgres for `audit_logs` at real scale).
3. Update deployment manifests (`docker-compose.yml` locally, or real orchestration manifests later) to run it separately — new host, new scaling policy, new namespace, whatever the actual driver for extraction is.
4. Verify against that service's published contract (§3) exactly as any other consumer would — this is also a good moment to add or run the E2E test that exercises the contract end-to-end (`testing-strategy.md`).

## 6. When *not* to apply this

- Don't add an interface for a dependency with exactly one plausible implementation and no documented driver for an alternative — that's premature abstraction, and it works against the project's own "no speculative generality" rule. The registry in §2 is deliberately the *complete* list for now; growing it requires a concrete reason, not just "to be safe."
- Don't split a service's deployment (§5) before there's a real operational reason (independent scaling need, a language rewrite, a team boundary) — ADR-0012 guarantees the option is cheap when needed, which is exactly why it doesn't need to be exercised speculatively.

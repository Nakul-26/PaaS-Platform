# Phase 2 — Multi-Node Infrastructure: Detailed Task Plan

Parent scope: `ARCHITECTURE.md` §10 (Phase 2 entry), §2.1-2.4 (API server / scheduler / controller manager / worker agent responsibilities), §9 risks R1/R3/R9, ADR-0005 (NATS), ADR-0009 (local logical-node simulation), ADR-0012 (network-only service boundaries). This doc breaks that scope into implementable, orderable tasks with acceptance criteria, mirroring `docs/phases/phase-1-mvp.md`'s format.

Exit criteria for the whole phase (restated from `ARCHITECTURE.md` §10):

```text
platform deploy demo --image nginx:latest
# scheduler places the deployment on one of 3 running worker processes
platform get nodes                        # shows 3 healthy nodes, one carrying the deployment
docker-compose stop worker-2 (or kill -9)  # simulate node failure
platform get nodes                         # the killed node drops out / shows unreachable
```

Phase 1 built exactly one worker, called directly by the API server over HTTP (`docs/worker-agent-contract.md`) — there was no scheduling decision to make. Phase 2's job is to introduce the pieces that make placement a real decision across multiple nodes: node registration, heartbeats, a real scheduler (filter-then-score, §2.2), and NATS-based work assignment. Per `worker-agent-contract.md`'s own note, this is **additive** — Phase 1's HTTP contract is not retrofitted or removed, NATS is a second transport for the same lifecycle operations.

## Task sequence

Tasks are ordered by dependency — each assumes the ones above it exist. Parallelizable pairs are noted.

### 1. NATS infrastructure + subject/message contract doc
- Add a `nats` service to `infrastructure/docker-compose.yml`.
- Thin NATS client wrapper in `internal/eventing` — connect/publish/subscribe helpers. This is domain-free shared code (config loading and connection setup, no business logic), the same category ADR-0012 already permits in `internal/` alongside `internal/config` and `internal/runtime`.
- Write `docs/nats-contract.md` documenting every subject and its JSON message schema, mirroring how `worker-agent-contract.md` documents the worker's HTTP contract — this is the "published network contract" ADR-0012 requires before any service is allowed to depend on another service's NATS surface. At minimum: `node.<id>.register`, `node.<id>.heartbeat`, `node.<id>.assign`, `node.<id>.status`, and the "needs placement" subject the API server publishes to (Task 6).
- Decide core NATS vs. JetStream per subject here (ADR-0005: lossy-tolerant traffic like heartbeats stays on core NATS; state-change events where loss is unacceptable use JetStream) — a lost placement event would strand a deployment in `pending` forever, so the placement subject is a strong JetStream candidate; heartbeats are the canonical core-NATS case.
- Acceptance: an integration test round-trips a message through a real (testcontainers) NATS instance using the wrapper.

### 2. DB: `nodes` + `containers` migration
- `0007_nodes_containers.sql` per `database-schema.md` §4's already-planned ordering — `nodes` (`id`, `hostname`, `ip`, `cpu_capacity_millicores`, `memory_capacity_mb`, `status enum(healthy|degraded|unreachable)`, `last_heartbeat_at`, `labels jsonb`, `created_at`) and `containers` (`id`, `deployment_id fk`, `node_id fk→nodes`, `container_runtime_id`, `status enum(pending|running|crashed|stopped)`, `restart_count`, `started_at`, `stopped_at`).
- No RLS on either table — both are infrastructure, not tenant-scoped (`database-schema.md`'s `nodes` entry); node visibility is gated at the application layer against a platform-level `owner`/`admin` permission instead.
- `NodeRepository` / `ContainerRepository`, one per aggregate, following the same interface-not-raw-SQL convention Phase 1 Task 5 established (ADR-0011).
- Acceptance: migration applies cleanly to a fresh DB (on top of 0001-0006); repositories covered by an integration test against real Postgres.

*(Can run in parallel with Task 1 — no dependency between them.)*

### 3. Worker: node registration + heartbeat over NATS
- On startup, the worker publishes a registration message (capacity ceiling from its own config — the artificial per-process CPU/mem cap ADR-0009 calls for) and then heartbeats on an interval (configurable — flagged for R9's tuning concern, pick a placeholder default now, e.g. 5s, revisit under real load in Phase 10).
- This is purely additive to the Phase 1 worker: its HTTP surface (`worker-agent-contract.md`) is untouched, the worker just gains a second, NATS-side loop running alongside it.
- Acceptance: integration test starts a real NATS instance + a real worker process, confirms a registration message arrives and heartbeats keep arriving on schedule.

### 4. Worker: NATS assignment subscription
- Worker subscribes to its own `node.<id>.assign` subject; on message, drives the same start/stop container lifecycle it already exposes over HTTP (Phase 1 Task 4, `ContainerRuntime` interface), then publishes a result/status message.
- Acceptance: integration test publishes an assignment message directly (scheduler not involved yet) against a real Docker daemon, confirms the container actually starts, and confirms a status message is published back.

*(Tasks 3 and 4 depend on Task 1 and the existing Phase 1 worker; can run in parallel with Task 2.)*

### 5. Scheduler service (`services/scheduler`, new binary)
- New service under `services/`, per the repo structure in `ARCHITECTURE.md` §5 — imports only `internal/` domain-free packages and other services' published contracts (NATS subjects, DB via its own repository), never another service's package tree (ADR-0012).
- Node registry: consumes the registration/heartbeat messages from Task 3, upserts `nodes` rows, and marks a node `unreachable` once `last_heartbeat_at` exceeds the configurable timeout (paired with the heartbeat interval from Task 3). This satisfies the "kill a worker, watch it drop out of the node list" exit criterion without yet building the full Controller Manager pattern — that binary and its idempotent-reconciliation-loop discipline (§2.3, R2) is Phase 3's job; Phase 2 only needs liveness tracking, not drift repair.
- Placement: subscribes to the "needs placement" subject (Task 1/6), applies the filter-then-score algorithm from `ARCHITECTURE.md` §2.2 — filter out nodes that are unhealthy or lack capacity, score the rest by lowest current load, pick the best — writes the decision to Postgres (`containers` row with `node_id` set), and publishes the assignment onto the winning node's subject.
- Single scheduler instance only for Phase 2. This is a documented constraint (`ARCHITECTURE.md` §9 R3), not solved with leader election yet — running two would risk double-scheduling the same placement.
- Acceptance: integration test against real Postgres + NATS + ≥2 real worker processes confirms a "needs placement" event results in exactly one `containers` row pointing at a healthy node, and that node actually receives the assignment.

### 6. API server: switch deploy path to event-driven placement
- Change the `deploy` route (Phase 1 Task 5) from calling the worker directly over HTTP to: write the `deployments` row, then publish a "needs placement" event — matching `ARCHITECTURE.md` §2.1 ("validates and persists desired state... does not talk to workers directly"). This is the one behavior change to already-shipped Phase 1 code this phase requires.
- New `GET /v1/nodes` route, gated behind the platform-level permission noted in Task 2, returning node id/hostname/status/last_heartbeat_at — without this, placement and node-down transitions aren't observable by any client.
- Extend the deployment read path (`get deployments` or equivalent) to surface which node a deployment landed on, via a join through `containers` — otherwise "watch the scheduler place it" has nothing to watch.
- Acceptance: integration test covering the full create→deploy→(event)→placement→visible-in-reads flow against real Postgres + NATS + scheduler + worker.

### 7. CLI: `platform get nodes`
- Thin REST client command per ADR-0007, mirroring the existing `get deployments` command shape.
- Acceptance: integration/manual check against a running stack lists real node rows with live status.

### 8. Local dev: 3 logical worker nodes
- Extend `infrastructure/docker-compose.yml` to run `worker-1`/`worker-2`/`worker-3`, each with a distinct artificial capacity ceiling via config/env, all pointed at the one host Docker daemon — wiring up the decision ADR-0009 already made, not re-deciding it.
- Acceptance: `docker-compose up` brings up all 3; each registers as a distinct `nodes` row within one heartbeat interval.

### 9. End-to-end test automation
- Automate this phase's exit-criteria script (top of this doc) as `e2e/phase2_test.go` (`-tags=e2e`), following the exact pattern `e2e/phase1_test.go` established: build and run real `apiserver`/`scheduler`/`worker` (×3)/`platform` binaries as separate OS processes, real Postgres + NATS via testcontainers, driven entirely through the compiled CLI binary.
- Script: deploy → poll `get nodes`/`get deployments` until the scheduler has placed it on one of the 3 workers → kill that worker's process → poll `get nodes` until it's reported unreachable/dropped.
- Acceptance: this test is what actually certifies Phase 2 done, exactly as Phase 1 Task 8 certified Phase 1 — running it is the literal exit-criteria check.

## Explicitly deferred out of Phase 2

Per `ARCHITECTURE.md` §10 — do not build any of this now, even if it looks small:
- Controller Manager / reconciliation loops enforcing `replicas_desired`, automatic restart of crashed containers (Phase 3) — Phase 2's scheduler places work once; it does not notice or repair drift after initial placement.
- Leader election / multiple scheduler instances — single-instance is a documented Phase 2 constraint (R3), revisit only once running >1 control-plane instance is actually on the table.
- Load balancer / service discovery (Phase 4/5) — no `services`/`service_instances` tables yet; those are migration 0008 per `database-schema.md` §4.
- Real multi-host resource isolation validation — local dev with 3 logical nodes (Task 8) only proves scheduler/reconciliation *logic*, per ADR-0009 and R1; it explicitly does not prove real isolation. That validation happens against real separate hosts, which is an infrastructure/ops exercise outside this doc's scope, not a code task — flag separately if/when actually provisioning real multi-VPS infrastructure.

## Open implementation decisions to resolve at the start of coding

1. Exact NATS subject naming scheme (`node.<id>.assign` etc. above are working names) — finalize in Task 1's `docs/nats-contract.md`.
2. Core NATS vs. JetStream per subject — leaning JetStream for the placement subject (loss is unacceptable) and core NATS for heartbeats (loss-tolerant by nature), per ADR-0005; confirm during Task 1.
3. Heartbeat interval and unreachable-timeout defaults (Task 3/5) — ship a reasonable placeholder now, revisit for real during Phase 10's failure-injection/load-testing pass (R9 explicitly flags this as "load-test it, don't just guess a constant once").
4. Whether the scheduler's node-registry responsibilities (Task 5) eventually split out of the scheduler binary once Phase 3's Controller Manager lands, or the Controller Manager absorbs them — no need to decide now, just noting the seam exists.

---

**Next step:** review this task plan (or flag changes) before any Phase 2 code is written, per the same working agreement Phase 1 followed. Once approved, flip Phase 2 to 🟨 in [`ROADMAP.md`](../../ROADMAP.md) and start from Task 1.

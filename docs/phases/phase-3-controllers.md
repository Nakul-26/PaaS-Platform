# Phase 3 — Desired State + Controllers: Detailed Task Plan

Parent scope: `ARCHITECTURE.md` §10 (Phase 3 entry), §2.3 (Controller Manager pattern), §9 R2 (idempotent, concurrency-safe reconciliation) and R3's single-instance posture (same constraint, new binary), `database-schema.md`'s `applications.replicas_desired` and `containers.status` columns, `docs/nats-contract.md`, ADR-0012. This doc breaks that scope into implementable, orderable tasks with acceptance criteria, mirroring `docs/phases/phase-2-multi-node.md`'s format.

Exit criteria for the whole phase (restated from `ARCHITECTURE.md` §10):

```text
manually kill a container process (docker kill, not the worker process)
# the controller notices the deployment now has fewer running replicas than desired
# the controller schedules a replacement without any user action
platform get deployments demo   # within one reconcile interval, shows the replacement running
```

Phase 2 built exactly enough machinery to place a workload once: `applications.replicas_desired` exists in the schema (migration 0004) but nothing reads it, and a worker publishes `node.<id>.status` exactly once — right after a container starts (`services/worker/internal/nodeagent/assign.go`'s `handleAssignment`). Nothing watches a container afterward, so a container killed out-of-band is never noticed, and nothing ever requests a second replica. Phase 3's job is to close that loop: detect drift between desired and actual state, and act on it, continuously and idempotently (§2.3's core loop shape).

Two gaps carried over from Phase 2, both closed by this phase's work rather than needing separate tracked tasks:
- **Worker-routing bug** (flagged in `ROADMAP.md`'s Phase 2 Task 9 note): `handleDeleteApplication` stops/removes a container via a single hardcoded `WORKER_ADDR`, regardless of which node actually holds it. Task 2 below introduces a proper "stop this specific container on its actual node" NATS subject for scale-down; Task 6 re-routes deletion through it.
- **Open decision #4** from `phase-2-multi-node.md`: whether the scheduler's node-registry responsibilities eventually move into the Controller Manager. Resolved here: they stay in `services/scheduler` — it's already working, node liveness is a different concern (node health, not replica count) from what this phase's controller owns, and splitting it now has no forcing function. `controller-manager` owns exactly one thing in Phase 3: the Deployment controller.

## Task sequence

Tasks are ordered by dependency — each assumes the ones above it exist. Parallelizable pairs are noted.

### 1. Worker: container health monitoring (crash detection)
- `nodeagent` currently tracks nothing after `handleAssignment` publishes its one status message. Add an in-memory `assignment_id → container_id` map populated on successful assignment, and a poll loop (interval configurable, placeholder default 3s) that calls `ContainerRuntime.ContainerStatus` for each tracked container and republishes `node.<id>.status` (reusing the existing `containerStatus` mapping) whenever the observed status has changed since the last check (`running` → `crashed`/`stopped`).
- Explicitly not in scope: recovering tracked containers after a worker *process* restart (the in-memory map is lost; the container itself keeps running under Docker, untracked until reassigned). Flag as a known gap in `ROADMAP.md`, not fixed here — see "Explicitly deferred" below.
- Acceptance: integration test starts a real worker + real Docker container via an assignment, then kills the container out-of-band (`docker kill`, not the worker process), and confirms a new `node.<id>.status` message with `status: "crashed"` arrives within one poll interval, without any new assignment being sent.

### 2. NATS contract + worker: `node.<id>.unassign` subject
- New subject, documented in `docs/nats-contract.md` alongside the existing four: published to a specific node to stop and remove one specific container by `assignment_id`/`container_id` — the piece that lets anything (controller, apiserver) act on a container wherever it actually runs, instead of a hardcoded worker address.
- Worker subscribes (same durable-consumer pattern as `node.<id>.assign`), calls the existing `ContainerRuntime` stop/remove path, publishes a final `status: "stopped"` message, and stops tracking that assignment (Task 1's map).
- Transport: JetStream, same reasoning as `node.<id>.assign` — a lost unassign leaves a container running that the control plane thinks is gone.
- Acceptance: integration test publishes an unassign message directly against a real worker + Docker daemon, confirms the container is actually stopped/removed, and confirms the resulting status message arrives.

*(Can run in parallel with Task 1 — both extend the worker but touch different code paths.)*

### 3. Repositories: reconciliation queries + scale support
- `ContainerRepository`: add a method to count/list "active" (`pending`+`running`) containers for a given `deployment_id` — the "actual" side of the controller's diff. Follows the existing interface-not-raw-SQL convention (ADR-0011).
- `ApplicationRepository`: add `UpdateReplicas(ctx, id, replicas int) error`, mirroring the existing `UpdateImage` shape — needed by Task 5's scale endpoint.
- No schema migration needed: `containers.deployment_id` already has no uniqueness constraint, so multiple containers per deployment (multiple replicas) already fits the Phase 2 schema as-is.
- Acceptance: repository-level integration tests against real Postgres.

*(Can run in parallel with Tasks 1/2 — no dependency between them.)*

### 4. `controller-manager` service: Deployment reconcile loop
- Replace the `services/controller-manager/main.go` stub with a real service following §2.3's loop shape: on each tick (configurable interval, placeholder default 5s), for every application whose latest deployment is `running`, compare `replicas_desired` (desired) against Task 3's active-container count for that deployment (actual).
  - `actual < desired`: publish `placement.requested` (Phase 2 Task 1's existing subject/contract) for the shortfall — reuses the scheduler's existing filter-then-score placement unchanged, no new placement machinery.
  - `actual > desired`: pick the excess containers deterministically (oldest-created first, kept simplest — document the choice, no need for it to be configurable yet) and publish `node.<id>.unassign` (Task 2) for each.
- Idempotency / concurrency safety (R2): single `controller-manager` instance only for Phase 3, the same documented constraint Phase 2 placed on the scheduler (R3) — no leader election yet. Within that constraint, each tick recomputes actual fresh from Postgres immediately before acting, so an in-flight placement from the previous tick is naturally accounted for by the time it lands; a transient over-request self-corrects on the next tick rather than compounding.
- Acceptance: integration test against real Postgres + NATS + scheduler + ≥1 worker: seed an application/deployment already at steady state (1 replica running, via the existing Phase 2 path), `docker kill` that container directly, confirm Task 1's crash status arrives, then confirm the controller notices `actual(0) < desired(1)` on its next tick, a replacement gets placed and reaches `running` — this is the phase's literal exit-criteria scenario.

### 5. API + CLI: manual scale
- New route (exact shape decided at implementation time — see "Open implementation decisions") to update `applications.replicas_desired`, gated behind the same project-scoped permission `deploy` already uses.
- `platform scale <app> --replicas N` CLI command, thin REST client per ADR-0007, mirroring `deploy`'s command shape.
- Scaling itself does not place or remove anything synchronously — it only updates desired state; Task 4's reconcile loop picks up the change on its next tick, exactly like a crash does. No special-cased code path for "scale" vs. "crash recovery" — both are just the same diff.
- Acceptance: integration test scales an application from 1 to 3 replicas, polls until 3 are running; scales back to 1, polls until the excess 2 are stopped.

### 6. API: re-route `handleDeleteApplication` through per-node unassign
- Fix the Phase 2-flagged gap: replace the single hardcoded `WORKER_ADDR` HTTP call in `handleDeleteApplication` (`services/apiserver/internal/server/handlers_applications.go`) with a `node.<id>.unassign` publish (Task 2) per container, using each container's actual `node_id` from `ContainerRepository.ListByDeployment` (already fetched correctly today — only the dispatch target was wrong).
- Acceptance: extend the existing delete-application integration test to run against ≥2 worker nodes and confirm the container is actually stopped on whichever node it really landed on, not just the one `WORKER_ADDR` happens to point at.

*(Depends on Task 2 only — can run any time after it, in parallel with Tasks 3-5.)*

### 7. API + CLI: surface replica-level state
- Extend the deployment read path (`get deployments` or equivalent) to show a replica summary (e.g. `2/3 running`) instead of assuming one container per deployment, since Phase 2's version assumed exactly one. Without this, scale-up/crash-recovery has nothing observable to poll.
- Acceptance: manual/integration check — deploy at replicas=3, confirm the CLI shows `3/3 running` once the controller (Task 4) has converged.

### 8. End-to-end test automation
- Automate this phase's exit-criteria script (top of this doc) as `e2e/phase3_test.go` (`-tags=e2e`), following the exact real-binaries-as-real-OS-processes pattern `e2e/phase1_test.go` and `e2e/phase2_test.go` established — now also starting `controller-manager` as a real process alongside `apiserver`/`scheduler`/`worker`/`platform`.
- Script: deploy → poll until running → `docker kill` the actual container (not the worker process — that's Phase 2's failure domain, this phase's is a container-level crash) → poll `get deployments` until a replacement is running again, within one reconcile interval.
- Acceptance: this test is what actually certifies Phase 3 done, exactly as Phase 1 Task 8 and Phase 2 Task 9 did for their phases.

## Explicitly deferred out of Phase 3

Per `ARCHITECTURE.md` §10 — do not build any of this now, even if it looks small:
- Leader election / multiple `controller-manager` instances — single-instance is a documented Phase 3 constraint (mirroring R3's posture for the scheduler in Phase 2), revisit only once running >1 control-plane instance is actually on the table.
- Worker-restart recovery: Task 1's crash-detection map is in-memory only and lost on a worker *process* restart, even though the container itself keeps running under Docker, untracked, until reassigned. Re-discovering already-running containers via `docker ps` on worker startup and reconciling them back into tracking is a real gap, flagged here rather than fixed — needs its own task if/when worker restarts become a tested failure scenario (Phase 10 territory).
- Rolling deployments between revisions, automatic rollback on failed health checks (Phase 8) — this phase's controller only maintains replica *count* and restarts process-level crashes; it does not manage transitions between two different revisions of the same application.
- Health-check-based restart: `applications.health_check_path` already exists in the schema but is unused — Phase 3 only reacts to a container process exiting, not to an application that's running but failing its own health check. That's load-bearing for the load balancer's health-aware routing (Phase 4) and automatic rollback (Phase 8); flagged as an open question below, not solved here.
- Load balancer awareness of replica count changes (Phase 4/5) — no `services`/`service_instances` tables yet (`database-schema.md` §4, migration 0008).

## Open implementation decisions to resolve at the start of coding

1. Worker health-monitoring poll interval (Task 1) and controller-manager reconcile tick interval (Task 4) — both get a placeholder default now (3s / 5s), revisited under real load in Phase 10's failure-injection/load-testing pass, same R9 posture Phase 2 applied to heartbeat tuning.
2. Exact scale route shape (Task 5) — `PATCH /v1/applications/:id` with a `replicas_desired` field, vs. a dedicated `POST /v1/applications/:id/scale`. Finalize during Task 5; either is a thin wrapper over the same `UpdateReplicas` repository call.
3. Excess-replica selection on scale-down / over-desired correction (Task 4) — oldest-created-first is the working default; confirm no reason to prefer newest-first or a different signal (e.g. node load) before implementing.
4. Whether `node.<id>.unassign` (Task 2) needs a distinct "graceful stop" vs. "force remove" mode, or one behavior is enough for Phase 3 — Phase 1/2's existing stop/remove path is a single non-graceful call; keep it that way unless a concrete need for graceful shutdown surfaces during implementation.
5. Whether `applications.health_check_path` (deferred above) gets picked up in Phase 4 alongside the load balancer, or needs its own earlier task — no need to decide now, just noting the seam, same as Phase 2's own open-decision #4 did for the node registry.

---

**Next step:** review this task plan (or flag changes) before any Phase 3 code is written, per the same working agreement Phase 1 and Phase 2 followed. Once approved, flip Phase 3 to 🟨 in [`ROADMAP.md`](../../ROADMAP.md) and start from Task 1.

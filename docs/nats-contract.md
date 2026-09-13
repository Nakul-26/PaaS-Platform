# NATS Subject &amp; Message Contract (Phase 2)

This is the published network contract for every NATS subject in the system, the NATS-side counterpart to `worker-agent-contract.md`'s HTTP contract. Per ADR-0012, a service may depend on another service's NATS surface only through what's documented here — never by importing that service's package tree. The transport itself (connect/publish/subscribe) is `internal/eventbus`'s `EventBus` interface (ADR-0005, ADR-0011); this doc covers subjects and payloads, not the client wrapper.

All payloads are JSON; timestamps are RFC 3339 UTC, matching `api-conventions.md` and `worker-agent-contract.md`.

## Transport choice per subject (ADR-0005)

| Subject | Transport | Why |
|---|---|---|
| `node.<id>.register` | Core NATS | Sent once at worker startup; if missed, the next heartbeat re-establishes the node within one heartbeat interval — loss is self-healing, not worth JetStream's overhead. |
| `node.<id>.heartbeat` | Core NATS | Lossy-tolerant by nature (ADR-0005) — a single dropped heartbeat doesn't matter, only a *sustained* gap does, and that's what the unreachable-timeout in Task 5 detects. |
| `placement.requested` | JetStream (stream `PLACEMENT`) | A lost placement request strands a deployment in `pending` forever with nothing to retry it — loss is unacceptable. |
| `node.<id>.assign` | JetStream (stream `NODE_ASSIGNMENTS`) | Same reasoning as above: a lost assignment means the scheduler *thinks* it placed work that never actually started. |
| `node.<id>.unassign` | JetStream (stream `NODE_ASSIGNMENTS`) | Same reasoning as `node.<id>.assign`: a lost unassign leaves a container running that the control plane thinks is gone. |
| `node.<id>.status` | JetStream (stream `NODE_STATUS`) | Status transitions (esp. `running` → `crashed`) are the state-change events ADR-0005 calls out by name; losing one hides a real failure from the control plane until the next reconcile. |
| `build.requested` | JetStream (stream `BUILDS`) | A lost build request strands a `builds` row in `pending` forever with nothing to retry it — the same "unacceptable loss" case `placement.requested` already makes, one step earlier in the same chain (`phase-7-deployment-platform.md` Open Decision 1). |
| `build.completed` | JetStream (stream `BUILDS`) | A lost completion leaves a build stuck in `pending` and, worse, silently drops the deployment that success was supposed to produce — the git-deploy path's entire payoff. |

Every JetStream-backed read path here still needs the Postgres reconcile-fallback R7 requires — NATS being down must degrade placement/status visibility, not corrupt it. That fallback is each subject's consuming service's responsibility (scheduler for `NODE_STATUS`/`PLACEMENT`, worker for `NODE_ASSIGNMENTS`), not this contract's.

## Subjects

### `node.<id>.register`

Published once by a worker on startup, where `<id>` is the worker's own node ID (a UUID it generates and persists locally on first run, so restarts re-register as the same node rather than leaking a new row every restart).

```json
{
  "node_id": "b3f1...",
  "hostname": "worker-1",
  "ip": "172.20.0.3",
  "cpu_capacity_millicores": 2000,
  "memory_capacity_mb": 2048,
  "labels": {}
}
```

Consumed by: `scheduler` (upserts a `nodes` row — `database-schema.md` §2).

### `node.<id>.heartbeat`

Published by a worker on a fixed interval (default 5s — placeholder per R9, revisited under load in Phase 10).

```json
{
  "node_id": "b3f1...",
  "timestamp": "2026-08-18T10:00:00Z"
}
```

Consumed by: `scheduler` (updates `nodes.last_heartbeat_at`; a node is marked `unreachable` once this subject has gone silent for longer than the configurable timeout).

### `placement.requested`

Published by `apiserver` when a deployment needs scheduling — replaces Phase 1's direct HTTP call to the worker (`ARCHITECTURE.md` §2.1: the API server "does not talk to workers directly"). Also published by `controller-manager` (`phase-3-controllers.md` Task 4) once per missing replica whenever a deployment's actual running-container count falls below `applications.replicas_desired` — a crash-recovery or scale-up request looks identical to a deploy-time one; the scheduler can't tell them apart and doesn't need to.

```json
{
  "deployment_id": "d4e2...",
  "application_id": "a1b2...",
  "image": "nginx:latest",
  "env": { "PORT": "8080" },
  "ports": [{ "container_port": 80, "host_port": 0, "protocol": "tcp" }],
  "command": ["nginx", "-g", "daemon off;"]
}
```

Field shapes mirror `worker-agent-contract.md`'s `POST /v1/containers` body, since the scheduler forwards this into a `node.<id>.assign` message largely unchanged. Consumed by: `scheduler` (filter-then-score placement, `ARCHITECTURE.md` §2.2).

### `node.<id>.assign`

Published by `scheduler` to the specific node it placed the work on, after writing the corresponding `containers` row (`node_id` set) to Postgres.

```json
{
  "assignment_id": "f7a9...",
  "deployment_id": "d4e2...",
  "image": "nginx:latest",
  "env": { "PORT": "8080" },
  "ports": [{ "container_port": 80, "host_port": 0, "protocol": "tcp" }],
  "command": ["nginx", "-g", "daemon off;"]
}
```

`assignment_id` correlates this message with the `node.<id>.status` updates it produces. Consumed by: `worker` (drives the same `ContainerRuntime` start path its HTTP `POST /v1/containers` handler already uses).

### `node.<id>.unassign`

Published to the specific node a container actually runs on, to stop and remove that one container — the piece that lets anything (controller, apiserver) act on a container wherever it actually runs, instead of a hardcoded worker address (`phase-3-controllers.md` Task 2). Published by `controller-manager` (Task 4) once per excess replica whenever a deployment's actual running-container count exceeds `applications.replicas_desired`, oldest-started-first.

```json
{
  "assignment_id": "f7a9...",
  "container_id": "a1b2c3..."
}
```

Consumed by: `worker` (drives the same `ContainerRuntime` stop/remove path its HTTP `DELETE`/stop handlers already use, then stops tracking the assignment for health-check polling — Task 1's map). Always publishes a final `node.<id>.status` message with `status: "stopped"`, even if the container was already stopped or gone by the time this runs — an unassign for an already-crashed container still converges to `stopped`, not an error.

### `node.<id>.status`

Published by a worker after acting on a `node.<id>.assign` message, and again on any subsequent status transition it observes for that container (e.g. a later crash).

```json
{
  "assignment_id": "f7a9...",
  "container_id": "a1b2c3...",
  "status": "running",
  "exit_code": 0,
  "ports": [{ "container_port": 80, "host_port": 32768, "protocol": "tcp" }],
  "timestamp": "2026-08-18T10:00:05Z"
}
```

`status` uses the same vocabulary as `containers.status` (`database-schema.md` §2): `pending`, `running`, `crashed`, `stopped`. `ports` reports the runtime's actual assigned host port(s) — resolved even when the corresponding `node.<id>.assign` request's `host_port` was `0` (ephemeral) — and is omitted (not just zero) for states with no bound ports (e.g. `stopped`). Consumed by: `scheduler` (writes the observed status back onto the `containers` row); `ports` itself has no consumer yet — `phase-4-service-discovery-lb.md` Task 3's controller is the first to read it, to populate `service_instances.port`.

### `build.requested`

Published by `apiserver` when a deployment is requested from a Git repo URL instead of an already-pushed image (`phase-7-deployment-platform.md` Task 4), immediately after the `builds` row is created in `pending`.

```json
{
  "build_id": "b3c1...",
  "org_id": "0a9f...",
  "application_id": "a1b2...",
  "git_url": "https://github.com/example/app.git",
  "git_ref": "main",
  "image_repository": "localhost:5000/0a9f.../a1b2..."
}
```

`image_repository` is the push target *without* a tag — `image-builder` appends `:<commit_sha>` itself, since the SHA isn't known until the clone resolves it (`phase-7-deployment-platform.md` Open Decision 3). `org_id` is carried purely so it can be echoed back on `build.completed`; `image-builder` never interprets it. Consumed by: `image-builder` (clone → docker build → push).

### `build.completed`

Published by `image-builder` once a build attempt reaches a terminal outcome — one subject for both success and failure, with a `status` field carrying the branch, mirroring `node.<id>.status`'s existing precedent rather than splitting into two subjects.

```json
{
  "build_id": "b3c1...",
  "org_id": "0a9f...",
  "status": "succeeded",
  "commit_sha": "9f2c1ab...",
  "image": "localhost:5000/0a9f.../a1b2...:9f2c1ab"
}
```

`status` is `succeeded` or `failed`. A success carries `commit_sha`/`image` and no `error`; a failure carries `error` and neither — every failure mode (bad ref, missing `Dockerfile`, build failure, push failure) is reported identically, and there is no partial-success case.

`org_id` is echoed verbatim from the originating `build.requested`. It exists because `builds` carries RLS (`0014_builds.sql`) while a NATS message arrives with no session to scope by, so the consumer needs *some* org to open its transaction with. It is a **hint, not an authority**: every value actually acted on (`application_id`, `created_by`, and the org itself) is read back off the `builds` row, and a wrong or forged `org_id` simply makes RLS return no row for `build_id`, so the message fails closed rather than reaching another tenant's data.

Consumed by: `apiserver` (records the outcome via `BuildRepository.UpdateResult`; on `succeeded`, additionally creates the `deployments` row and publishes `placement.requested` through the same shared helper the HTTP `--image` path uses — `phase-7-deployment-platform.md` Task 4/Open Decision 2).

## Stream definitions

| Stream | Subjects | Retention |
|---|---|---|
| `PLACEMENT` | `placement.requested` | Limits (default JetStream retention — a request is consumed once by the single scheduler instance) |
| `NODE_ASSIGNMENTS` | `node.*.assign`, `node.*.unassign` | Limits |
| `NODE_STATUS` | `node.*.status` | Limits |
| `BUILDS` | `build.requested`, `build.completed` | Limits (both directions of one build's lifecycle share a stream — `phase-7-deployment-platform.md` Open Decision 1) |

Each service that publishes on a JetStream subject is responsible for calling `EventBus.EnsureStream` for it at startup (idempotent — `CreateOrUpdateStream`), so no separate provisioning step is required.

## Open items

Consumer durable-name conventions (e.g. `scheduler-node-registry`, `scheduler-placement`) are assigned per-consumer as each publishing/consuming task (3-6) is implemented, not fixed in advance here.

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
| `node.<id>.status` | JetStream (stream `NODE_STATUS`) | Status transitions (esp. `running` → `crashed`) are the state-change events ADR-0005 calls out by name; losing one hides a real failure from the control plane until the next reconcile. |

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

Published by `apiserver` when a deployment needs scheduling — replaces Phase 1's direct HTTP call to the worker (`ARCHITECTURE.md` §2.1: the API server "does not talk to workers directly").

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

### `node.<id>.status`

Published by a worker after acting on a `node.<id>.assign` message, and again on any subsequent status transition it observes for that container (e.g. a later crash).

```json
{
  "assignment_id": "f7a9...",
  "container_id": "a1b2c3...",
  "status": "running",
  "exit_code": 0,
  "timestamp": "2026-08-18T10:00:05Z"
}
```

`status` uses the same vocabulary as `containers.status` (`database-schema.md` §2): `pending`, `running`, `crashed`, `stopped`. Consumed by: `scheduler` (writes the observed status back onto the `containers` row).

## Stream definitions

| Stream | Subjects | Retention |
|---|---|---|
| `PLACEMENT` | `placement.requested` | Limits (default JetStream retention — a request is consumed once by the single scheduler instance) |
| `NODE_ASSIGNMENTS` | `node.*.assign` | Limits |
| `NODE_STATUS` | `node.*.status` | Limits |

Each service that publishes on a JetStream subject is responsible for calling `EventBus.EnsureStream` for it at startup (idempotent — `CreateOrUpdateStream`), so no separate provisioning step is required.

## Open items

Consumer durable-name conventions (e.g. `scheduler-node-registry`, `scheduler-placement`) are assigned per-consumer as each publishing/consuming task (3-6) is implemented, not fixed in advance here.

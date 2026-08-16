# Local Development Guide

Implements ADR-0009 (logical-node simulation). This doc will gain real commands as Phase 1 scaffolding lands — for now it records the intended shape so Phase 1 setup matches the plan rather than improvising it.

## 1. Prerequisites

- Go (version pinned once `go.mod` is created in Phase 1)
- Docker Engine + Docker Compose
- Node.js + pnpm (dashboard)
- `goose` CLI (migrations, per `database-schema.md` §4)

## 2. What `docker-compose up` brings up

Per `ARCHITECTURE.md` §6:

```text
postgres              # control-plane state
nats                  # eventing (JetStream enabled)
apiserver             # REST API, port 8080
scheduler             # Phase 2+
controller-manager    # Phase 3+
loadbalancer          # Phase 4+
worker-1, worker-2, worker-3   # logical nodes, shared host Docker daemon (ADR-0009)
```

Phase 1 only needs `postgres`, `apiserver`, and a single `worker`. Later services are added to the compose file as their phase introduces them — the compose file's growth should mirror the roadmap in `ROADMAP.md`, not get built out ahead of the phase that needs it.

## 3. Simulating cluster events locally

- **Node failure**: `docker compose stop worker-2` — its heartbeats stop, the node-health controller (Phase 3+) should mark it unreachable and the scheduler should stop placing new work there.
- **Container crash**: exec into a worker and kill a container process directly, or use the CLI/API to intentionally deploy a bad image — the Deployment controller (Phase 3) should detect and restart it.
- **Control-plane outage**: `docker compose stop apiserver scheduler controller-manager` — per ADR-0001, already-running applications should keep serving traffic through the load balancer, which reads only from the service registry.

These map directly to the failure-injection scenarios in `testing-strategy.md` §1 — local manual reproduction now, automated in Phase 10.

## 4. Known limitation (read before filing a confusing bug)

The 3 "worker nodes" above are processes sharing one real Docker daemon, not isolated hosts. Resource limits and placement decisions are real; resource *isolation* between simulated nodes is not. See ADR-0009 for the full reasoning and when this gets revisited.

## 5. Environment variables (placeholder — finalized during Phase 1 scaffolding)

```text
DATABASE_URL=postgres://...
NATS_URL=nats://localhost:4222
JWT_SIGNING_KEY=...
ENCRYPTION_KEY=...        # for env_vars.value_encrypted, see database-schema.md
```

## 6. Running migrations

```text
goose -dir infrastructure/postgres/migrations postgres "$DATABASE_URL" up
```

(Exact invocation to be confirmed once the migrations directory exists — placeholder per the ordering in `database-schema.md` §4.)

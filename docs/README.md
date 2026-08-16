# Documentation Index

This is the map of every planning document for the platform. Start at [`ARCHITECTURE.md`](../ARCHITECTURE.md) in the repo root for the high-level picture, then drill into whichever doc below matches what you're working on.

Status of the whole doc set: **Phase 0 planning — no implementation code exists yet.** Track phase-by-phase progress in [`ROADMAP.md`](../ROADMAP.md).

## Reading order for a newcomer (human or future-you)

1. [`ARCHITECTURE.md`](../ARCHITECTURE.md) — system overview, component responsibilities, MVP scope
2. [`adr/`](adr/) — *why* each major decision was made, in the order they were decided
3. [`database-schema.md`](database-schema.md) — the data model everything else hangs off of
4. [`api-conventions.md`](api-conventions.md) — how the API server's public surface behaves
5. [`rbac-multitenancy.md`](rbac-multitenancy.md) — how tenant isolation and permissions are actually enforced
6. [`modularity-and-extensibility.md`](modularity-and-extensibility.md) — the interface/service boundaries that keep components swappable and the platform microservices-ready long term
7. [`coding-standards.md`](coding-standards.md) — how code should look before it's reviewed
8. [`testing-strategy.md`](testing-strategy.md) — what "done" means for a given piece of work
9. [`local-development.md`](local-development.md) — how to actually run the thing on your machine
10. [`phases/phase-1-mvp.md`](phases/phase-1-mvp.md) — the concrete, task-level plan for what we build first

## Directory layout

```text
docs/
├── README.md                    # this file
├── adr/                          # Architecture Decision Records — one per major, hard-to-reverse call
│   ├── 0001-control-data-plane-split.md
│   ├── 0002-go-for-infrastructure.md
│   ├── 0003-postgres-control-plane-store.md
│   ├── 0004-multitenant-schema-from-day-one.md
│   ├── 0005-nats-for-eventing.md
│   ├── 0006-container-runtime-abstraction.md
│   ├── 0007-rest-external-grpc-internal.md
│   ├── 0008-jwt-refresh-token-auth.md
│   ├── 0009-local-dev-logical-node-simulation.md
│   ├── 0010-defense-in-depth-tenant-isolation.md
│   ├── 0011-ports-and-adapters-for-external-dependencies.md
│   └── 0012-network-only-service-boundaries.md
├── database-schema.md
├── api-conventions.md
├── rbac-multitenancy.md
├── modularity-and-extensibility.md
├── coding-standards.md
├── testing-strategy.md
├── local-development.md
└── phases/
    └── phase-1-mvp.md
```

## Document maintenance rules

- **ADRs are immutable once accepted.** If a decision changes, write a new ADR that supersedes the old one (link both ways) — don't edit history.
- **`ARCHITECTURE.md` and the docs here are updated as decisions are made**, not batched up after the fact. If an implementation task reveals the plan was wrong, fix the doc in the same PR as the code.
- **`ROADMAP.md`** is the one document expected to change constantly — it's a status tracker, not a design doc.

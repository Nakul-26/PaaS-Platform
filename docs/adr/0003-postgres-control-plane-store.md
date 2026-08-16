# ADR 0003: PostgreSQL as the control-plane system of record

- Status: Accepted
- Date: 2026-08-16

## Context

Control-plane state (organizations, projects, applications, deployments, nodes, quotas) is inherently relational: an application belongs to exactly one project, which belongs to exactly one org; quotas are enforced against real aggregate counts; audit logs need to durably and correctly reference the entities they describe. This data also needs strong consistency — a quota check or an RBAC check that's allowed to be "eventually correct" is a security bug, not a performance tradeoff.

## Decision

PostgreSQL is the single system of record for all control-plane state. JSONB columns are used where the shape is genuinely variable (node labels, port lists, audit metadata) but the core entity graph (orgs → projects → applications → deployments) is modeled as real foreign-keyed relational tables, not a document store.

## Consequences

- Positive: real foreign-key integrity prevents an entire class of "orphaned deployment pointing at a deleted application" bugs for free.
- Positive: row-level security (RLS) is available as a second, DB-enforced layer of tenant isolation on top of application-code checks — see ADR-0010.
- Positive: transactions give correct-by-construction quota enforcement (check-and-increment inside one transaction) instead of racy read-then-write application logic.
- Negative: horizontal write scaling is harder than a document store's default sharding story — acceptable at this project's scale; revisit only if actual measured load demands it (see `ARCHITECTURE.md` §9 R-obs on `resource_usage_samples` specifically).
- Time-series-shaped data (`resource_usage_samples`) is deliberately kept in Postgres for the MVP even though it's not the ideal long-term fit — flagged explicitly as a thing to revisit in Phase 9, not treated as settled.

## Alternatives considered

- **MongoDB**: better fit for genuinely document-shaped, schema-flexible data, but the core entity graph here is relational, and Mongo has no equivalent to RLS for tenant-isolation defense-in-depth.
- **MySQL**: comparable to Postgres for this use case; Postgres chosen for RLS, richer JSONB support, and `LISTEN/NOTIFY` (usable for lightweight reconciliation wake-ups without needing NATS for everything).

## Related

`database-schema.md`, ADR-0004, ADR-0010.

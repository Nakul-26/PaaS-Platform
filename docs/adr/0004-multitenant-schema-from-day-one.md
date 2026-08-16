# ADR 0004: Model multi-tenancy in the schema from Phase 1, even though the MVP doesn't expose it

- Status: Accepted
- Date: 2026-08-16

## Context

The original phased plan puts the multi-tenant SaaS layer (orgs, RBAC, quotas) at Phase 6, well after the single-node MVP (Phase 1). But `org_id`/tenant scoping touches nearly every table in the schema. Adding it later means an invasive migration across every existing table plus a rewrite of every existing query to add tenant filters — a much bigger, riskier change than carrying an unused column for a few phases.

Scheduling, load balancing, and networking, by contrast, are additive: they introduce new tables and new services without changing the shape of existing ones.

## Decision

The full multi-tenant schema (`organizations`, `memberships`, `projects` scoped to `org_id`, etc. — see `database-schema.md`) is created starting in Phase 1's migrations. The MVP creates and uses exactly one default organization and does not build RBAC UI or enforcement yet (that's still Phase 6 work), but every table that will eventually be tenant-scoped is tenant-scoped from its first migration.

## Consequences

- Positive: no invasive schema migration or query rewrite is needed when Phase 6 arrives — it's turning on enforcement and UI against a shape that already exists.
- Positive: RLS policies (ADR-0010) can be designed and tested early against real tables instead of retrofitted.
- Negative: Phase 1 code carries a small amount of apparently-unused complexity (every query includes an `org_id`, even though there's only ever one org) — judged worth it given the retrofit cost on the other side.
- This is the one deliberate exception to "don't pull work forward from later phases" — justified specifically because schema changes are the expensive-to-retrofit category and everything else in Phase 6 is not.

## Alternatives considered

- **Strict phase adherence** (add tenancy only at Phase 6): rejected due to the asymmetric cost of retrofitting relational schema vs. carrying a few unused columns early.

## Related

`database-schema.md`, `ARCHITECTURE.md` §7 (MVP scope), ADR-0010.

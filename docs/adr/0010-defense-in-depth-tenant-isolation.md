# ADR 0010: Tenant isolation enforced redundantly at three layers, never just one

- Status: Accepted
- Date: 2026-08-16

## Context

A single missed `WHERE org_id = ?` filter anywhere in the API server's query code is a cross-tenant data leak — the single most severe class of bug this platform can ship. Relying on "we'll be careful in application code" as the only defense is not an acceptable risk posture for a multi-tenant system.

## Decision

Enforce tenant scoping at three independent layers:

1. **Application layer**: every query handler filters by `org_id` derived from the authenticated session, not from client-supplied input.
2. **Database layer**: PostgreSQL row-level security (RLS) policies on every tenant-scoped table, keyed off a session variable (`app.current_org_id`) set at the start of each request's DB transaction. Even a buggy query missing an explicit filter cannot return another tenant's rows.
3. **Test layer**: integration tests specifically attempt cross-tenant access for every resource type and assert denial — these tests are treated as required, not optional, for any PR touching a tenant-scoped table or endpoint.

The same layered principle is applied to resource quotas (ADR-adjacent, see `ARCHITECTURE.md` §2.9: API-layer check, scheduler re-check, DB constraint backstop) for the same reason — TOCTOU gaps between layers are exactly where quota bypasses happen.

## Consequences

- Positive: no single missed check anywhere in application code results in a cross-tenant leak — RLS is a hard backstop enforced by Postgres itself.
- Positive: the required cross-tenant-access test suite gives a concrete, checkable definition of "isolation is enforced" rather than a vague aspiration.
- Negative: RLS policies add a small amount of per-query overhead and require discipline to keep in sync with schema changes (every new tenant-scoped table needs its policy written at creation time, not bolted on later) — mitigated by making this part of the Phase 1 migration template (see `database-schema.md`).
- Negative: local debugging occasionally requires deliberately bypassing RLS (as a superuser) to inspect cross-tenant state for support/debugging purposes — this path must itself be audited (logged) when used.

## Alternatives considered

- **Application-layer filtering only**: rejected as the sole mechanism — a single missed filter is a full breach with no backstop, unacceptable for the stated severity of this bug class.

## Related

`rbac-multitenancy.md`, `database-schema.md`, `ARCHITECTURE.md` §9 R4.

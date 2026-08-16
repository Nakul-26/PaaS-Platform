# Database Schema

Backs ADR-0003 (Postgres as system of record) and ADR-0004 (multi-tenant schema from day one). This is the detailed version of `ARCHITECTURE.md` §4 — treat this file as the source of truth for the schema; update it in the same PR as any migration.

## 1. Entity relationship overview

```text
organizations 1───N memberships N───1 users
      │
      1
      │
      N
   projects
      │
      1
      │
      N
 applications ──1───N env_vars
      │  │
      │  N
      │  └── deployments (revisions)
      │           │
      │           1
      │           N
      │       containers ───N───1 nodes
      │
      N
      └── domains
      │
      1
      N
   services ───1───N service_instances ───N───1 containers

organizations 1───1 resource_quotas
organizations 1───N audit_logs
organizations 1───N api_keys
```

## 2. Tables

Each table lists: purpose, key columns beyond the obvious, indexes, and RLS notes (ADR-0010).

### `organizations`
Tenant root. Everything else is scoped to one, directly or transitively.
- `id uuid pk`, `name`, `slug unique`, `created_at`
- No RLS needed on this table itself (it *is* the tenant boundary); access controlled via `memberships`.

### `users`
Account identity, not tenant-scoped (a user can belong to multiple orgs).
- `id uuid pk`, `email unique`, `password_hash`, `created_at`
- No RLS — a user's own row is always visible to themself; membership rows are what's tenant-scoped.

### `memberships`
Join table: which users belong to which orgs, with what role.
- `id pk`, `org_id fk→organizations`, `user_id fk→users`, `role enum(owner|admin|developer|viewer)`, `created_at`
- Unique constraint on `(org_id, user_id)`.
- Index: `(user_id)` — "what orgs am I in" is a hot lookup on every request (auth middleware).
- RLS: a row is visible only if `user_id = current_user_id()` OR the requester has `owner`/`admin` role in the same `org_id` (needed for the org's member-management UI).

### `api_keys`
Non-interactive auth (ADR-0008).
- `id pk`, `org_id fk`, `name`, `key_hash`, `scopes text[]`, `created_by fk→users`, `created_at`, `revoked_at nullable`
- Index: `(key_hash)` for lookup on every API-key-authenticated request.
- RLS: scoped to `org_id`.

### `projects`
- `id pk`, `org_id fk`, `name`, `slug`, `created_at`
- Unique: `(org_id, slug)`.
- RLS: scoped to `org_id`.

### `applications`
Desired-state root for a deployable unit.
- `id pk`, `project_id fk→projects`, `name`, `image`, `replicas_desired int`, `cpu_millicores int`, `memory_mb int`, `ports jsonb`, `health_check_path`, `created_at`, `updated_at`
- RLS: scoped via join to `projects.org_id` (RLS policy uses a subquery or a denormalized `org_id` column — see §4 note on denormalization).

### `env_vars`
- `id pk`, `application_id fk`, `key`, `value_encrypted bytea`, `is_secret boolean`, `created_at`
- `value_encrypted`: application-layer envelope encryption (ADR-0010-adjacent risk R5 in `ARCHITECTURE.md`) — never plaintext, from the first migration that creates this table.
- Unique: `(application_id, key)`.

### `deployments`
One row per deployment revision — this is the audit trail of "what was deployed when," not just current state.
- `id pk`, `application_id fk`, `image`, `revision int`, `status enum(pending|scheduling|running|failed|rolled_back)`, `strategy enum(recreate|rolling)`, `created_by fk→users`, `created_at`, `completed_at nullable`
- Index: `(application_id, revision desc)` — "latest deployment for this app" and "deployment history" are both hot.

### `nodes`
Worker node registry.
- `id pk`, `hostname`, `ip`, `cpu_capacity_millicores`, `memory_capacity_mb`, `status enum(healthy|degraded|unreachable)`, `last_heartbeat_at`, `labels jsonb`, `created_at`
- Not tenant-scoped — nodes are infrastructure, shared across tenants. No RLS; access restricted to control-plane services and org `owner`/`admin` roles via application-layer checks against a platform-level (not org-level) permission, since exposing node topology to every tenant is itself a minor info leak worth gating.

### `containers`
Actual running (or terminated) instances.
- `id pk`, `deployment_id fk`, `node_id fk→nodes`, `container_runtime_id` (Docker's own container ID), `status enum(pending|running|crashed|stopped)`, `restart_count int`, `started_at`, `stopped_at nullable`
- Index: `(deployment_id, status)` — reconciliation's "how many are actually running" query.

### `services` / `service_instances`
Durable backing store for service discovery (`ARCHITECTURE.md` §2.6); the in-memory registry in the load balancer is a cache of this.
- `services`: `id pk`, `application_id fk`, `dns_name`, `created_at`
- `service_instances`: `id pk`, `service_id fk`, `container_id fk`, `ip`, `port`, `healthy boolean`, `last_seen_at`
- Index on `service_instances(service_id, healthy)` — the load balancer's resync query.
- `last_seen_at` + a TTL check is what ages out instances whose heartbeats stopped arriving.

### `domains`
- `id pk`, `project_id fk`, `hostname unique`, `application_id fk→applications`, `tls_status enum(pending|active|failed)`, `created_at`

### `resource_quotas`
One row per org (not a history table).
- `org_id pk fk→organizations`, `max_cpu_millicores`, `max_memory_mb`, `max_containers`, `max_projects`, `max_deployments_per_day`
- CHECK constraints as the final backstop layer (`ARCHITECTURE.md` §2.9) — e.g. a trigger on `applications`/`containers` insert that rejects if it would exceed `max_containers` for the owning org. This is a deliberate belt-and-suspenders layer: the API server and scheduler should already have rejected the request first.

### `audit_logs`
Append-only.
- `id pk`, `org_id fk`, `actor_user_id fk→users nullable` (nullable for system-initiated actions), `action text`, `target_type text`, `target_id uuid`, `metadata jsonb`, `created_at`
- No updates or deletes permitted at the application layer (enforce via a `REVOKE UPDATE, DELETE` on the table for the app's DB role, not just convention).

### `resource_usage_samples`
- `id pk`, `org_id fk`, `node_id fk`, `application_id fk`, `cpu_millicores`, `memory_mb`, `sampled_at`
- Flagged in `ARCHITECTURE.md` §9 (risk register) as a Postgres-for-now, revisit-in-Phase-9 decision — do not over-invest in indexing/partitioning this table until real volume is measured.

## 3. Multi-tenancy implementation notes (ADR-0004, ADR-0010)

- Tables that aren't directly `org_id`-scoped (`applications`, `deployments`, `containers`, `env_vars`, `service_instances`) reach their org via a join chain. For RLS policies to stay simple and performant, **denormalize `org_id` onto every tenant-scoped table** (even ones that could derive it via join) rather than writing multi-hop RLS policies. This trades a small amount of storage/write complexity (must keep the denormalized column consistent, e.g. via a trigger or by always setting it explicitly at insert time) for RLS policies that are a single flat `org_id = current_setting('app.current_org_id')::uuid` check everywhere — much easier to audit for correctness than nested-subquery policies.
- The application sets `app.current_org_id` as a transaction-local session variable at the start of every authenticated request's DB transaction, derived from the verified membership row — never from client-supplied input directly.

## 4. Migration tooling and ordering

Per `ARCHITECTURE.md` §11 open question 1: **goose**, SQL-file based, numbered sequentially. Proposed Phase 1 migration order (each migration is additive; RLS policies are written in the same migration that creates the table, not bolted on later per ADR-0010):

```text
0001_organizations_users_memberships.sql
0002_api_keys.sql
0003_projects.sql
0004_applications_env_vars.sql
0005_deployments.sql
0006_nodes_containers.sql
0007_services_service_instances.sql
0008_domains.sql
0009_resource_quotas.sql
0010_audit_logs.sql
0011_resource_usage_samples.sql
0012_rls_policies.sql   -- or inline per-table above; decide at implementation time
```

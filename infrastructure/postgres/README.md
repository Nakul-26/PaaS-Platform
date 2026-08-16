# infrastructure/postgres

`goose` migrations live in `migrations/`, per `docs/database-schema.md` §4 and `docs/phases/phase-1-mvp.md` Task 2.

Migrations `0001`-`0004` cover the Phase 1 tables (organizations/users/memberships, api_keys, projects, applications/env_vars), each with its RLS policy in the same migration (ADR-0010). Migration 0001 also creates the `platform_app` role that application services connect as — RLS is meaningless against a superuser connection (Postgres superusers bypass even `FORCE ROW LEVEL SECURITY`).

Run migrations with `go tool goose -dir infrastructure/postgres/migrations postgres "$DATABASE_URL" up` (see `.env.example` at the repo root for `DATABASE_URL`/`APP_DATABASE_URL`).

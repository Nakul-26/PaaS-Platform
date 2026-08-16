-- +goose Up
CREATE TABLE organizations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    slug TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TYPE membership_role AS ENUM ('owner', 'admin', 'developer', 'viewer');

CREATE TABLE memberships (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id UUID NOT NULL REFERENCES organizations (id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    role membership_role NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (org_id, user_id)
);

CREATE INDEX memberships_user_id_idx ON memberships (user_id);

-- Dedicated non-superuser, non-BYPASSRLS role for the application to connect
-- as. Postgres superusers (and roles with BYPASSRLS) ignore RLS entirely,
-- even FORCE ROW LEVEL SECURITY — so RLS is meaningless unless the app
-- connects as a role like this one (ADR-0010 layer 2). Local-dev-only
-- credential; revisit via a real secrets mechanism before this ships past
-- Phase 1 (see docs/modularity-and-extensibility.md's SecretsProvider row).
-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'platform_app') THEN
        CREATE ROLE platform_app LOGIN PASSWORD 'platform_app';
    END IF;
END
$$;
-- +goose StatementEnd

GRANT USAGE ON SCHEMA public TO platform_app;
-- ALTER DEFAULT PRIVILEGES only covers tables created after this point
-- (migrations 0002+); grant explicitly on the three already created above.
GRANT SELECT, INSERT, UPDATE, DELETE ON organizations, users, memberships TO platform_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO platform_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO platform_app;

-- organizations and users carry no RLS policy of their own: organizations
-- *is* the tenant boundary, and a user's identity row isn't tenant-scoped
-- (database-schema.md §2) — access to both is controlled via memberships.
ALTER TABLE memberships ENABLE ROW LEVEL SECURITY;
ALTER TABLE memberships FORCE ROW LEVEL SECURITY;

-- Visible if it's the requester's own membership, or the requester is an
-- owner/admin in the same org (needed for member-management UI, Phase 6).
CREATE POLICY memberships_isolation ON memberships
    USING (
        user_id = current_setting('app.current_user_id', true)::uuid
        OR EXISTS (
            SELECT 1 FROM memberships m2
            WHERE m2.org_id = memberships.org_id
              AND m2.user_id = current_setting('app.current_user_id', true)::uuid
              AND m2.role IN ('owner', 'admin')
        )
    );

-- +goose Down
ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE SELECT, INSERT, UPDATE, DELETE ON TABLES FROM platform_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE USAGE, SELECT ON SEQUENCES FROM platform_app;
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM platform_app;
REVOKE USAGE ON SCHEMA public FROM platform_app;
DROP ROLE IF EXISTS platform_app;
DROP TABLE memberships;
DROP TYPE membership_role;
DROP TABLE users;
DROP TABLE organizations;

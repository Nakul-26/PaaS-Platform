-- +goose Up
CREATE TABLE applications (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id UUID NOT NULL REFERENCES organizations (id) ON DELETE CASCADE,
    project_id UUID NOT NULL REFERENCES projects (id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    image TEXT NOT NULL,
    replicas_desired INT NOT NULL DEFAULT 1,
    cpu_millicores INT NOT NULL DEFAULT 0,
    memory_mb INT NOT NULL DEFAULT 0,
    ports JSONB NOT NULL DEFAULT '[]',
    health_check_path TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE applications ENABLE ROW LEVEL SECURITY;
ALTER TABLE applications FORCE ROW LEVEL SECURITY;

-- See the comment on api_keys_isolation (0002_api_keys.sql) for why this is
-- an OR of two branches rather than a single org_id check.
CREATE POLICY applications_isolation ON applications
    USING (
        org_id = NULLIF(current_setting('app.current_org_id', true), '')::uuid
        OR org_id IN (
            SELECT m.org_id FROM memberships m
            WHERE m.user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid
        )
    );

-- env_vars reaches its org only via applications.project_id → projects.org_id;
-- org_id is denormalized here directly per database-schema.md §3 so its RLS
-- policy stays a flat column check rather than a multi-hop subquery.
CREATE TABLE env_vars (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id UUID NOT NULL REFERENCES organizations (id) ON DELETE CASCADE,
    application_id UUID NOT NULL REFERENCES applications (id) ON DELETE CASCADE,
    key TEXT NOT NULL,
    value_encrypted BYTEA NOT NULL,
    is_secret BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (application_id, key)
);

ALTER TABLE env_vars ENABLE ROW LEVEL SECURITY;
ALTER TABLE env_vars FORCE ROW LEVEL SECURITY;

CREATE POLICY env_vars_isolation ON env_vars
    USING (
        org_id = NULLIF(current_setting('app.current_org_id', true), '')::uuid
        OR org_id IN (
            SELECT m.org_id FROM memberships m
            WHERE m.user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid
        )
    );

-- +goose Down
DROP TABLE env_vars;
DROP TABLE applications;

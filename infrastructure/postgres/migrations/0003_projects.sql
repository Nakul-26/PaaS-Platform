-- +goose Up
CREATE TABLE projects (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id UUID NOT NULL REFERENCES organizations (id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    slug TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (org_id, slug)
);

ALTER TABLE projects ENABLE ROW LEVEL SECURITY;
ALTER TABLE projects FORCE ROW LEVEL SECURITY;

-- See the comment on api_keys_isolation (0002_api_keys.sql) for why this is
-- an OR of two branches rather than a single org_id check.
CREATE POLICY projects_isolation ON projects
    USING (
        org_id = NULLIF(current_setting('app.current_org_id', true), '')::uuid
        OR org_id IN (
            SELECT m.org_id FROM memberships m
            WHERE m.user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid
        )
    );

-- +goose Down
DROP TABLE projects;

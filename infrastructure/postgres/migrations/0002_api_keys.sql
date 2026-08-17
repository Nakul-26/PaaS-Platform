-- +goose Up
CREATE TABLE api_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id UUID NOT NULL REFERENCES organizations (id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    key_hash TEXT NOT NULL,
    scopes TEXT[] NOT NULL DEFAULT '{}',
    created_by UUID NOT NULL REFERENCES users (id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at TIMESTAMPTZ
);

CREATE INDEX api_keys_key_hash_idx ON api_keys (key_hash);

ALTER TABLE api_keys ENABLE ROW LEVEL SECURITY;
ALTER TABLE api_keys FORCE ROW LEVEL SECURITY;

-- Two ways to satisfy this policy: the caller already knows and has
-- verified org_id (set app.current_org_id directly, e.g. routes shaped
-- /v1/orgs/:orgId/...), or the caller only knows a child resource's ID and
-- needs to *resolve* its org_id first (deep routes like
-- /v1/projects/:projectId/applications per api-conventions.md §2) — the
-- membership branch lets that resolution query run RLS-scoped by
-- app.current_user_id alone, returning zero rows (not an error) for a
-- project/application the caller isn't a member of, which is exactly the
-- "don't leak existence" behavior rbac-multitenancy.md §5 requires.
CREATE POLICY api_keys_isolation ON api_keys
    USING (
        org_id = NULLIF(current_setting('app.current_org_id', true), '')::uuid
        OR org_id IN (
            SELECT m.org_id FROM memberships m
            WHERE m.user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid
        )
    );

-- +goose Down
DROP TABLE api_keys;

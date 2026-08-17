-- +goose Up
CREATE TYPE deployment_status AS ENUM ('pending', 'scheduling', 'running', 'failed', 'rolled_back');
CREATE TYPE deployment_strategy AS ENUM ('recreate', 'rolling');

CREATE TABLE deployments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id UUID NOT NULL REFERENCES organizations (id) ON DELETE CASCADE,
    application_id UUID NOT NULL REFERENCES applications (id) ON DELETE CASCADE,
    image TEXT NOT NULL,
    revision INT NOT NULL,
    status deployment_status NOT NULL DEFAULT 'pending',
    strategy deployment_strategy NOT NULL DEFAULT 'recreate',
    -- Phase 1 has exactly one worker, called directly over HTTP
    -- (phase-1-mvp.md Task 5) — no nodes/containers tables yet (those land
    -- with real scheduling in Phase 2, database-schema.md §4). Until then,
    -- the worker's own container id is the only handle the API server has
    -- for stopping/removing what it deployed, so it's kept here rather than
    -- introduced via a table this phase doesn't otherwise need.
    worker_container_id TEXT,
    created_by UUID NOT NULL REFERENCES users (id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    UNIQUE (application_id, revision)
);

CREATE INDEX deployments_application_id_revision_idx ON deployments (application_id, revision DESC);

ALTER TABLE deployments ENABLE ROW LEVEL SECURITY;
ALTER TABLE deployments FORCE ROW LEVEL SECURITY;

-- See the comment on api_keys_isolation (0002_api_keys.sql) for why this is
-- an OR of two branches rather than a single org_id check.
CREATE POLICY deployments_isolation ON deployments
    USING (
        org_id = NULLIF(current_setting('app.current_org_id', true), '')::uuid
        OR org_id IN (
            SELECT m.org_id FROM memberships m
            WHERE m.user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid
        )
    );

-- +goose Down
DROP TABLE deployments;
DROP TYPE deployment_strategy;
DROP TYPE deployment_status;

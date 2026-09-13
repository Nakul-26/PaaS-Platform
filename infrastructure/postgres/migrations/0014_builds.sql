-- +goose Up
-- phase-7-deployment-platform.md Task 1: one row per git-based deploy
-- attempt. Deliberately a separate table from `deployments`, not a
-- nullable git_url/git_ref pair bolted onto it — a build is a distinct
-- lifecycle (pending -> cloning -> building -> pushing -> succeeded/failed)
-- that only sometimes produces a deployments row (handleBuildCompleted,
-- phase-7-deployment-platform.md Task 4, creates one only on success), and
-- an image-based deploy never has a build at all.
CREATE TYPE build_status AS ENUM ('pending', 'cloning', 'building', 'pushing', 'succeeded', 'failed');

-- org_id is denormalized here (database-schema.md §3's rule: every
-- tenant-scoped table gets org_id directly so RLS stays a flat check) —
-- builds is tenant-authored config like domains, not cluster-observed
-- runtime state like containers/nodes, so (unlike those) it does carry RLS.
CREATE TABLE builds (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id UUID NOT NULL REFERENCES organizations (id) ON DELETE CASCADE,
    application_id UUID NOT NULL REFERENCES applications (id) ON DELETE CASCADE,
    git_url TEXT NOT NULL,
    git_ref TEXT NOT NULL DEFAULT 'main',
    commit_sha TEXT,
    status build_status NOT NULL DEFAULT 'pending',
    image TEXT,
    error_message TEXT,
    created_by UUID NOT NULL REFERENCES users (id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);

-- ListByApplication's own query shape: newest-first, keyset-paginated on
-- (application_id, created_at, id) — same cursor convention
-- AuditLogRepository.ListByOrg/DeploymentRepository.ListByApplication use.
CREATE INDEX builds_application_id_created_at_id_idx ON builds (application_id, created_at DESC, id DESC);

ALTER TABLE builds ENABLE ROW LEVEL SECURITY;
ALTER TABLE builds FORCE ROW LEVEL SECURITY;

-- See the comment on api_keys_isolation (0002_api_keys.sql) for why this is
-- an OR of two branches rather than a single org_id check.
CREATE POLICY builds_isolation ON builds
    USING (
        org_id = NULLIF(current_setting('app.current_org_id', true), '')::uuid
        OR org_id IN (
            SELECT m.org_id FROM memberships m
            WHERE m.user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid
        )
    );

-- +goose Down
DROP TABLE builds;
DROP TYPE build_status;

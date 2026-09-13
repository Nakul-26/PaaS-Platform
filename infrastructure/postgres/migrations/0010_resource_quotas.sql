-- +goose Up
-- One row per org (not a history table) — database-schema.md's
-- `resource_quotas` entry, phase-6-multi-tenant-saas.md Task 1.
CREATE TABLE resource_quotas (
    org_id UUID PRIMARY KEY REFERENCES organizations (id) ON DELETE CASCADE,
    max_cpu_millicores INT NOT NULL,
    max_memory_mb INT NOT NULL,
    max_containers INT NOT NULL,
    max_projects INT NOT NULL,
    max_deployments_per_day INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE resource_quotas ENABLE ROW LEVEL SECURITY;
ALTER TABLE resource_quotas FORCE ROW LEVEL SECURITY;

-- See the comment on api_keys_isolation (0002_api_keys.sql) for why this is
-- an OR of two branches rather than a single org_id check.
CREATE POLICY resource_quotas_isolation ON resource_quotas
    USING (
        org_id = NULLIF(current_setting('app.current_org_id', true), '')::uuid
        OR org_id IN (
            SELECT m.org_id FROM memberships m
            WHERE m.user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid
        )
    );

-- +goose Down
DROP TABLE resource_quotas;

-- +goose Up
-- Append-only — database-schema.md's `audit_logs` entry,
-- phase-6-multi-tenant-saas.md Task 1/7. target_id/target_type are NOT
-- NULL: every action this table ever records is a mutation on a specific
-- resource (Task 7 only wires this into mutating routes), unlike
-- actor_user_id, which stays nullable for a system-initiated action even
-- though none exists yet.
CREATE TABLE audit_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id UUID NOT NULL REFERENCES organizations (id) ON DELETE CASCADE,
    actor_user_id UUID REFERENCES users (id),
    action TEXT NOT NULL,
    target_type TEXT NOT NULL,
    target_id UUID NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ListByOrg's own query shape: newest-first, keyset-paginated on
-- (org_id, created_at, id).
CREATE INDEX audit_logs_org_id_created_at_id_idx ON audit_logs (org_id, created_at DESC, id DESC);

ALTER TABLE audit_logs ENABLE ROW LEVEL SECURITY;
ALTER TABLE audit_logs FORCE ROW LEVEL SECURITY;

-- See the comment on api_keys_isolation (0002_api_keys.sql) for why this is
-- an OR of two branches rather than a single org_id check.
CREATE POLICY audit_logs_isolation ON audit_logs
    USING (
        org_id = NULLIF(current_setting('app.current_org_id', true), '')::uuid
        OR org_id IN (
            SELECT m.org_id FROM memberships m
            WHERE m.user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid
        )
    );

-- Append-only per database-schema.md's own spec: migration 0001's
-- `ALTER DEFAULT PRIVILEGES ... GRANT SELECT, INSERT, UPDATE, DELETE ON
-- TABLES TO platform_app` would otherwise let the app role update/delete
-- rows here like any other table. Revoked explicitly so the guarantee holds
-- at the DB layer regardless of any application-code bug, not just by
-- convention — AuditLogRepository (internal/db) also simply has no
-- Update/Delete method, but this is the layer that actually can't be
-- bypassed by a bug.
REVOKE UPDATE, DELETE ON audit_logs FROM platform_app;

-- +goose Down
GRANT UPDATE, DELETE ON audit_logs TO platform_app;
DROP TABLE audit_logs;

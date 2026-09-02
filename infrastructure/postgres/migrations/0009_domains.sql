-- +goose Up
CREATE TYPE domain_tls_status AS ENUM ('pending', 'active', 'failed');

-- org_id is denormalized here (not in database-schema.md §2's original ERD
-- sketch column list) per §3's own rule: every tenant-scoped table gets
-- org_id directly so RLS stays a flat check rather than a multi-hop
-- subquery through projects/applications — domains is tenant-scoped exactly
-- like applications/env_vars.
CREATE TABLE domains (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id UUID NOT NULL REFERENCES organizations (id) ON DELETE CASCADE,
    project_id UUID NOT NULL REFERENCES projects (id) ON DELETE CASCADE,
    application_id UUID NOT NULL REFERENCES applications (id) ON DELETE CASCADE,
    hostname TEXT NOT NULL UNIQUE,
    tls_status domain_tls_status NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE domains ENABLE ROW LEVEL SECURITY;
ALTER TABLE domains FORCE ROW LEVEL SECURITY;

-- See the comment on api_keys_isolation (0002_api_keys.sql) for why this is
-- an OR of two branches rather than a single org_id check.
CREATE POLICY domains_isolation ON domains
    USING (
        org_id = NULLIF(current_setting('app.current_org_id', true), '')::uuid
        OR org_id IN (
            SELECT m.org_id FROM memberships m
            WHERE m.user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid
        )
    );

-- The load balancer's resync query (phase-5-networking-ingress.md Task 4):
-- every domain currently pointed at an application, joined against
-- services.application_id — this table is read with no RLS session set up
-- (DomainRepository.ListAllActive, same cross-tenant-read posture as
-- LoadBalancerRepository, phase-4-service-discovery-lb.md Task 1), so the
-- index just needs to support "all domains" efficiently, which the primary
-- key scan already does; application_id is indexed for the join itself.
CREATE INDEX domains_application_id_idx ON domains (application_id);

-- +goose Down
DROP TABLE domains;
DROP TYPE domain_tls_status;

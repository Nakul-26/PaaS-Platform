-- +goose Up
CREATE TABLE services (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    application_id UUID NOT NULL UNIQUE REFERENCES applications (id) ON DELETE CASCADE,
    dns_name TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE service_instances (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    service_id UUID NOT NULL REFERENCES services (id) ON DELETE CASCADE,
    container_id UUID NOT NULL UNIQUE REFERENCES containers (id) ON DELETE CASCADE,
    ip TEXT NOT NULL,
    port INT NOT NULL,
    healthy BOOLEAN NOT NULL DEFAULT true,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The load balancer's resync query (phase-4-service-discovery-lb.md Task 4):
-- for a given service, which instances are currently healthy.
CREATE INDEX service_instances_service_id_healthy_idx ON service_instances (service_id, healthy);

-- Neither table carries RLS, matching nodes/containers' Phase 2 precedent
-- (database-schema.md's `nodes` entry): this is cluster runtime state
-- derived from containers/nodes, not tenant-scoped, and the load balancer's
-- hot path can't depend on an RLS session being set up. platform_app's
-- blanket grant from migration 0001 already covers these tables.

-- +goose Down
DROP TABLE service_instances;
DROP TABLE services;

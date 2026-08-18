-- +goose Up
CREATE TYPE node_status AS ENUM ('healthy', 'degraded', 'unreachable');

CREATE TABLE nodes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    hostname TEXT NOT NULL,
    ip TEXT NOT NULL,
    cpu_capacity_millicores INT NOT NULL,
    memory_capacity_mb INT NOT NULL,
    status node_status NOT NULL DEFAULT 'healthy',
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    labels JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TYPE container_status AS ENUM ('pending', 'running', 'crashed', 'stopped');

CREATE TABLE containers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    deployment_id UUID NOT NULL REFERENCES deployments (id) ON DELETE CASCADE,
    node_id UUID NOT NULL REFERENCES nodes (id),
    container_runtime_id TEXT,
    status container_status NOT NULL DEFAULT 'pending',
    restart_count INT NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ,
    stopped_at TIMESTAMPTZ
);

CREATE INDEX containers_deployment_id_status_idx ON containers (deployment_id, status);
-- Scheduler's filter-then-score placement (ARCHITECTURE.md §2.2,
-- phase-2-multi-node.md Task 5) scores candidate nodes by current load, i.e.
-- a count of each node's running containers.
CREATE INDEX containers_node_id_status_idx ON containers (node_id, status);

-- Neither table carries RLS: both are infrastructure, not tenant-scoped
-- (database-schema.md's `nodes` entry). Visibility is gated at the
-- application layer against a platform-level owner/admin permission instead
-- (phase-2-multi-node.md Task 6, GET /v1/nodes) — platform_app's blanket
-- grant from migration 0001 already covers these tables.

-- +goose Down
DROP TABLE containers;
DROP TYPE container_status;
DROP TABLE nodes;
DROP TYPE node_status;

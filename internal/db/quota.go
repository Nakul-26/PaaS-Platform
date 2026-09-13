package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ResourceQuota is one org's resource ceilings (ARCHITECTURE.md §2.9,
// database-schema.md's `resource_quotas` entry) — one row per org, not a
// history table.
type ResourceQuota struct {
	OrgID                uuid.UUID
	MaxCPUMillicores     int
	MaxMemoryMB          int
	MaxContainers        int
	MaxProjects          int
	MaxDeploymentsPerDay int
	CreatedAt            time.Time
}

// DefaultResourceQuota is the placeholder ceiling every new org gets at
// signup (phase-6-multi-tenant-saas.md Task 1) — same "untuned constant,
// revisit with real usage data" posture as every other placeholder default
// this codebase carries (resync intervals, health-check windows, ejection
// windows, ...). OrgID/CreatedAt are left zero-valued; Create fills them in.
var DefaultResourceQuota = ResourceQuota{
	MaxCPUMillicores:     8000,
	MaxMemoryMB:          16384,
	MaxContainers:        20,
	MaxProjects:          10,
	MaxDeploymentsPerDay: 100,
}

// ResourceUsage is one org's current consumption against each of
// ResourceQuota's ceilings. Always computed live from the tables that are
// already the source of truth for each figure (projects, applications,
// containers via deployments) — never a cached counter, so it can never
// drift out of sync with what those tables actually say. Containers and
// CPU/memory are measured differently on purpose: Containers counts actual
// rows in `pending`/`running` status (real committed runtime state, what
// the scheduler is about to add to), while CPUMillicores/MemoryMB sums each
// application's desired commitment (`cpu_millicores`/`memory_mb` ×
// `replicas_desired`) directly from `applications` — the resource an org
// has *declared* it wants, which is what should be checked before a
// deploy/scale request that hasn't produced any container rows yet.
type ResourceUsage struct {
	Projects         int
	Containers       int
	CPUMillicores    int
	MemoryMB         int
	DeploymentsToday int
}

// QuotaRepository is the port apiserver/scheduler depend on (ADR-0011).
// RLS-bound (database-schema.md §3's two-branch policy, same shape as
// every other tenant-scoped repository).
type QuotaRepository interface {
	// Create inserts orgID's quota row using q's limit fields (q.OrgID and
	// q.CreatedAt are ignored — the returned ResourceQuota carries the
	// real ones). Called exactly once per org, in the same transaction as
	// the org itself (handleSignup).
	Create(ctx context.Context, orgID uuid.UUID, q ResourceQuota) (ResourceQuota, error)
	Get(ctx context.Context, orgID uuid.UUID) (ResourceQuota, error)
	// CurrentUsage computes orgID's live usage against every ResourceQuota
	// ceiling — see ResourceUsage's own doc comment for the measurement
	// choices.
	CurrentUsage(ctx context.Context, orgID uuid.UUID) (ResourceUsage, error)
}

type quotaRepository struct{ conn Conn }

func NewQuotaRepository(conn Conn) QuotaRepository {
	return &quotaRepository{conn: conn}
}

func (r *quotaRepository) Create(ctx context.Context, orgID uuid.UUID, q ResourceQuota) (ResourceQuota, error) {
	var out ResourceQuota
	err := r.conn.QueryRow(ctx,
		`INSERT INTO resource_quotas (org_id, max_cpu_millicores, max_memory_mb, max_containers, max_projects, max_deployments_per_day)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 RETURNING org_id, max_cpu_millicores, max_memory_mb, max_containers, max_projects, max_deployments_per_day, created_at`,
		orgID, q.MaxCPUMillicores, q.MaxMemoryMB, q.MaxContainers, q.MaxProjects, q.MaxDeploymentsPerDay,
	).Scan(&out.OrgID, &out.MaxCPUMillicores, &out.MaxMemoryMB, &out.MaxContainers, &out.MaxProjects, &out.MaxDeploymentsPerDay, &out.CreatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return ResourceQuota{}, ErrConflict
		}
		return ResourceQuota{}, fmt.Errorf("creating resource quota: %w", err)
	}
	return out, nil
}

func (r *quotaRepository) Get(ctx context.Context, orgID uuid.UUID) (ResourceQuota, error) {
	var q ResourceQuota
	err := r.conn.QueryRow(ctx,
		`SELECT org_id, max_cpu_millicores, max_memory_mb, max_containers, max_projects, max_deployments_per_day, created_at
		 FROM resource_quotas WHERE org_id = $1`,
		orgID,
	).Scan(&q.OrgID, &q.MaxCPUMillicores, &q.MaxMemoryMB, &q.MaxContainers, &q.MaxProjects, &q.MaxDeploymentsPerDay, &q.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ResourceQuota{}, ErrNotFound
		}
		return ResourceQuota{}, fmt.Errorf("getting resource quota: %w", err)
	}
	return q, nil
}

func (r *quotaRepository) CurrentUsage(ctx context.Context, orgID uuid.UUID) (ResourceUsage, error) {
	var u ResourceUsage

	if err := r.conn.QueryRow(ctx,
		`SELECT COUNT(*) FROM projects WHERE org_id = $1`, orgID,
	).Scan(&u.Projects); err != nil {
		return ResourceUsage{}, fmt.Errorf("counting projects: %w", err)
	}

	// containers carries no org_id of its own (database-schema.md's
	// `containers` entry) — reached via deployment_id -> deployments.org_id,
	// same join every other cross-table quota-adjacent query in this
	// codebase would need.
	if err := r.conn.QueryRow(ctx,
		`SELECT COUNT(*) FROM containers c
		 JOIN deployments d ON c.deployment_id = d.id
		 WHERE d.org_id = $1 AND c.status IN ('pending', 'running')`, orgID,
	).Scan(&u.Containers); err != nil {
		return ResourceUsage{}, fmt.Errorf("counting containers: %w", err)
	}

	if err := r.conn.QueryRow(ctx,
		`SELECT COALESCE(SUM(cpu_millicores * replicas_desired), 0), COALESCE(SUM(memory_mb * replicas_desired), 0)
		 FROM applications WHERE org_id = $1`, orgID,
	).Scan(&u.CPUMillicores, &u.MemoryMB); err != nil {
		return ResourceUsage{}, fmt.Errorf("summing desired cpu/memory: %w", err)
	}

	if err := r.conn.QueryRow(ctx,
		`SELECT COUNT(*) FROM deployments WHERE org_id = $1 AND created_at >= now() - interval '24 hours'`, orgID,
	).Scan(&u.DeploymentsToday); err != nil {
		return ResourceUsage{}, fmt.Errorf("counting today's deployments: %w", err)
	}

	return u, nil
}

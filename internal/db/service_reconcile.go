package db

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// ServiceInstanceCandidate is one currently-running container eligible to
// back a service_instances row (phase-4-service-discovery-lb.md Task 3):
// enough of its owning application's identity to lazily create the
// services row's dns_name, plus the node ip that, paired with Task 2's
// reported port, is the instance's full reachable address.
type ServiceInstanceCandidate struct {
	ApplicationID   uuid.UUID
	ProjectSlug     string
	ApplicationName string
	ContainerID     uuid.UUID
	NodeIP          string
}

// ServiceReconcileRepository is the port controller-manager's Service
// Instance controller depends on (ADR-0011, phase-4-service-discovery-lb.md
// Task 3). Like ReconcileRepository, its queries span applications,
// projects, and deployments across every tenant by design — the controller
// has no per-request user session to scope RLS to, and reconciling every
// tenant's instances every tick is exactly its job. It must therefore run
// over the same RLS-bypass connection controller-manager already gives
// ReconcileRepository (see that type's own doc comment, and
// controller-manager's main.go).
type ServiceReconcileRepository interface {
	// ListRunningCandidates returns one row per container currently
	// 'running', for every application whose latest deployment hasn't
	// dead-ended — the same "not failed/rolled_back" signal
	// ReconcileRepository's own doc comment explains is the closest
	// available "active" filter, since no deployment in this codebase ever
	// reaches status 'running' itself.
	ListRunningCandidates(ctx context.Context) ([]ServiceInstanceCandidate, error)
	// ListOrphanedInstances returns the container_id of every
	// service_instances row whose backing container is no longer
	// 'running' — the tick's removal side. Self-limiting: it only ever
	// considers containers an instance already exists for, never the full
	// containers table.
	ListOrphanedInstances(ctx context.Context) ([]uuid.UUID, error)
}

type serviceReconcileRepository struct{ conn Conn }

func NewServiceReconcileRepository(conn Conn) ServiceReconcileRepository {
	return &serviceReconcileRepository{conn: conn}
}

func (r *serviceReconcileRepository) ListRunningCandidates(ctx context.Context) ([]ServiceInstanceCandidate, error) {
	rows, err := r.conn.Query(ctx,
		`SELECT a.id, p.slug, a.name, c.id, n.ip
		 FROM applications a
		 JOIN projects p ON p.id = a.project_id
		 JOIN LATERAL (
		     SELECT id, status FROM deployments
		     WHERE application_id = a.id
		     ORDER BY revision DESC LIMIT 1
		 ) d ON true
		 JOIN containers c ON c.deployment_id = d.id AND c.status = 'running'
		 JOIN nodes n ON n.id = c.node_id
		 WHERE d.status NOT IN ('failed', 'rolled_back')`,
	)
	if err != nil {
		return nil, fmt.Errorf("listing service instance candidates: %w", err)
	}
	defer rows.Close()

	var candidates []ServiceInstanceCandidate
	for rows.Next() {
		var c ServiceInstanceCandidate
		if err := rows.Scan(&c.ApplicationID, &c.ProjectSlug, &c.ApplicationName, &c.ContainerID, &c.NodeIP); err != nil {
			return nil, fmt.Errorf("scanning service instance candidate: %w", err)
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating service instance candidates: %w", err)
	}
	return candidates, nil
}

func (r *serviceReconcileRepository) ListOrphanedInstances(ctx context.Context) ([]uuid.UUID, error) {
	rows, err := r.conn.Query(ctx,
		`SELECT si.container_id
		 FROM service_instances si
		 JOIN containers c ON c.id = si.container_id
		 WHERE c.status <> 'running'`,
	)
	if err != nil {
		return nil, fmt.Errorf("listing orphaned service instances: %w", err)
	}
	defer rows.Close()

	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scanning orphaned service instance container id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating orphaned service instances: %w", err)
	}
	return ids, nil
}

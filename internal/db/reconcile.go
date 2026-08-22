package db

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

// ReconcileTarget is one row of the Deployment controller's per-tick
// worklist (phase-3-controllers.md Task 4): an application whose latest
// deployment is currently 'running', paired with enough of its desired
// state to diff against Task 3's active-container count and, if short,
// re-request placement without a second round-trip to Postgres.
type ReconcileTarget struct {
	ApplicationID   uuid.UUID
	DeploymentID    uuid.UUID
	Image           string
	Ports           []PortSpec
	ReplicasDesired int
}

// ReconcileRepository is the port controller-manager depends on
// (ADR-0011). Unlike every other repository in this package, its query
// spans applications and deployments across every tenant by design — the
// Deployment controller has no per-request user session to scope
// app.current_org_id to (RLS on both tables is keyed on a real,
// authenticated membership, ADR-0010), and reconciling every tenant's
// desired state every tick is exactly its job. It must therefore run over
// an RLS-bypass connection (a superuser or BYPASSRLS role) rather than the
// platform_app pool everything else in this package uses — ADR-0010's own
// "local debugging occasionally requires deliberately bypassing RLS...
// this path must itself be audited" consequence, applied to a legitimate
// standing control-plane need instead of a one-off debugging session. See
// controller-manager's main.go for which connection it's given.
type ReconcileRepository interface {
	// ListActive returns one row per application whose latest deployment
	// hasn't dead-ended (status not 'failed'/'rolled_back'). Not filtered
	// on status = 'running': nothing in this codebase ever sets a
	// deployment to 'running' today (UpdateResult is only ever called with
	// 'failed', on a publish error) — the CLI's "shows running"
	// (apps/cli/internal/cmd/get_deployments.go) comes from a live join
	// onto the container's own status, not deployments.status. Filtering
	// literally on 'running' here would mean this repository — and the
	// controller built on it — reconciles nothing, ever.
	ListActive(ctx context.Context) ([]ReconcileTarget, error)
}

type reconcileRepository struct{ conn Conn }

func NewReconcileRepository(conn Conn) ReconcileRepository {
	return &reconcileRepository{conn: conn}
}

func (r *reconcileRepository) ListActive(ctx context.Context) ([]ReconcileTarget, error) {
	rows, err := r.conn.Query(ctx,
		`SELECT a.id, d.id, d.image, a.ports, a.replicas_desired
		 FROM applications a
		 JOIN LATERAL (
		     SELECT id, image, status FROM deployments
		     WHERE application_id = a.id
		     ORDER BY revision DESC LIMIT 1
		 ) d ON true
		 WHERE d.status NOT IN ('failed', 'rolled_back')`,
	)
	if err != nil {
		return nil, fmt.Errorf("listing reconcile targets: %w", err)
	}
	defer rows.Close()

	var targets []ReconcileTarget
	for rows.Next() {
		var t ReconcileTarget
		var portsRaw []byte
		if err := rows.Scan(&t.ApplicationID, &t.DeploymentID, &t.Image, &portsRaw, &t.ReplicasDesired); err != nil {
			return nil, fmt.Errorf("scanning reconcile target: %w", err)
		}
		if err := json.Unmarshal(portsRaw, &t.Ports); err != nil {
			return nil, fmt.Errorf("unmarshaling reconcile target ports: %w", err)
		}
		targets = append(targets, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating reconcile targets: %w", err)
	}
	return targets, nil
}

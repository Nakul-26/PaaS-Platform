package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type DeploymentStatus string

const (
	DeploymentStatusPending    DeploymentStatus = "pending"
	DeploymentStatusScheduling DeploymentStatus = "scheduling"
	DeploymentStatusRunning    DeploymentStatus = "running"
	DeploymentStatusFailed     DeploymentStatus = "failed"
	DeploymentStatusRolledBack DeploymentStatus = "rolled_back"
)

type Deployment struct {
	ID                uuid.UUID
	OrgID             uuid.UUID
	ApplicationID     uuid.UUID
	Image             string
	Revision          int
	Status            DeploymentStatus
	Strategy          string
	WorkerContainerID *string
	CreatedBy         uuid.UUID
	CreatedAt         time.Time
	CompletedAt       *time.Time
}

// DeploymentRepository is the port apiserver depends on (ADR-0011).
type DeploymentRepository interface {
	// Create inserts a new deployment revision — Phase 1 always
	// strategy='recreate' (phase-1-mvp.md Task 5: "recreate-only, single
	// replica"; rolling/rollback are Phase 8). revision auto-increments
	// per application.
	Create(ctx context.Context, orgID, applicationID uuid.UUID, image string, createdBy uuid.UUID) (Deployment, error)
	// UpdateResult records the outcome of actually driving the worker
	// (docs/worker-agent-contract.md) for a deployment created by Create.
	UpdateResult(ctx context.Context, id uuid.UUID, status DeploymentStatus, workerContainerID *string) error
	// ListByApplication returns deployments newest-revision-first, cursor
	// paginated per api-conventions.md §4 (afterRevision is the last
	// revision the caller already saw; nil starts from the top).
	ListByApplication(ctx context.Context, applicationID uuid.UUID, limit int, afterRevision *int) ([]Deployment, error)
	// Latest returns the most recent deployment for an application, or
	// ErrNotFound if it has never been deployed.
	Latest(ctx context.Context, applicationID uuid.UUID) (Deployment, error)
}

type deploymentRepository struct{ conn Conn }

func NewDeploymentRepository(conn Conn) DeploymentRepository {
	return &deploymentRepository{conn: conn}
}

func scanDeployment(row pgx.Row) (Deployment, error) {
	var d Deployment
	err := row.Scan(&d.ID, &d.OrgID, &d.ApplicationID, &d.Image, &d.Revision, &d.Status, &d.Strategy, &d.WorkerContainerID, &d.CreatedBy, &d.CreatedAt, &d.CompletedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Deployment{}, ErrNotFound
		}
		return Deployment{}, err
	}
	return d, nil
}

const deploymentColumns = `id, org_id, application_id, image, revision, status, strategy, worker_container_id, created_by, created_at, completed_at`

func (r *deploymentRepository) Create(ctx context.Context, orgID, applicationID uuid.UUID, image string, createdBy uuid.UUID) (Deployment, error) {
	d, err := scanDeployment(r.conn.QueryRow(ctx,
		`INSERT INTO deployments (org_id, application_id, image, revision, status, strategy, created_by)
		 VALUES ($1, $2, $3, COALESCE((SELECT MAX(revision) FROM deployments WHERE application_id = $2), 0) + 1, 'pending', 'recreate', $4)
		 RETURNING `+deploymentColumns,
		orgID, applicationID, image, createdBy,
	))
	if err != nil {
		if isUniqueViolation(err) {
			return Deployment{}, ErrConflict
		}
		return Deployment{}, fmt.Errorf("creating deployment: %w", err)
	}
	return d, nil
}

func (r *deploymentRepository) UpdateResult(ctx context.Context, id uuid.UUID, status DeploymentStatus, workerContainerID *string) error {
	tag, err := r.conn.Exec(ctx,
		`UPDATE deployments SET status = $2, worker_container_id = $3, completed_at = now() WHERE id = $1`,
		id, status, workerContainerID,
	)
	if err != nil {
		return fmt.Errorf("updating deployment result: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *deploymentRepository) ListByApplication(ctx context.Context, applicationID uuid.UUID, limit int, afterRevision *int) ([]Deployment, error) {
	var rows pgx.Rows
	var err error
	if afterRevision != nil {
		rows, err = r.conn.Query(ctx,
			`SELECT `+deploymentColumns+` FROM deployments
			 WHERE application_id = $1 AND revision < $2
			 ORDER BY revision DESC LIMIT $3`,
			applicationID, *afterRevision, limit,
		)
	} else {
		rows, err = r.conn.Query(ctx,
			`SELECT `+deploymentColumns+` FROM deployments
			 WHERE application_id = $1
			 ORDER BY revision DESC LIMIT $2`,
			applicationID, limit,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("listing deployments: %w", err)
	}
	defer rows.Close()

	var deployments []Deployment
	for rows.Next() {
		d, err := scanDeployment(rows)
		if err != nil {
			return nil, fmt.Errorf("scanning deployment: %w", err)
		}
		deployments = append(deployments, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating deployments: %w", err)
	}
	return deployments, nil
}

func (r *deploymentRepository) Latest(ctx context.Context, applicationID uuid.UUID) (Deployment, error) {
	return scanDeployment(r.conn.QueryRow(ctx,
		`SELECT `+deploymentColumns+` FROM deployments
		 WHERE application_id = $1
		 ORDER BY revision DESC LIMIT 1`,
		applicationID,
	))
}

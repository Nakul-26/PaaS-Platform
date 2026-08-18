package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type ContainerStatus string

const (
	ContainerStatusPending ContainerStatus = "pending"
	ContainerStatusRunning ContainerStatus = "running"
	ContainerStatusCrashed ContainerStatus = "crashed"
	ContainerStatusStopped ContainerStatus = "stopped"
)

type Container struct {
	ID                 uuid.UUID
	DeploymentID       uuid.UUID
	NodeID             uuid.UUID
	ContainerRuntimeID *string
	Status             ContainerStatus
	RestartCount       int
	StartedAt          *time.Time
	StoppedAt          *time.Time
}

// ContainerRepository is the port scheduler/apiserver depend on
// (ADR-0011). containers carries no RLS (database-schema.md's `nodes`
// entry — infrastructure, not tenant-scoped) so its methods work against
// any Conn.
type ContainerRepository interface {
	// Create records a placement decision: the scheduler has picked nodeID
	// for deploymentID (phase-2-multi-node.md Task 5). Starts 'pending'
	// until the worker's node.<id>.status confirms it actually started.
	Create(ctx context.Context, deploymentID, nodeID uuid.UUID) (Container, error)
	Get(ctx context.Context, id uuid.UUID) (Container, error)
	// UpdateStatus records a worker's node.<id>.status observation
	// (docs/nats-contract.md). containerRuntimeID is only set once known
	// (nil until the worker actually starts the container); started_at and
	// stopped_at are stamped the first time status reaches 'running' or a
	// terminal state, respectively.
	UpdateStatus(ctx context.Context, id uuid.UUID, status ContainerStatus, containerRuntimeID *string) error
	// ListByDeployment surfaces which node(s) a deployment landed on
	// (phase-2-multi-node.md Task 6), newest-first.
	ListByDeployment(ctx context.Context, deploymentID uuid.UUID) ([]Container, error)
	// ListByNode backs the scheduler's load-scoring step (Task 5's
	// filter-then-score: score candidate nodes by current load).
	ListByNode(ctx context.Context, nodeID uuid.UUID) ([]Container, error)
}

type containerRepository struct{ conn Conn }

func NewContainerRepository(conn Conn) ContainerRepository {
	return &containerRepository{conn: conn}
}

const containerColumns = `id, deployment_id, node_id, container_runtime_id, status, restart_count, started_at, stopped_at`

func scanContainer(row pgx.Row) (Container, error) {
	var c Container
	err := row.Scan(&c.ID, &c.DeploymentID, &c.NodeID, &c.ContainerRuntimeID, &c.Status, &c.RestartCount, &c.StartedAt, &c.StoppedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Container{}, ErrNotFound
		}
		return Container{}, err
	}
	return c, nil
}

func (r *containerRepository) Create(ctx context.Context, deploymentID, nodeID uuid.UUID) (Container, error) {
	c, err := scanContainer(r.conn.QueryRow(ctx,
		`INSERT INTO containers (deployment_id, node_id) VALUES ($1, $2) RETURNING `+containerColumns,
		deploymentID, nodeID,
	))
	if err != nil {
		return Container{}, fmt.Errorf("creating container: %w", err)
	}
	return c, nil
}

func (r *containerRepository) Get(ctx context.Context, id uuid.UUID) (Container, error) {
	return scanContainer(r.conn.QueryRow(ctx, `SELECT `+containerColumns+` FROM containers WHERE id = $1`, id))
}

func (r *containerRepository) UpdateStatus(ctx context.Context, id uuid.UUID, status ContainerStatus, containerRuntimeID *string) error {
	tag, err := r.conn.Exec(ctx,
		`UPDATE containers SET
		     status = $2::container_status,
		     container_runtime_id = COALESCE($3, container_runtime_id),
		     started_at = CASE WHEN $2::container_status = 'running' AND started_at IS NULL THEN now() ELSE started_at END,
		     stopped_at = CASE WHEN $2::container_status IN ('crashed', 'stopped') AND stopped_at IS NULL THEN now() ELSE stopped_at END
		 WHERE id = $1`,
		id, status, containerRuntimeID,
	)
	if err != nil {
		return fmt.Errorf("updating container status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *containerRepository) ListByDeployment(ctx context.Context, deploymentID uuid.UUID) ([]Container, error) {
	rows, err := r.conn.Query(ctx,
		`SELECT `+containerColumns+` FROM containers WHERE deployment_id = $1 ORDER BY started_at DESC NULLS FIRST`,
		deploymentID,
	)
	if err != nil {
		return nil, fmt.Errorf("listing containers by deployment: %w", err)
	}
	defer rows.Close()
	return scanContainers(rows)
}

func (r *containerRepository) ListByNode(ctx context.Context, nodeID uuid.UUID) ([]Container, error) {
	rows, err := r.conn.Query(ctx,
		`SELECT `+containerColumns+` FROM containers WHERE node_id = $1`,
		nodeID,
	)
	if err != nil {
		return nil, fmt.Errorf("listing containers by node: %w", err)
	}
	defer rows.Close()
	return scanContainers(rows)
}

func scanContainers(rows pgx.Rows) ([]Container, error) {
	var containers []Container
	for rows.Next() {
		c, err := scanContainer(rows)
		if err != nil {
			return nil, fmt.Errorf("scanning container: %w", err)
		}
		containers = append(containers, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating containers: %w", err)
	}
	return containers, nil
}

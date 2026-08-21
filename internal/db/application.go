package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// PortSpec is one entry of an application's `ports` column — the same
// shape the worker agent's contract uses (docs/worker-agent-contract.md),
// so it can be passed straight through without translation.
type PortSpec struct {
	ContainerPort int    `json:"container_port"`
	HostPort      int    `json:"host_port,omitempty"`
	Protocol      string `json:"protocol,omitempty"`
}

type Application struct {
	ID              uuid.UUID
	OrgID           uuid.UUID
	ProjectID       uuid.UUID
	Name            string
	Image           string
	ReplicasDesired int
	CPUMillicores   int
	MemoryMB        int
	Ports           []PortSpec
	HealthCheckPath *string
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// ApplicationRepository is the port apiserver depends on (ADR-0011).
type ApplicationRepository interface {
	Create(ctx context.Context, orgID, projectID uuid.UUID, name, image string, ports []PortSpec) (Application, error)
	// Get requires app.current_org_id to already be set (ADR-0010) — use
	// OrgID first to resolve it on deep-by-ID routes that don't have it
	// yet.
	Get(ctx context.Context, id uuid.UUID) (Application, error)
	// OrgID resolves id's owning org, RLS-scoped only by
	// app.current_user_id — see ProjectRepository.OrgID for why.
	OrgID(ctx context.Context, id uuid.UUID) (uuid.UUID, error)
	// UpdateImage records a new desired-state image (a deploy updates what
	// the application *should* be running, database-schema.md §2).
	UpdateImage(ctx context.Context, id uuid.UUID, image string) error
	// UpdateReplicas records a new desired replica count (a scale request
	// updates what the application *should* be running, same as UpdateImage;
	// phase-3-controllers.md Tasks 3/5). It only updates desired state — the
	// Deployment controller (Task 4) is what actually reconciles toward it.
	UpdateReplicas(ctx context.Context, id uuid.UUID, replicas int) error
	Delete(ctx context.Context, id uuid.UUID) error
}

type applicationRepository struct{ conn Conn }

func NewApplicationRepository(conn Conn) ApplicationRepository {
	return &applicationRepository{conn: conn}
}

func (r *applicationRepository) Create(ctx context.Context, orgID, projectID uuid.UUID, name, image string, ports []PortSpec) (Application, error) {
	portsJSON, err := json.Marshal(ports)
	if err != nil {
		return Application{}, fmt.Errorf("marshaling ports: %w", err)
	}

	var a Application
	var portsRaw []byte
	err = r.conn.QueryRow(ctx,
		`INSERT INTO applications (org_id, project_id, name, image, ports)
		 VALUES ($1, $2, $3, $4, $5::jsonb)
		 RETURNING id, org_id, project_id, name, image, replicas_desired, cpu_millicores, memory_mb, ports, health_check_path, created_at, updated_at`,
		orgID, projectID, name, image, string(portsJSON),
	).Scan(&a.ID, &a.OrgID, &a.ProjectID, &a.Name, &a.Image, &a.ReplicasDesired, &a.CPUMillicores, &a.MemoryMB, &portsRaw, &a.HealthCheckPath, &a.CreatedAt, &a.UpdatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return Application{}, ErrConflict
		}
		return Application{}, fmt.Errorf("creating application: %w", err)
	}
	if err := json.Unmarshal(portsRaw, &a.Ports); err != nil {
		return Application{}, fmt.Errorf("unmarshaling ports: %w", err)
	}
	return a, nil
}

func (r *applicationRepository) Get(ctx context.Context, id uuid.UUID) (Application, error) {
	var a Application
	var portsRaw []byte
	err := r.conn.QueryRow(ctx,
		`SELECT id, org_id, project_id, name, image, replicas_desired, cpu_millicores, memory_mb, ports, health_check_path, created_at, updated_at
		 FROM applications WHERE id = $1`,
		id,
	).Scan(&a.ID, &a.OrgID, &a.ProjectID, &a.Name, &a.Image, &a.ReplicasDesired, &a.CPUMillicores, &a.MemoryMB, &portsRaw, &a.HealthCheckPath, &a.CreatedAt, &a.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Application{}, ErrNotFound
		}
		return Application{}, fmt.Errorf("fetching application: %w", err)
	}
	if err := json.Unmarshal(portsRaw, &a.Ports); err != nil {
		return Application{}, fmt.Errorf("unmarshaling ports: %w", err)
	}
	return a, nil
}

func (r *applicationRepository) OrgID(ctx context.Context, id uuid.UUID) (uuid.UUID, error) {
	var orgID uuid.UUID
	err := r.conn.QueryRow(ctx, `SELECT org_id FROM applications WHERE id = $1`, id).Scan(&orgID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return uuid.Nil, ErrNotFound
		}
		return uuid.Nil, fmt.Errorf("resolving application org: %w", err)
	}
	return orgID, nil
}

func (r *applicationRepository) UpdateImage(ctx context.Context, id uuid.UUID, image string) error {
	tag, err := r.conn.Exec(ctx,
		`UPDATE applications SET image = $2, updated_at = now() WHERE id = $1`,
		id, image,
	)
	if err != nil {
		return fmt.Errorf("updating application image: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *applicationRepository) UpdateReplicas(ctx context.Context, id uuid.UUID, replicas int) error {
	tag, err := r.conn.Exec(ctx,
		`UPDATE applications SET replicas_desired = $2, updated_at = now() WHERE id = $1`,
		id, replicas,
	)
	if err != nil {
		return fmt.Errorf("updating application replicas: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *applicationRepository) Delete(ctx context.Context, id uuid.UUID) error {
	tag, err := r.conn.Exec(ctx, `DELETE FROM applications WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("deleting application: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

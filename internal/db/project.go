package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type Project struct {
	ID        uuid.UUID
	OrgID     uuid.UUID
	Name      string
	Slug      string
	CreatedAt time.Time
}

// ProjectRepository is the port apiserver depends on (ADR-0011).
type ProjectRepository interface {
	Create(ctx context.Context, orgID uuid.UUID, name, slug string) (Project, error)
	// OrgID resolves id's owning org, RLS-scoped only by
	// app.current_user_id (database-schema.md §3) — used to answer "which
	// org does this project belong to" for deep-by-ID routes
	// (api-conventions.md §2) before app.current_org_id is known. Returns
	// ErrNotFound both when the project genuinely doesn't exist and when
	// the caller isn't a member of its org (rbac-multitenancy.md §5).
	OrgID(ctx context.Context, id uuid.UUID) (uuid.UUID, error)
}

type projectRepository struct{ conn Conn }

func NewProjectRepository(conn Conn) ProjectRepository {
	return &projectRepository{conn: conn}
}

func (r *projectRepository) Create(ctx context.Context, orgID uuid.UUID, name, slug string) (Project, error) {
	var p Project
	err := r.conn.QueryRow(ctx,
		`INSERT INTO projects (org_id, name, slug) VALUES ($1, $2, $3)
		 RETURNING id, org_id, name, slug, created_at`,
		orgID, name, slug,
	).Scan(&p.ID, &p.OrgID, &p.Name, &p.Slug, &p.CreatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return Project{}, ErrConflict
		}
		return Project{}, fmt.Errorf("creating project: %w", err)
	}
	return p, nil
}

func (r *projectRepository) OrgID(ctx context.Context, id uuid.UUID) (uuid.UUID, error) {
	var orgID uuid.UUID
	err := r.conn.QueryRow(ctx, `SELECT org_id FROM projects WHERE id = $1`, id).Scan(&orgID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return uuid.Nil, ErrNotFound
		}
		return uuid.Nil, fmt.Errorf("resolving project org: %w", err)
	}
	return orgID, nil
}

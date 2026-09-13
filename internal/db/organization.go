package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type Organization struct {
	ID        uuid.UUID
	Name      string
	Slug      string
	CreatedAt time.Time
}

// OrganizationRepository is the port apiserver depends on (ADR-0011); no
// RLS applies to organizations itself (database-schema.md §2 — it *is* the
// tenant boundary), so its methods work against any Conn, transactional or
// not. Access control for Get/Update/Delete is the caller's job
// (requireMembership/requirePermission in services/apiserver, per
// rbac-multitenancy.md §2) — this repository trusts whatever id it's given.
type OrganizationRepository interface {
	Create(ctx context.Context, name, slug string) (Organization, error)
	Get(ctx context.Context, id uuid.UUID) (Organization, error)
	// Update changes only name — slug is immutable once created (it's part
	// of every DNS-facing identity the platform derives, e.g. Phase 4's
	// service dns_name; changing it is a bigger feature than
	// phase-6-multi-tenant-saas.md Task 3 scopes).
	Update(ctx context.Context, id uuid.UUID, name string) (Organization, error)
	// Delete cascades through every FK chain rooted at organizations (all
	// ON DELETE CASCADE — memberships, projects, applications, deployments,
	// domains, resource_quotas, audit_logs, api_keys, and everything those
	// in turn own down to containers/services). It does not stop any
	// actually-running container first — unlike handleDeleteApplication,
	// which publishes node.<id>.unassign per container before deleting, a
	// full org-scoped unassign fan-out across every application is out of
	// Task 3's scope (see ROADMAP.md); flagged here as a known gap rather
	// than silently left, same practice as the Phase 2 Task 9 WORKER_ADDR
	// finding.
	Delete(ctx context.Context, id uuid.UUID) error
}

type organizationRepository struct{ conn Conn }

func NewOrganizationRepository(conn Conn) OrganizationRepository {
	return &organizationRepository{conn: conn}
}

func (r *organizationRepository) Create(ctx context.Context, name, slug string) (Organization, error) {
	var org Organization
	err := r.conn.QueryRow(ctx,
		`INSERT INTO organizations (name, slug) VALUES ($1, $2) RETURNING id, name, slug, created_at`,
		name, slug,
	).Scan(&org.ID, &org.Name, &org.Slug, &org.CreatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return Organization{}, ErrConflict
		}
		return Organization{}, fmt.Errorf("creating organization: %w", err)
	}
	return org, nil
}

func (r *organizationRepository) Get(ctx context.Context, id uuid.UUID) (Organization, error) {
	var org Organization
	err := r.conn.QueryRow(ctx,
		`SELECT id, name, slug, created_at FROM organizations WHERE id = $1`, id,
	).Scan(&org.ID, &org.Name, &org.Slug, &org.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Organization{}, ErrNotFound
		}
		return Organization{}, fmt.Errorf("fetching organization: %w", err)
	}
	return org, nil
}

func (r *organizationRepository) Update(ctx context.Context, id uuid.UUID, name string) (Organization, error) {
	var org Organization
	err := r.conn.QueryRow(ctx,
		`UPDATE organizations SET name = $2 WHERE id = $1 RETURNING id, name, slug, created_at`,
		id, name,
	).Scan(&org.ID, &org.Name, &org.Slug, &org.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Organization{}, ErrNotFound
		}
		return Organization{}, fmt.Errorf("updating organization: %w", err)
	}
	return org, nil
}

func (r *organizationRepository) Delete(ctx context.Context, id uuid.UUID) error {
	tag, err := r.conn.Exec(ctx, `DELETE FROM organizations WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("deleting organization: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

package db

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
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
// not.
type OrganizationRepository interface {
	Create(ctx context.Context, name, slug string) (Organization, error)
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

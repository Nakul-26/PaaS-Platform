package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Domain maps a tenant-registered hostname onto one of their applications
// (phase-5-networking-ingress.md Task 1, database-schema.md's `domains`
// entry) — the load balancer's real, external routing key, as opposed to
// Phase 4's internal-only `services.dns_name`.
type Domain struct {
	ID            uuid.UUID
	OrgID         uuid.UUID
	ProjectID     uuid.UUID
	ApplicationID uuid.UUID
	Hostname      string
	TLSStatus     string
	CreatedAt     time.Time
}

// DomainRepository is the port apiserver depends on (ADR-0011). RLS-bound
// (database-schema.md §3's two-branch policy, same shape as
// ApplicationRepository) — every method here runs over the RLS-scoped
// platform_app connection, never the admin one DomainRoutingRepository
// needs (see that type's own doc comment for why domains, unlike
// services/service_instances, actually needs one).
type DomainRepository interface {
	Create(ctx context.Context, orgID, projectID, applicationID uuid.UUID, hostname string) (Domain, error)
	ListByProject(ctx context.Context, projectID uuid.UUID) ([]Domain, error)
	// OrgID resolves id's owning org, RLS-scoped only by
	// app.current_user_id — see ProjectRepository.OrgID for why.
	OrgID(ctx context.Context, id uuid.UUID) (uuid.UUID, error)
	Delete(ctx context.Context, id uuid.UUID) error
}

type domainRepository struct{ conn Conn }

func NewDomainRepository(conn Conn) DomainRepository {
	return &domainRepository{conn: conn}
}

func (r *domainRepository) Create(ctx context.Context, orgID, projectID, applicationID uuid.UUID, hostname string) (Domain, error) {
	var d Domain
	err := r.conn.QueryRow(ctx,
		`INSERT INTO domains (org_id, project_id, application_id, hostname)
		 VALUES ($1, $2, $3, $4)
		 RETURNING id, org_id, project_id, application_id, hostname, tls_status, created_at`,
		orgID, projectID, applicationID, hostname,
	).Scan(&d.ID, &d.OrgID, &d.ProjectID, &d.ApplicationID, &d.Hostname, &d.TLSStatus, &d.CreatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return Domain{}, ErrConflict
		}
		return Domain{}, fmt.Errorf("creating domain: %w", err)
	}
	return d, nil
}

func (r *domainRepository) ListByProject(ctx context.Context, projectID uuid.UUID) ([]Domain, error) {
	rows, err := r.conn.Query(ctx,
		`SELECT id, org_id, project_id, application_id, hostname, tls_status, created_at
		 FROM domains WHERE project_id = $1 ORDER BY created_at`,
		projectID,
	)
	if err != nil {
		return nil, fmt.Errorf("listing domains: %w", err)
	}
	defer rows.Close()

	var domains []Domain
	for rows.Next() {
		var d Domain
		if err := rows.Scan(&d.ID, &d.OrgID, &d.ProjectID, &d.ApplicationID, &d.Hostname, &d.TLSStatus, &d.CreatedAt); err != nil {
			return nil, fmt.Errorf("scanning domain: %w", err)
		}
		domains = append(domains, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating domains: %w", err)
	}
	return domains, nil
}

func (r *domainRepository) OrgID(ctx context.Context, id uuid.UUID) (uuid.UUID, error) {
	var orgID uuid.UUID
	err := r.conn.QueryRow(ctx, `SELECT org_id FROM domains WHERE id = $1`, id).Scan(&orgID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return uuid.Nil, ErrNotFound
		}
		return uuid.Nil, fmt.Errorf("resolving domain org: %w", err)
	}
	return orgID, nil
}

func (r *domainRepository) Delete(ctx context.Context, id uuid.UUID) error {
	tag, err := r.conn.Exec(ctx, `DELETE FROM domains WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("deleting domain: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

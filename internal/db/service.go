package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type Service struct {
	ID            uuid.UUID
	ApplicationID uuid.UUID
	DNSName       string
	CreatedAt     time.Time
}

// ServiceRepository is the port controller-manager's Service Instance
// controller depends on (ADR-0011, phase-4-service-discovery-lb.md Task 3).
// services carries no RLS (database-schema.md's `services`/`service_instances`
// entry — cluster runtime state derived from containers/nodes, not
// tenant-scoped) so its methods work against any Conn.
type ServiceRepository interface {
	// GetOrCreate returns the existing service for applicationID, or creates
	// one with dnsName if none exists yet — a service is created lazily on
	// an application's first healthy instance (Task 3), never eagerly at
	// deploy time. dnsName is ignored on an already-existing row.
	GetOrCreate(ctx context.Context, applicationID uuid.UUID, dnsName string) (Service, error)
	// GetByApplication looks up a service by its owning application, without
	// creating one — used by read paths (Task 6) that must 404 rather than
	// conjure a service that no healthy instance has ever backed.
	GetByApplication(ctx context.Context, applicationID uuid.UUID) (Service, error)
}

type serviceRepository struct{ conn Conn }

func NewServiceRepository(conn Conn) ServiceRepository {
	return &serviceRepository{conn: conn}
}

const serviceColumns = `id, application_id, dns_name, created_at`

func scanService(row pgx.Row) (Service, error) {
	var s Service
	err := row.Scan(&s.ID, &s.ApplicationID, &s.DNSName, &s.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Service{}, ErrNotFound
		}
		return Service{}, err
	}
	return s, nil
}

func (r *serviceRepository) GetOrCreate(ctx context.Context, applicationID uuid.UUID, dnsName string) (Service, error) {
	s, err := scanService(r.conn.QueryRow(ctx,
		`INSERT INTO services (application_id, dns_name) VALUES ($1, $2)
		 ON CONFLICT (application_id) DO UPDATE SET application_id = EXCLUDED.application_id
		 RETURNING `+serviceColumns,
		applicationID, dnsName,
	))
	if err != nil {
		return Service{}, fmt.Errorf("getting or creating service: %w", err)
	}
	return s, nil
}

func (r *serviceRepository) GetByApplication(ctx context.Context, applicationID uuid.UUID) (Service, error) {
	return scanService(r.conn.QueryRow(ctx,
		`SELECT `+serviceColumns+` FROM services WHERE application_id = $1`,
		applicationID,
	))
}

package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type ServiceInstance struct {
	ID          uuid.UUID
	ServiceID   uuid.UUID
	ContainerID uuid.UUID
	IP          string
	Port        int
	Healthy     bool
	LastSeenAt  time.Time
}

// ServiceInstanceRepository is the port controller-manager's Service
// Instance controller depends on (ADR-0011, phase-4-service-discovery-lb.md
// Task 3), and the port the load balancer's periodic Postgres resync (Task
// 4) reads through. service_instances carries no RLS, same reasoning as
// ServiceRepository.
type ServiceInstanceRepository interface {
	// Upsert records containerID as a reachable instance of serviceID at
	// ip:port, marking it healthy and refreshing last_seen_at — one row per
	// container (container_id is unique), so a repeated call from the next
	// reconcile tick (Task 3) or worker heartbeat just refreshes the
	// existing row instead of duplicating it.
	Upsert(ctx context.Context, serviceID, containerID uuid.UUID, ip string, port int) (ServiceInstance, error)
	// MarkUnhealthy flips an instance to unhealthy without deleting it —
	// used when a container is observed not running but might still come
	// back (e.g. a transient crash the Deployment controller is about to
	// replace). Returns ErrNotFound if containerID has no instance row.
	MarkUnhealthy(ctx context.Context, containerID uuid.UUID) error
	// DeleteByContainer removes an instance row outright — used once a
	// container is confirmed gone for good (Task 3's reconcile diff no
	// longer sees it at all).
	DeleteByContainer(ctx context.Context, containerID uuid.UUID) error
	// ListHealthyByService is the load balancer's resync query (Task 4):
	// every healthy instance currently backing serviceID.
	ListHealthyByService(ctx context.Context, serviceID uuid.UUID) ([]ServiceInstance, error)
}

type serviceInstanceRepository struct{ conn Conn }

func NewServiceInstanceRepository(conn Conn) ServiceInstanceRepository {
	return &serviceInstanceRepository{conn: conn}
}

const serviceInstanceColumns = `id, service_id, container_id, ip, port, healthy, last_seen_at`

func scanServiceInstance(row pgx.Row) (ServiceInstance, error) {
	var si ServiceInstance
	err := row.Scan(&si.ID, &si.ServiceID, &si.ContainerID, &si.IP, &si.Port, &si.Healthy, &si.LastSeenAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ServiceInstance{}, ErrNotFound
		}
		return ServiceInstance{}, err
	}
	return si, nil
}

func (r *serviceInstanceRepository) Upsert(ctx context.Context, serviceID, containerID uuid.UUID, ip string, port int) (ServiceInstance, error) {
	si, err := scanServiceInstance(r.conn.QueryRow(ctx,
		`INSERT INTO service_instances (service_id, container_id, ip, port, healthy, last_seen_at)
		 VALUES ($1, $2, $3, $4, true, now())
		 ON CONFLICT (container_id) DO UPDATE SET
		     service_id = EXCLUDED.service_id,
		     ip = EXCLUDED.ip,
		     port = EXCLUDED.port,
		     healthy = true,
		     last_seen_at = now()
		 RETURNING `+serviceInstanceColumns,
		serviceID, containerID, ip, port,
	))
	if err != nil {
		return ServiceInstance{}, fmt.Errorf("upserting service instance: %w", err)
	}
	return si, nil
}

func (r *serviceInstanceRepository) MarkUnhealthy(ctx context.Context, containerID uuid.UUID) error {
	tag, err := r.conn.Exec(ctx,
		`UPDATE service_instances SET healthy = false WHERE container_id = $1`,
		containerID,
	)
	if err != nil {
		return fmt.Errorf("marking service instance unhealthy: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *serviceInstanceRepository) DeleteByContainer(ctx context.Context, containerID uuid.UUID) error {
	tag, err := r.conn.Exec(ctx, `DELETE FROM service_instances WHERE container_id = $1`, containerID)
	if err != nil {
		return fmt.Errorf("deleting service instance: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *serviceInstanceRepository) ListHealthyByService(ctx context.Context, serviceID uuid.UUID) ([]ServiceInstance, error) {
	rows, err := r.conn.Query(ctx,
		`SELECT `+serviceInstanceColumns+` FROM service_instances WHERE service_id = $1 AND healthy = true`,
		serviceID,
	)
	if err != nil {
		return nil, fmt.Errorf("listing healthy service instances: %w", err)
	}
	defer rows.Close()

	var instances []ServiceInstance
	for rows.Next() {
		si, err := scanServiceInstance(rows)
		if err != nil {
			return nil, fmt.Errorf("scanning service instance: %w", err)
		}
		instances = append(instances, si)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating service instances: %w", err)
	}
	return instances, nil
}

package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ServiceRegistryEntry is one reachable backend, joined with its owning
// service's dns_name — the shape the load balancer's in-memory registry
// (phase-4-service-discovery-lb.md Task 4) actually needs. Neither
// ServiceRepository nor ServiceInstanceRepository returns this joined shape
// on its own; assembling it in the caller would mean either an N+1 query
// per service (ListAll then GetByApplication per instance) or the load
// balancer reaching into both repositories' raw SQL, so it gets its own
// small cross-table repository instead, same precedent as
// ServiceReconcileRepository.
type ServiceRegistryEntry struct {
	DNSName     string
	ContainerID uuid.UUID
	IP          string
	Port        int
}

// LoadBalancerRepository is the port the loadbalancer service depends on
// (ADR-0011, phase-4-service-discovery-lb.md Task 4). Like
// ServiceReconcileRepository, it spans services+service_instances by design
// and carries no RLS (database-schema.md's services/service_instances
// entry), so it works against the same platform_app connection
// apiserver/scheduler already use — no superuser role needed here, unlike
// controller-manager.
type LoadBalancerRepository interface {
	// ListHealthyRegistry returns every currently-healthy instance across
	// every service, for the load balancer's periodic full resync
	// (ARCHITECTURE.md §2.6's "periodic full-resync fallback in case an
	// event is missed").
	ListHealthyRegistry(ctx context.Context) ([]ServiceRegistryEntry, error)
	// GetHealthyInstanceByContainer looks up the single instance backing
	// containerID, for the incremental service.updated path (Task 3's
	// publisher, Task 4's consumer). ok=false means containerID currently
	// has no healthy instance — either it was just removed (the load
	// balancer should evict it from its registry) or it never had one.
	GetHealthyInstanceByContainer(ctx context.Context, containerID uuid.UUID) (entry ServiceRegistryEntry, ok bool, err error)
}

type loadBalancerRepository struct{ conn Conn }

func NewLoadBalancerRepository(conn Conn) LoadBalancerRepository {
	return &loadBalancerRepository{conn: conn}
}

const serviceRegistryJoin = `
	SELECT s.dns_name, si.container_id, si.ip, si.port
	FROM service_instances si
	JOIN services s ON s.id = si.service_id
	WHERE si.healthy = true`

func (r *loadBalancerRepository) ListHealthyRegistry(ctx context.Context) ([]ServiceRegistryEntry, error) {
	rows, err := r.conn.Query(ctx, serviceRegistryJoin)
	if err != nil {
		return nil, fmt.Errorf("listing service registry: %w", err)
	}
	defer rows.Close()

	var entries []ServiceRegistryEntry
	for rows.Next() {
		var e ServiceRegistryEntry
		if err := rows.Scan(&e.DNSName, &e.ContainerID, &e.IP, &e.Port); err != nil {
			return nil, fmt.Errorf("scanning service registry entry: %w", err)
		}
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating service registry: %w", err)
	}
	return entries, nil
}

func (r *loadBalancerRepository) GetHealthyInstanceByContainer(ctx context.Context, containerID uuid.UUID) (ServiceRegistryEntry, bool, error) {
	var e ServiceRegistryEntry
	err := r.conn.QueryRow(ctx, serviceRegistryJoin+` AND si.container_id = $1`, containerID).
		Scan(&e.DNSName, &e.ContainerID, &e.IP, &e.Port)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ServiceRegistryEntry{}, false, nil
		}
		return ServiceRegistryEntry{}, false, fmt.Errorf("getting service registry entry: %w", err)
	}
	return e, true, nil
}

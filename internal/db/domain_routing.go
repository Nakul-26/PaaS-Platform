package db

import (
	"context"
	"fmt"
)

// DomainRoute is one resolvable hostname → internal service mapping, the
// shape the load balancer's registry needs to route real external traffic
// (phase-5-networking-ingress.md Task 4) — a step beyond Phase 4's
// dns_name-only registry.
type DomainRoute struct {
	Hostname string
	DNSName  string
}

// DomainRoutingRepository is the port the loadbalancer service depends on
// (ADR-0011, phase-5-networking-ingress.md Task 1/4). Unlike
// LoadBalancerRepository (Phase 4), whose services/service_instances tables
// carry no RLS at all, this repository's query spans domains — which does
// carry RLS, because unlike service_instances (cluster-observed runtime
// state) a domain is tenant-authored configuration a customer explicitly
// registers, deserving the same protection applications/projects get. The
// load balancer's resync has no per-request tenant session to scope
// app.current_org_id to, and reading every tenant's domains every resync
// tick is exactly its job — so, exactly like
// internal/db.ReconcileRepository's own documented exception for
// controller-manager, this repository must run over an RLS-bypass
// connection (a superuser or BYPASSRLS role), not the platform_app pool
// LoadBalancerRepository and everything else in this package uses. See
// services/loadbalancer's main.go for which connection it's given.
type DomainRoutingRepository interface {
	// ListRoutes returns every domain currently pointed at an application,
	// resolved to that application's service dns_name, for the load
	// balancer's periodic full resync (same "always pair pub/sub with
	// reconciliation" posture ARCHITECTURE.md §2.6 already established in
	// Phase 4 — see phase-5-networking-ingress.md's open decision 2 for why
	// this phase doesn't add a push event on top of it). A domain whose
	// application has no service yet (Task 3 of Phase 4 hasn't lazily
	// created one — no healthy instance ever existed) is simply absent from
	// the result, not an error.
	ListRoutes(ctx context.Context) ([]DomainRoute, error)
}

type domainRoutingRepository struct{ conn Conn }

func NewDomainRoutingRepository(conn Conn) DomainRoutingRepository {
	return &domainRoutingRepository{conn: conn}
}

func (r *domainRoutingRepository) ListRoutes(ctx context.Context) ([]DomainRoute, error) {
	rows, err := r.conn.Query(ctx,
		`SELECT d.hostname, s.dns_name
		 FROM domains d
		 JOIN services s ON s.application_id = d.application_id`,
	)
	if err != nil {
		return nil, fmt.Errorf("listing domain routes: %w", err)
	}
	defer rows.Close()

	var routes []DomainRoute
	for rows.Next() {
		var rt DomainRoute
		if err := rows.Scan(&rt.Hostname, &rt.DNSName); err != nil {
			return nil, fmt.Errorf("scanning domain route: %w", err)
		}
		routes = append(routes, rt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating domain routes: %w", err)
	}
	return routes, nil
}

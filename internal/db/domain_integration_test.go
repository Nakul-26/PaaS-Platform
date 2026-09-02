//go:build integration

package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// TestDomainRepository covers Task 1's acceptance
// (phase-5-networking-ingress.md): the 0009_domains.sql migration applies
// cleanly on top of 0001-0008, DomainRepository's CRUD works against real
// Postgres with real RLS enforcing cross-tenant isolation (unlike Phase 4's
// services/service_instances, domains is tenant-authored config and does
// carry RLS), and DomainRoutingRepository's cross-tenant join works when run
// over an RLS-bypass (admin) connection.
func TestDomainRepository(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	const (
		dbName    = "platform"
		adminUser = "platform"
		adminPass = "platform"
	)

	container, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase(dbName),
		postgres.WithUsername(adminUser),
		postgres.WithPassword(adminPass),
	)
	if err != nil {
		t.Fatalf("starting postgres container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	adminConnStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("admin connection string: %v", err)
	}
	adminDB, err := sql.Open("pgx", adminConnStr)
	if err != nil {
		t.Fatalf("opening admin connection: %v", err)
	}
	defer func() { _ = adminDB.Close() }()

	if err := waitForPing(ctx, adminDB); err != nil {
		t.Fatalf("waiting for postgres to accept connections: %v", err)
	}

	migrationsDir := filepath.Join("..", "..", "infrastructure", "postgres", "migrations")
	if err := goose.SetDialect("postgres"); err != nil {
		t.Fatalf("goose.SetDialect: %v", err)
	}
	if err := goose.Up(adminDB, migrationsDir); err != nil {
		t.Fatalf("applying migrations: %v", err)
	}

	orgAID, projectAID, appAID := seedApplication(t, ctx, adminDB, "Org A", "org-a", "app-a")
	orgBID, projectBID, appBID := seedApplication(t, ctx, adminDB, "Org B", "org-b", "app-b")

	endpoint, err := container.PortEndpoint(ctx, "5432/tcp", "")
	if err != nil {
		t.Fatalf("resolving container endpoint: %v", err)
	}
	appConnStr := fmt.Sprintf("postgres://platform_app:platform_app@%s/%s?sslmode=disable", endpoint, dbName)
	pool, err := Open(ctx, appConnStr)
	if err != nil {
		t.Fatalf("opening app-role pool: %v", err)
	}
	t.Cleanup(pool.Close)

	// --- DomainRepository: happy path, scoped to org A's own session ---
	var domainAID uuid.UUID
	if err := pool.WithTx(ctx, uuid.Nil, orgAID, func(ctx context.Context, conn Conn) error {
		d, err := NewDomainRepository(conn).Create(ctx, orgAID, projectAID, appAID, "a.example.com")
		if err != nil {
			return err
		}
		if d.Hostname != "a.example.com" || d.TLSStatus != "pending" || d.ApplicationID != appAID {
			t.Fatalf("unexpected domain after Create: %+v", d)
		}
		domainAID = d.ID
		return nil
	}); err != nil {
		t.Fatalf("creating domain for org A: %v", err)
	}

	// --- Duplicate hostname across any org -> ErrConflict ---
	if err := pool.WithTx(ctx, uuid.Nil, orgBID, func(ctx context.Context, conn Conn) error {
		_, err := NewDomainRepository(conn).Create(ctx, orgBID, projectBID, appBID, "a.example.com")
		return err
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict creating a duplicate hostname, got %v", err)
	}

	// A second, distinct hostname for org B, to exercise cross-tenant
	// isolation below.
	if err := pool.WithTx(ctx, uuid.Nil, orgBID, func(ctx context.Context, conn Conn) error {
		_, err := NewDomainRepository(conn).Create(ctx, orgBID, projectBID, appBID, "b.example.com")
		return err
	}); err != nil {
		t.Fatalf("creating domain for org B: %v", err)
	}

	// --- ListByProject, scoped to each org's own session ---
	if err := pool.WithTx(ctx, uuid.Nil, orgAID, func(ctx context.Context, conn Conn) error {
		domains, err := NewDomainRepository(conn).ListByProject(ctx, projectAID)
		if err != nil {
			return err
		}
		if len(domains) != 1 || domains[0].ID != domainAID {
			t.Fatalf("expected org A to see exactly its own domain, got %+v", domains)
		}
		return nil
	}); err != nil {
		t.Fatalf("listing domains for org A: %v", err)
	}

	// --- RLS cross-tenant isolation: org B's session must not see org A's
	// domain, whether via ListByProject (org A's own project_id, queried
	// under org B's session) or OrgID (a direct point lookup by id). ---
	if err := pool.WithTx(ctx, uuid.Nil, orgBID, func(ctx context.Context, conn Conn) error {
		domains, err := NewDomainRepository(conn).ListByProject(ctx, projectAID)
		if err != nil {
			return err
		}
		if len(domains) != 0 {
			t.Fatalf("expected org B's session to see none of org A's domains, got %+v", domains)
		}
		if _, err := NewDomainRepository(conn).OrgID(ctx, domainAID); !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected ErrNotFound resolving org A's domain from org B's session, got %v", err)
		}
		return nil
	}); err != nil {
		t.Fatalf("cross-tenant isolation check: %v", err)
	}

	// OrgID from the owning org's own session resolves correctly.
	if err := pool.WithTx(ctx, uuid.Nil, orgAID, func(ctx context.Context, conn Conn) error {
		gotOrgID, err := NewDomainRepository(conn).OrgID(ctx, domainAID)
		if err != nil {
			return err
		}
		if gotOrgID != orgAID {
			t.Fatalf("expected OrgID to resolve to org A, got %s", gotOrgID)
		}
		return nil
	}); err != nil {
		t.Fatalf("resolving domain org: %v", err)
	}

	// --- Delete ---
	if err := pool.WithTx(ctx, uuid.Nil, orgAID, func(ctx context.Context, conn Conn) error {
		return NewDomainRepository(conn).Delete(ctx, domainAID)
	}); err != nil {
		t.Fatalf("deleting domain: %v", err)
	}
	if err := pool.WithTx(ctx, uuid.Nil, orgAID, func(ctx context.Context, conn Conn) error {
		return NewDomainRepository(conn).Delete(ctx, domainAID)
	}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound deleting an already-deleted domain, got %v", err)
	}

	// --- DomainRoutingRepository: cross-tenant join over the admin
	// connection, no app.current_org_id/current_user_id set at all — the
	// whole point is that it works without one. Only org B's domain
	// (domainBID, app-b) remains; give app-b a service so the join has
	// something to resolve, and confirm a domain whose application has no
	// service yet (there is none seeded for app-a, and its domain is
	// deleted anyway) simply doesn't appear rather than erroring.
	adminPool, err := Open(ctx, adminConnStr)
	if err != nil {
		t.Fatalf("opening admin-role pool: %v", err)
	}
	t.Cleanup(adminPool.Close)

	if _, err := NewServiceRepository(adminPool.Conn()).GetOrCreate(ctx, appBID, "app-b.internal"); err != nil {
		t.Fatalf("creating service for app B: %v", err)
	}

	routes, err := NewDomainRoutingRepository(adminPool.Conn()).ListRoutes(ctx)
	if err != nil {
		t.Fatalf("ListRoutes: %v", err)
	}
	if len(routes) != 1 || routes[0].Hostname != "b.example.com" || routes[0].DNSName != "app-b.internal" {
		t.Fatalf("expected exactly one resolved route (b.example.com -> app-b.internal), got %+v", routes)
	}
}

// seedApplication seeds one org/project/application, mirroring
// node_container_integration_test.go's seedDeployment but also returning
// projectID (which domains needs and that helper doesn't) and taking
// caller-chosen names/slugs so a single test can seed more than one
// tenant's worth of data without unique-constraint collisions.
func seedApplication(t *testing.T, ctx context.Context, adminDB *sql.DB, orgName, orgSlug, appName string) (orgID, projectID, applicationID uuid.UUID) {
	t.Helper()

	if err := adminDB.QueryRowContext(ctx,
		`INSERT INTO organizations (name, slug) VALUES ($1, $2) RETURNING id`, orgName, orgSlug,
	).Scan(&orgID); err != nil {
		t.Fatalf("seeding organization: %v", err)
	}
	if err := adminDB.QueryRowContext(ctx,
		`INSERT INTO projects (org_id, name, slug) VALUES ($1, 'Project', $2) RETURNING id`,
		orgID, orgSlug+"-project",
	).Scan(&projectID); err != nil {
		t.Fatalf("seeding project: %v", err)
	}
	if err := adminDB.QueryRowContext(ctx,
		`INSERT INTO applications (org_id, project_id, name, image) VALUES ($1, $2, $3, 'nginx:latest') RETURNING id`,
		orgID, projectID, appName,
	).Scan(&applicationID); err != nil {
		t.Fatalf("seeding application: %v", err)
	}
	return orgID, projectID, applicationID
}

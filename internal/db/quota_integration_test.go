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

// TestQuotaRepository covers Task 1's acceptance
// (phase-6-multi-tenant-saas.md): the 0010_resource_quotas.sql migration
// applies cleanly on top of 0001-0009, QuotaRepository's Create/Get work
// against real Postgres with real RLS enforcing cross-tenant isolation, and
// CurrentUsage reflects real counts/sums from the tables that are already
// the source of truth for them — including staying RLS-scoped even when
// called with another org's id as an explicit argument (an app-layer bug
// passing the wrong orgID must not leak that org's counts).
func TestQuotaRepository(t *testing.T) {
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

	// seedDeployment gives org A exactly 1 project, 1 application (default
	// cpu_millicores/memory_mb = 0), 1 deployment, 1 user.
	orgAID, userAID, _, appAID := seedDeployment(t, ctx, adminDB)

	// A second application under the same org/project, with real
	// cpu/memory/replica figures, so CurrentUsage's SUM has more than one
	// row (and a non-zero one) to actually sum.
	var projectAID uuid.UUID
	if err := adminDB.QueryRowContext(ctx, `SELECT project_id FROM applications WHERE id = $1`, appAID).Scan(&projectAID); err != nil {
		t.Fatalf("resolving org A's project id: %v", err)
	}
	if _, err := adminDB.ExecContext(ctx,
		`INSERT INTO applications (org_id, project_id, name, image, cpu_millicores, memory_mb, replicas_desired)
		 VALUES ($1, $2, 'app-2', 'nginx:latest', 500, 512, 2)`,
		orgAID, projectAID,
	); err != nil {
		t.Fatalf("seeding second application: %v", err)
	}

	// One node, and 3 containers against the deployment seedDeployment
	// created — 2 counted (pending/running), 1 not (crashed).
	var nodeID uuid.UUID
	if err := adminDB.QueryRowContext(ctx,
		`INSERT INTO nodes (hostname, ip, cpu_capacity_millicores, memory_capacity_mb) VALUES ('node-1', '10.0.0.1', 4000, 8192) RETURNING id`,
	).Scan(&nodeID); err != nil {
		t.Fatalf("seeding node: %v", err)
	}
	var deploymentID uuid.UUID
	if err := adminDB.QueryRowContext(ctx, `SELECT id FROM deployments WHERE application_id = $1`, appAID).Scan(&deploymentID); err != nil {
		t.Fatalf("resolving deployment id: %v", err)
	}
	for _, status := range []string{"running", "pending", "crashed"} {
		if _, err := adminDB.ExecContext(ctx,
			`INSERT INTO containers (deployment_id, node_id, status) VALUES ($1, $2, $3)`,
			deploymentID, nodeID, status,
		); err != nil {
			t.Fatalf("seeding %s container: %v", status, err)
		}
	}

	// A second, unrelated org, to exercise cross-tenant isolation below.
	orgBID, _, _ := seedApplication(t, ctx, adminDB, "Org B", "org-b", "app-b")

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

	// --- Create, under org A's own session ---
	if err := pool.WithTx(ctx, userAID, orgAID, func(ctx context.Context, conn Conn) error {
		q, err := NewQuotaRepository(conn).Create(ctx, orgAID, DefaultResourceQuota)
		if err != nil {
			return err
		}
		if q.OrgID != orgAID || q.MaxProjects != DefaultResourceQuota.MaxProjects || q.MaxContainers != DefaultResourceQuota.MaxContainers ||
			q.MaxCPUMillicores != DefaultResourceQuota.MaxCPUMillicores || q.MaxMemoryMB != DefaultResourceQuota.MaxMemoryMB ||
			q.MaxDeploymentsPerDay != DefaultResourceQuota.MaxDeploymentsPerDay {
			t.Fatalf("unexpected quota after Create: %+v", q)
		}
		return nil
	}); err != nil {
		t.Fatalf("creating quota for org A: %v", err)
	}

	// --- Duplicate Create for the same org -> ErrConflict ---
	if err := pool.WithTx(ctx, userAID, orgAID, func(ctx context.Context, conn Conn) error {
		_, err := NewQuotaRepository(conn).Create(ctx, orgAID, DefaultResourceQuota)
		return err
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict creating a duplicate quota row, got %v", err)
	}

	// --- Get, cross-tenant: org B's session must not see org A's quota ---
	if err := pool.WithTx(ctx, uuid.Nil, orgBID, func(ctx context.Context, conn Conn) error {
		_, err := NewQuotaRepository(conn).Get(ctx, orgAID)
		return err
	}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound getting org A's quota from org B's session, got %v", err)
	}

	// --- CurrentUsage, under org A's own session ---
	if err := pool.WithTx(ctx, userAID, orgAID, func(ctx context.Context, conn Conn) error {
		u, err := NewQuotaRepository(conn).CurrentUsage(ctx, orgAID)
		if err != nil {
			return err
		}
		want := ResourceUsage{Projects: 1, Containers: 2, CPUMillicores: 1000, MemoryMB: 1024, DeploymentsToday: 1}
		if u != want {
			t.Fatalf("unexpected usage: got %+v, want %+v", u, want)
		}
		return nil
	}); err != nil {
		t.Fatalf("computing current usage for org A: %v", err)
	}

	// --- CurrentUsage, cross-tenant: org B's session asking about org A's
	// id must see zero everywhere, not org A's real counts — RLS applies
	// independently of which orgID the caller passes in. ---
	if err := pool.WithTx(ctx, uuid.Nil, orgBID, func(ctx context.Context, conn Conn) error {
		u, err := NewQuotaRepository(conn).CurrentUsage(ctx, orgAID)
		if err != nil {
			return err
		}
		if u != (ResourceUsage{}) {
			t.Fatalf("expected org B's session to see none of org A's usage, got %+v", u)
		}
		return nil
	}); err != nil {
		t.Fatalf("computing current usage for org A from org B's session: %v", err)
	}
}

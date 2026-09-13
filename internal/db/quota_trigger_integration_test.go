//go:build integration

package db

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// pgCheckViolation is the SQLSTATE 0013_quota_enforcement_trigger.sql's
// RAISE EXCEPTION ... USING ERRCODE = 'check_violation' produces — the
// same convention pgUniqueViolation (errors.go) uses for a different
// Postgres-native guarantee.
const pgCheckViolation = "23514"

// TestQuotaEnforcementTriggers is Layer 3's (Postgres backstop) acceptance
// test for Task 6 (phase-6-multi-tenant-saas.md, ARCHITECTURE.md §2.9):
// direct SQL inserts into containers/projects — bypassing both the API
// server's Layer 1 check (quota.go) and the scheduler's Layer 2 check
// (placement.go's checkQuota) entirely, exactly as a raw INSERT from any
// other source would — are still rejected by
// 0013_quota_enforcement_trigger.sql's triggers once an org is at its
// ceiling, and still allowed exactly up to it.
func TestQuotaEnforcementTriggers(t *testing.T) {
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

	// --- containers_quota_check ---
	orgID, _, deploymentID, _ := seedDeployment(t, ctx, adminDB)
	var nodeID uuid.UUID
	if err := adminDB.QueryRowContext(ctx,
		`INSERT INTO nodes (hostname, ip, cpu_capacity_millicores, memory_capacity_mb) VALUES ('node-1', '10.0.0.1', 4000, 8192) RETURNING id`,
	).Scan(&nodeID); err != nil {
		t.Fatalf("seeding node: %v", err)
	}
	if _, err := adminDB.ExecContext(ctx,
		`INSERT INTO resource_quotas (org_id, max_cpu_millicores, max_memory_mb, max_containers, max_projects, max_deployments_per_day)
		 VALUES ($1, 8000, 16384, 1, 10, 100)`, orgID,
	); err != nil {
		t.Fatalf("seeding org quota (max_containers=1): %v", err)
	}

	// exactly at the boundary: allowed
	if _, err := adminDB.ExecContext(ctx,
		`INSERT INTO containers (deployment_id, node_id) VALUES ($1, $2)`, deploymentID, nodeID,
	); err != nil {
		t.Fatalf("expected the boundary insert (1st container, max 1) to succeed, got: %v", err)
	}

	// one more: rejected by the trigger itself, nothing upstream involved
	_, err = adminDB.ExecContext(ctx,
		`INSERT INTO containers (deployment_id, node_id) VALUES ($1, $2)`, deploymentID, nodeID,
	)
	assertQuotaViolation(t, err, "over-quota container insert")

	// --- projects_quota_check ---
	orgID2, _, _ := seedApplication(t, ctx, adminDB, "Org Two", "org-two", "app-two")
	if _, err := adminDB.ExecContext(ctx,
		`INSERT INTO resource_quotas (org_id, max_cpu_millicores, max_memory_mb, max_containers, max_projects, max_deployments_per_day)
		 VALUES ($1, 8000, 16384, 20, 1, 100)`, orgID2,
	); err != nil {
		t.Fatalf("seeding org quota (max_projects=1): %v", err)
	}
	// seedApplication already created 1 project under orgID2 — already at
	// the boundary, so one more must be rejected
	_, err = adminDB.ExecContext(ctx,
		`INSERT INTO projects (org_id, name, slug) VALUES ($1, 'Second', 'second')`, orgID2,
	)
	assertQuotaViolation(t, err, "over-quota project insert")
}

func assertQuotaViolation(t *testing.T, err error, what string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected the %s to be rejected by the trigger, got no error", what)
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != pgCheckViolation {
		t.Fatalf("expected a check_violation (%s) from the trigger for the %s, got: %v", pgCheckViolation, what, err)
	}
}

//go:build integration

package db

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// TestRLS_CrossTenantIsolation is the required RLS cross-tenant test
// (ADR-0010, Phase 1 Task 2 acceptance): two orgs, two rows in a
// tenant-scoped table, queried as each org's session — each must see only
// its own row, against real Postgres with real RLS policies active.
func TestRLS_CrossTenantIsolation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	const (
		dbName   = "platform"
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
	t.Cleanup(func() {
		_ = container.Terminate(context.Background())
	})

	adminConnStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("admin connection string: %v", err)
	}

	adminDB, err := sql.Open("pgx", adminConnStr)
	if err != nil {
		t.Fatalf("opening admin connection: %v", err)
	}
	defer func() { _ = adminDB.Close() }()

	// The postgres image restarts once internally after initdb; the
	// container's "ready" log line can be observed just before that
	// restart, so tolerate a few failed pings before giving up.
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

	endpoint, err := container.PortEndpoint(ctx, "5432/tcp", "")
	if err != nil {
		t.Fatalf("resolving container endpoint: %v", err)
	}
	appConnStr := fmt.Sprintf("postgres://platform_app:platform_app@%s/%s?sslmode=disable", endpoint, dbName)
	appDB, err := sql.Open("pgx", appConnStr)
	if err != nil {
		t.Fatalf("opening app-role connection: %v", err)
	}
	defer func() { _ = appDB.Close() }()

	var orgAID, orgBID string
	if err := adminDB.QueryRowContext(ctx,
		`INSERT INTO organizations (name, slug) VALUES ('Org A', 'org-a') RETURNING id`,
	).Scan(&orgAID); err != nil {
		t.Fatalf("seeding org A: %v", err)
	}
	if err := adminDB.QueryRowContext(ctx,
		`INSERT INTO organizations (name, slug) VALUES ('Org B', 'org-b') RETURNING id`,
	).Scan(&orgBID); err != nil {
		t.Fatalf("seeding org B: %v", err)
	}

	orgAProjectID := createProjectAs(t, ctx, appDB, orgAID, "Project A", "proj-a")
	orgBProjectID := createProjectAs(t, ctx, appDB, orgBID, "Project B", "proj-b")

	// Org A's session must see exactly its own project, and nothing of org B's.
	seenIDs := listProjectsAs(t, ctx, appDB, orgAID)
	if len(seenIDs) != 1 || seenIDs[0] != orgAProjectID {
		t.Fatalf("org A session: expected only project %s visible, got %v", orgAProjectID, seenIDs)
	}

	// Org B's session must see exactly its own project, and nothing of org A's.
	seenIDs = listProjectsAs(t, ctx, appDB, orgBID)
	if len(seenIDs) != 1 || seenIDs[0] != orgBProjectID {
		t.Fatalf("org B session: expected only project %s visible, got %v", orgBProjectID, seenIDs)
	}

	// Explicit point lookup of the other org's row must return nothing, not
	// just be absent from a listing (proves row-level filtering).
	var count int
	tx, err := appDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `SELECT set_config('app.current_org_id', $1, true)`, orgAID); err != nil {
		t.Fatalf("set_config: %v", err)
	}
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM projects WHERE id = $1`, orgBProjectID).Scan(&count); err != nil {
		t.Fatalf("point lookup of org B's project as org A: %v", err)
	}
	if count != 0 {
		t.Fatalf("org A session could see org B's project via point lookup (count=%d)", count)
	}

	// A cross-tenant insert attempt (claiming org B's id while scoped to org
	// A) must be rejected by the WITH CHECK side of the same policy.
	_, err = tx.ExecContext(ctx,
		`INSERT INTO projects (org_id, name, slug) VALUES ($1, 'Sneaky', 'sneaky')`, orgBID)
	if err == nil {
		t.Fatal("expected cross-tenant insert to be rejected by RLS, but it succeeded")
	}
}

func waitForPing(ctx context.Context, db *sql.DB) error {
	var err error
	for i := 0; i < 10; i++ {
		if err = db.PingContext(ctx); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return err
}

func createProjectAs(t *testing.T, ctx context.Context, db *sql.DB, orgID, name, slug string) string {
	t.Helper()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `SELECT set_config('app.current_org_id', $1, true)`, orgID); err != nil {
		t.Fatalf("set_config: %v", err)
	}
	var id string
	if err := tx.QueryRowContext(ctx,
		`INSERT INTO projects (org_id, name, slug) VALUES ($1, $2, $3) RETURNING id`,
		orgID, name, slug,
	).Scan(&id); err != nil {
		t.Fatalf("creating project for org %s: %v", orgID, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return id
}

func listProjectsAs(t *testing.T, ctx context.Context, db *sql.DB, orgID string) []string {
	t.Helper()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `SELECT set_config('app.current_org_id', $1, true)`, orgID); err != nil {
		t.Fatalf("set_config: %v", err)
	}
	rows, err := tx.QueryContext(ctx, `SELECT id FROM projects`)
	if err != nil {
		t.Fatalf("listing projects for org %s: %v", orgID, err)
	}
	defer func() { _ = rows.Close() }()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scanning project id: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating projects: %v", err)
	}
	return ids
}

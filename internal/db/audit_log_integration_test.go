//go:build integration

package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// TestAuditLogRepository covers Task 1's acceptance
// (phase-6-multi-tenant-saas.md): the 0011_audit_logs.sql migration applies
// cleanly on top of 0001-0010, AuditLogRepository's Record/ListByOrg work
// against real Postgres with real RLS enforcing cross-tenant isolation,
// ListByOrg's cursor pagination actually pages (rather than just returning
// everything under a limit that happens to be big enough), and — this
// table's own defining guarantee — a direct UPDATE/DELETE from the
// platform_app role is rejected outright by Postgres itself, not merely
// absent from AuditLogRepository's own method set.
func TestAuditLogRepository(t *testing.T) {
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

	orgAID, _, _ := seedApplication(t, ctx, adminDB, "Org A", "org-a", "app-a")
	orgBID, _, _ := seedApplication(t, ctx, adminDB, "Org B", "org-b", "app-b")

	var actorID uuid.UUID
	if err := adminDB.QueryRowContext(ctx,
		`INSERT INTO users (email, password_hash) VALUES ('actor@example.com', 'hash') RETURNING id`,
	).Scan(&actorID); err != nil {
		t.Fatalf("seeding actor user: %v", err)
	}

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

	// --- Record three entries for org A, one for org B, each in its own
	// transaction so real wall-clock time (not a shared transaction
	// snapshot) separates their created_at values for the pagination check
	// below. actorID is nil on the middle entry to exercise the
	// system-initiated-action path (ActorUserID nullable). ---
	targetID := uuid.New()
	wantActions := map[string]bool{"action-1": true, "action-2": true, "action-3": true}
	for i, action := range []string{"action-1", "action-2", "action-3"} {
		var actor *uuid.UUID
		if i != 1 {
			actor = &actorID
		}
		entry := AuditLogEntry{
			OrgID:       orgAID,
			ActorUserID: actor,
			Action:      action,
			TargetType:  "application",
			TargetID:    targetID,
			Metadata:    json.RawMessage(`{"k":"v"}`),
		}
		if err := pool.WithTx(ctx, uuid.Nil, orgAID, func(ctx context.Context, conn Conn) error {
			return NewAuditLogRepository(conn).Record(ctx, entry)
		}); err != nil {
			t.Fatalf("recording %s: %v", action, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := pool.WithTx(ctx, uuid.Nil, orgBID, func(ctx context.Context, conn Conn) error {
		return NewAuditLogRepository(conn).Record(ctx, AuditLogEntry{
			OrgID: orgBID, Action: "org-b-action", TargetType: "application", TargetID: uuid.New(),
		})
	}); err != nil {
		t.Fatalf("recording org B's entry: %v", err)
	}

	// --- ListByOrg, paginated with limit=2: page 1 has a nextCursor, page
	// 2 doesn't, and the union of both pages is exactly org A's 3 entries,
	// no duplicates, no entries missing, none of org B's. ---
	var page1, page2 []AuditLogEntry
	var cursor string
	if err := pool.WithTx(ctx, uuid.Nil, orgAID, func(ctx context.Context, conn Conn) error {
		var err error
		page1, cursor, err = NewAuditLogRepository(conn).ListByOrg(ctx, orgAID, "", 2)
		return err
	}); err != nil {
		t.Fatalf("listing page 1: %v", err)
	}
	if len(page1) != 2 {
		t.Fatalf("expected page 1 to have 2 entries, got %d: %+v", len(page1), page1)
	}
	if cursor == "" {
		t.Fatalf("expected page 1 to return a non-empty next cursor")
	}

	var nextCursor string
	if err := pool.WithTx(ctx, uuid.Nil, orgAID, func(ctx context.Context, conn Conn) error {
		var err error
		page2, nextCursor, err = NewAuditLogRepository(conn).ListByOrg(ctx, orgAID, cursor, 2)
		return err
	}); err != nil {
		t.Fatalf("listing page 2: %v", err)
	}
	if len(page2) != 1 {
		t.Fatalf("expected page 2 to have exactly the 1 remaining entry, got %d: %+v", len(page2), page2)
	}
	if nextCursor != "" {
		t.Fatalf("expected no next cursor once every entry has been returned, got %q", nextCursor)
	}

	seen := make(map[string]bool)
	for _, e := range append(page1, page2...) {
		if e.OrgID != orgAID {
			t.Fatalf("expected every entry to belong to org A, got %+v", e)
		}
		seen[e.Action] = true
	}
	if len(seen) != len(wantActions) {
		t.Fatalf("expected exactly %v across both pages, got %v", wantActions, seen)
	}
	for action := range wantActions {
		if !seen[action] {
			t.Fatalf("expected %q among the paginated results, got %v", action, seen)
		}
	}

	// --- Cross-tenant: org B's session must see none of org A's entries. ---
	if err := pool.WithTx(ctx, uuid.Nil, orgBID, func(ctx context.Context, conn Conn) error {
		entries, _, err := NewAuditLogRepository(conn).ListByOrg(ctx, orgAID, "", 10)
		if err != nil {
			return err
		}
		if len(entries) != 0 {
			t.Fatalf("expected org B's session to see none of org A's audit log, got %+v", entries)
		}
		return nil
	}); err != nil {
		t.Fatalf("cross-tenant isolation check: %v", err)
	}

	// --- Append-only, enforced at the DB layer regardless of caller code:
	// platform_app has had UPDATE/DELETE revoked on this table entirely
	// (0011_audit_logs.sql), so both fail with insufficient_privilege
	// (pgcode 42501) before RLS or WHERE-clause matching is even
	// considered — this holds even for a row id that doesn't exist. ---
	const insufficientPrivilege = "42501"
	if _, err := pool.Conn().Exec(ctx, `UPDATE audit_logs SET action = 'tampered' WHERE id = $1`, uuid.New()); err == nil {
		t.Fatalf("expected UPDATE against audit_logs to be rejected, it succeeded")
	} else {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != insufficientPrivilege {
			t.Fatalf("expected insufficient_privilege (%s) rejecting UPDATE, got %v", insufficientPrivilege, err)
		}
	}
	if _, err := pool.Conn().Exec(ctx, `DELETE FROM audit_logs WHERE id = $1`, uuid.New()); err == nil {
		t.Fatalf("expected DELETE against audit_logs to be rejected, it succeeded")
	} else {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != insufficientPrivilege {
			t.Fatalf("expected insufficient_privilege (%s) rejecting DELETE, got %v", insufficientPrivilege, err)
		}
	}
}

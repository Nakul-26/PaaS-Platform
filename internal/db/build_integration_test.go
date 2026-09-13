//go:build integration

package db

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// TestBuildRepository covers Task 1's acceptance
// (phase-7-deployment-platform.md): the 0014_builds.sql migration applies
// cleanly on top of 0001-0013, BuildRepository's Create/Get/ListByApplication/
// OrgID/UpdateResult work against real Postgres with real RLS enforcing
// cross-tenant isolation, and ListByApplication's cursor pagination actually
// pages — same coverage shape TestAuditLogRepository already established
// for the sibling append-heavy, cursor-paginated table.
func TestBuildRepository(t *testing.T) {
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

	orgAID, _, appAID := seedApplication(t, ctx, adminDB, "Org A", "org-a", "app-a")
	orgBID, _, appBID := seedApplication(t, ctx, adminDB, "Org B", "org-b", "app-b")

	var creatorID uuid.UUID
	if err := adminDB.QueryRowContext(ctx,
		`INSERT INTO users (email, password_hash) VALUES ('creator@example.com', 'hash') RETURNING id`,
	).Scan(&creatorID); err != nil {
		t.Fatalf("seeding creator user: %v", err)
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

	// --- Create three builds for org A's app, one for org B's, each in its
	// own transaction so real wall-clock time separates created_at for the
	// pagination check below. ---
	wantRefs := map[string]bool{"ref-1": true, "ref-2": true, "ref-3": true}
	for _, ref := range []string{"ref-1", "ref-2", "ref-3"} {
		if err := pool.WithTx(ctx, creatorID, orgAID, func(ctx context.Context, conn Conn) error {
			b, err := NewBuildRepository(conn).Create(ctx, orgAID, appAID, "https://example.com/org-a/repo.git", ref, creatorID)
			if err != nil {
				return err
			}
			if b.Status != BuildStatusPending {
				t.Fatalf("expected a freshly created build to default to status pending, got %q", b.Status)
			}
			if b.GitRef != ref {
				t.Fatalf("expected git_ref %q, got %q", ref, b.GitRef)
			}
			return nil
		}); err != nil {
			t.Fatalf("creating build %s: %v", ref, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := pool.WithTx(ctx, creatorID, orgBID, func(ctx context.Context, conn Conn) error {
		_, err := NewBuildRepository(conn).Create(ctx, orgBID, appBID, "https://example.com/org-b/repo.git", "main", creatorID)
		return err
	}); err != nil {
		t.Fatalf("creating org B's build: %v", err)
	}

	// --- Default git_ref: an empty ref falls through to 'main' at the DB
	// layer (Create's own doc comment). ---
	var defaultedRefBuild Build
	if err := pool.WithTx(ctx, creatorID, orgAID, func(ctx context.Context, conn Conn) error {
		var err error
		defaultedRefBuild, err = NewBuildRepository(conn).Create(ctx, orgAID, appAID, "https://example.com/org-a/repo.git", "", creatorID)
		return err
	}); err != nil {
		t.Fatalf("creating build with empty git_ref: %v", err)
	}
	if defaultedRefBuild.GitRef != "main" {
		t.Fatalf("expected an empty git_ref to default to 'main', got %q", defaultedRefBuild.GitRef)
	}

	// --- ListByApplication, paginated with limit=2: page 1 has a
	// nextCursor, page 2 doesn't, and the union of both pages is exactly
	// org A's 3 originally-seeded builds plus the default-ref one, no
	// duplicates, none of org B's. ---
	var page1, page2 []Build
	var cursor string
	if err := pool.WithTx(ctx, creatorID, orgAID, func(ctx context.Context, conn Conn) error {
		var err error
		page1, cursor, err = NewBuildRepository(conn).ListByApplication(ctx, appAID, 2, "")
		return err
	}); err != nil {
		t.Fatalf("listing page 1: %v", err)
	}
	if len(page1) != 2 {
		t.Fatalf("expected page 1 to have 2 builds, got %d: %+v", len(page1), page1)
	}
	if cursor == "" {
		t.Fatalf("expected page 1 to return a non-empty next cursor")
	}

	var nextCursor string
	if err := pool.WithTx(ctx, creatorID, orgAID, func(ctx context.Context, conn Conn) error {
		var err error
		page2, nextCursor, err = NewBuildRepository(conn).ListByApplication(ctx, appAID, 2, cursor)
		return err
	}); err != nil {
		t.Fatalf("listing page 2: %v", err)
	}
	if len(page2) != 2 {
		t.Fatalf("expected page 2 to have the 2 remaining builds, got %d: %+v", len(page2), page2)
	}
	if nextCursor != "" {
		t.Fatalf("expected no next cursor once every build has been returned, got %q", nextCursor)
	}

	seen := make(map[string]bool)
	for _, b := range append(page1, page2...) {
		if b.OrgID != orgAID {
			t.Fatalf("expected every build to belong to org A, got %+v", b)
		}
		seen[b.GitRef] = true
	}
	for ref := range wantRefs {
		if !seen[ref] {
			t.Fatalf("expected %q among the paginated results, got %v", ref, seen)
		}
	}
	if !seen["main"] {
		t.Fatalf("expected the default-ref build among the paginated results, got %v", seen)
	}

	// --- Cross-tenant: org B's session must see none of org A's builds,
	// and OrgID resolution for one of org A's ids must fail closed (same
	// posture ProjectRepository.OrgID/DomainRepository.OrgID already
	// establish — id lookups aren't scoped by an explicit WHERE org_id
	// clause, RLS is the only thing stopping cross-tenant reads here). ---
	if err := pool.WithTx(ctx, creatorID, orgBID, func(ctx context.Context, conn Conn) error {
		builds, _, err := NewBuildRepository(conn).ListByApplication(ctx, appAID, 10, "")
		if err != nil {
			return err
		}
		if len(builds) != 0 {
			t.Fatalf("expected org B's session to see none of org A's builds, got %+v", builds)
		}
		_, err = NewBuildRepository(conn).OrgID(ctx, page1[0].ID)
		if err != ErrNotFound {
			t.Fatalf("expected org B's session to get ErrNotFound resolving org A's build id, got %v", err)
		}
		return nil
	}); err != nil {
		t.Fatalf("cross-tenant isolation check: %v", err)
	}

	// --- OrgID resolves correctly from the owning org's own session. ---
	if err := pool.WithTx(ctx, creatorID, orgAID, func(ctx context.Context, conn Conn) error {
		resolvedOrgID, err := NewBuildRepository(conn).OrgID(ctx, page1[0].ID)
		if err != nil {
			return err
		}
		if resolvedOrgID != orgAID {
			t.Fatalf("expected OrgID to resolve to org A, got %v", resolvedOrgID)
		}
		return nil
	}); err != nil {
		t.Fatalf("OrgID resolution check: %v", err)
	}

	// --- Get, exercised the way apiserver's build.completed consumer
	// actually calls it (phase-7-deployment-platform.md Task 4): with
	// app.current_org_id set from the message's org hint and *no*
	// app.current_user_id, since a NATS message carries no session. That
	// leaves the policy's first branch as the only one in play, which is
	// precisely what makes a wrong org hint fail closed rather than reach
	// another tenant's row — the property the consumer's whole
	// trust-the-row-not-the-message design rests on. ---
	if err := pool.WithTx(ctx, uuid.Nil, orgAID, func(ctx context.Context, conn Conn) error {
		got, err := NewBuildRepository(conn).Get(ctx, page1[0].ID)
		if err != nil {
			return err
		}
		if got.ID != page1[0].ID || got.OrgID != orgAID || got.ApplicationID != appAID || got.CreatedBy != creatorID {
			t.Fatalf("Get returned the wrong build: %+v", got)
		}
		return nil
	}); err != nil {
		t.Fatalf("Get with only an org session: %v", err)
	}
	if err := pool.WithTx(ctx, uuid.Nil, orgBID, func(ctx context.Context, conn Conn) error {
		_, err := NewBuildRepository(conn).Get(ctx, page1[0].ID)
		return err
	}); err != ErrNotFound {
		t.Fatalf("expected a wrong org hint to fail closed with ErrNotFound getting org A's build, got %v", err)
	}
	if err := pool.WithTx(ctx, creatorID, orgAID, func(ctx context.Context, conn Conn) error {
		_, err := NewBuildRepository(conn).Get(ctx, uuid.New())
		return err
	}); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound getting a nonexistent build, got %v", err)
	}

	// --- UpdateResult: a successful outcome carries commit_sha/image and
	// no error; a failed one carries error_message and no commit_sha/image. ---
	if err := pool.WithTx(ctx, creatorID, orgAID, func(ctx context.Context, conn Conn) error {
		repo := NewBuildRepository(conn)
		commitSHA, image := "abc1234", "localhost:5000/org-a/app-a:abc1234"
		if err := repo.UpdateResult(ctx, page1[0].ID, BuildStatusSucceeded, &commitSHA, &image, nil); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("recording a successful build result: %v", err)
	}
	var succeeded Build
	if err := pool.WithTx(ctx, creatorID, orgAID, func(ctx context.Context, conn Conn) error {
		builds, _, err := NewBuildRepository(conn).ListByApplication(ctx, appAID, 10, "")
		if err != nil {
			return err
		}
		for _, b := range builds {
			if b.ID == page1[0].ID {
				succeeded = b
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("re-reading the updated build: %v", err)
	}
	if succeeded.Status != BuildStatusSucceeded || succeeded.CommitSHA == nil || *succeeded.CommitSHA != "abc1234" || succeeded.Image == nil || succeeded.CompletedAt == nil {
		t.Fatalf("expected a fully populated succeeded build, got %+v", succeeded)
	}

	errMsg := "Dockerfile not found at repo root"
	if err := pool.WithTx(ctx, creatorID, orgAID, func(ctx context.Context, conn Conn) error {
		return NewBuildRepository(conn).UpdateResult(ctx, page1[1].ID, BuildStatusFailed, nil, nil, &errMsg)
	}); err != nil {
		t.Fatalf("recording a failed build result: %v", err)
	}
	var failed Build
	if err := pool.WithTx(ctx, creatorID, orgAID, func(ctx context.Context, conn Conn) error {
		builds, _, err := NewBuildRepository(conn).ListByApplication(ctx, appAID, 10, "")
		if err != nil {
			return err
		}
		for _, b := range builds {
			if b.ID == page1[1].ID {
				failed = b
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("re-reading the updated build: %v", err)
	}
	if failed.Status != BuildStatusFailed || failed.ErrorMessage == nil || *failed.ErrorMessage != errMsg || failed.CommitSHA != nil || failed.Image != nil {
		t.Fatalf("expected a failed build with only error_message populated, got %+v", failed)
	}

	// --- UpdateResult against a nonexistent build id fails closed. ---
	if err := pool.WithTx(ctx, creatorID, orgAID, func(ctx context.Context, conn Conn) error {
		return NewBuildRepository(conn).UpdateResult(ctx, uuid.New(), BuildStatusFailed, nil, nil, &errMsg)
	}); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound updating a nonexistent build, got %v", err)
	}
}

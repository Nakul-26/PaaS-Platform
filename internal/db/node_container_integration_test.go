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

// TestNodeAndContainerRepositories covers Task 2's acceptance
// (phase-2-multi-node.md): the 0007_nodes_containers.sql migration applies
// cleanly on top of 0001-0006, and NodeRepository/ContainerRepository work
// against real Postgres. Neither table carries RLS, so this runs against
// the platform_app role directly, no WithTx session variables needed.
func TestNodeAndContainerRepositories(t *testing.T) {
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

	orgID, userID, deploymentID, applicationID := seedDeployment(t, ctx, adminDB)

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

	nodes := NewNodeRepository(pool.Conn())
	containers := NewContainerRepository(pool.Conn())

	nodeID := uuid.New()
	node, err := nodes.Register(ctx, nodeID, "worker-1", "172.20.0.3", 2000, 2048, map[string]string{"zone": "local"})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if node.Status != NodeStatusHealthy {
		t.Fatalf("expected new node healthy, got %s", node.Status)
	}

	// Re-registration (a worker restart) upserts the same row rather than
	// creating a second one, and revives it from unreachable.
	if err := nodes.SetStatus(ctx, nodeID, NodeStatusUnreachable); err != nil {
		t.Fatalf("SetStatus: %v", err)
	}
	node, err = nodes.Register(ctx, nodeID, "worker-1", "172.20.0.3", 2000, 2048, nil)
	if err != nil {
		t.Fatalf("re-Register: %v", err)
	}
	if node.Status != NodeStatusHealthy {
		t.Fatalf("expected re-registration to revive node to healthy, got %s", node.Status)
	}

	allNodes, err := nodes.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(allNodes) != 1 {
		t.Fatalf("expected exactly one node after upsert, got %d", len(allNodes))
	}

	if err := nodes.Heartbeat(ctx, nodeID, time.Now()); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	c, err := containers.Create(ctx, deploymentID, nodeID)
	if err != nil {
		t.Fatalf("Create container: %v", err)
	}
	if c.Status != ContainerStatusPending {
		t.Fatalf("expected new container pending, got %s", c.Status)
	}

	active, err := containers.CountActiveByDeployment(ctx, deploymentID)
	if err != nil {
		t.Fatalf("CountActiveByDeployment (pending): %v", err)
	}
	if active != 1 {
		t.Fatalf("expected 1 active container while pending, got %d", active)
	}

	runtimeID := "abc123"
	if err := containers.UpdateStatus(ctx, c.ID, ContainerStatusRunning, &runtimeID); err != nil {
		t.Fatalf("UpdateStatus: %v", err)
	}
	c, err = containers.Get(ctx, c.ID)
	if err != nil {
		t.Fatalf("Get container: %v", err)
	}
	if c.Status != ContainerStatusRunning || c.ContainerRuntimeID == nil || *c.ContainerRuntimeID != runtimeID || c.StartedAt == nil {
		t.Fatalf("unexpected container after UpdateStatus: %+v", c)
	}

	active, err = containers.CountActiveByDeployment(ctx, deploymentID)
	if err != nil {
		t.Fatalf("CountActiveByDeployment (running): %v", err)
	}
	if active != 1 {
		t.Fatalf("expected 1 active container while running, got %d", active)
	}

	byDeployment, err := containers.ListByDeployment(ctx, deploymentID)
	if err != nil {
		t.Fatalf("ListByDeployment: %v", err)
	}
	if len(byDeployment) != 1 || byDeployment[0].ID != c.ID {
		t.Fatalf("expected one container for deployment, got %v", byDeployment)
	}

	byNode, err := containers.ListByNode(ctx, nodeID)
	if err != nil {
		t.Fatalf("ListByNode: %v", err)
	}
	if len(byNode) != 1 || byNode[0].ID != c.ID {
		t.Fatalf("expected one container for node, got %v", byNode)
	}

	if err := containers.UpdateStatus(ctx, c.ID, ContainerStatusStopped, nil); err != nil {
		t.Fatalf("UpdateStatus (stop): %v", err)
	}
	c, err = containers.Get(ctx, c.ID)
	if err != nil {
		t.Fatalf("Get container after stop: %v", err)
	}
	if c.Status != ContainerStatusStopped || c.StoppedAt == nil {
		t.Fatalf("expected stopped container with stopped_at set, got %+v", c)
	}
	if c.ContainerRuntimeID == nil || *c.ContainerRuntimeID != runtimeID {
		t.Fatalf("expected container_runtime_id to survive a nil-runtime-id update, got %v", c.ContainerRuntimeID)
	}

	active, err = containers.CountActiveByDeployment(ctx, deploymentID)
	if err != nil {
		t.Fatalf("CountActiveByDeployment (stopped): %v", err)
	}
	if active != 0 {
		t.Fatalf("expected 0 active containers once stopped, got %d", active)
	}

	// ApplicationRepository.Get requires app.current_org_id set (ADR-0010),
	// so this runs inside a WithTx session scoped to the seeded org/user,
	// matching how apiserver's handlers use it.
	err = pool.WithTx(ctx, userID, orgID, func(ctx context.Context, conn Conn) error {
		applications := NewApplicationRepository(conn)

		app, err := applications.Get(ctx, applicationID)
		if err != nil {
			return fmt.Errorf("Get application: %w", err)
		}
		if app.ReplicasDesired != 1 {
			return fmt.Errorf("expected default replicas_desired 1, got %d", app.ReplicasDesired)
		}
		if err := applications.UpdateReplicas(ctx, applicationID, 3); err != nil {
			return fmt.Errorf("UpdateReplicas: %w", err)
		}
		app, err = applications.Get(ctx, applicationID)
		if err != nil {
			return fmt.Errorf("Get application after UpdateReplicas: %w", err)
		}
		if app.ReplicasDesired != 3 {
			return fmt.Errorf("expected replicas_desired 3 after UpdateReplicas, got %d", app.ReplicasDesired)
		}

		if err := applications.UpdateReplicas(ctx, uuid.New(), 5); !errors.Is(err, ErrNotFound) {
			return fmt.Errorf("expected ErrNotFound updating replicas for unknown application, got %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("application RLS session: %v", err)
	}
}

// seedDeployment inserts one organization/user/project/application/deployment
// row (as the admin, bypassing RLS) so a containers row has a real
// deployment_id fk to point at. Returns both IDs since Task 3's
// ApplicationRepository.UpdateReplicas coverage needs the org/user too, to
// scope the RLS session ApplicationRepository.Get requires (ADR-0010).
func seedDeployment(t *testing.T, ctx context.Context, adminDB *sql.DB) (orgID, userID, deploymentID, applicationID uuid.UUID) {
	t.Helper()

	var projectID uuid.UUID
	if err := adminDB.QueryRowContext(ctx,
		`INSERT INTO organizations (name, slug) VALUES ('Org', 'org') RETURNING id`,
	).Scan(&orgID); err != nil {
		t.Fatalf("seeding organization: %v", err)
	}
	if err := adminDB.QueryRowContext(ctx,
		`INSERT INTO users (email, password_hash) VALUES ('user@example.com', 'hash') RETURNING id`,
	).Scan(&userID); err != nil {
		t.Fatalf("seeding user: %v", err)
	}
	if err := adminDB.QueryRowContext(ctx,
		`INSERT INTO projects (org_id, name, slug) VALUES ($1, 'Project', 'project') RETURNING id`, orgID,
	).Scan(&projectID); err != nil {
		t.Fatalf("seeding project: %v", err)
	}
	if err := adminDB.QueryRowContext(ctx,
		`INSERT INTO applications (org_id, project_id, name, image) VALUES ($1, $2, 'app', 'nginx:latest') RETURNING id`,
		orgID, projectID,
	).Scan(&applicationID); err != nil {
		t.Fatalf("seeding application: %v", err)
	}
	if err := adminDB.QueryRowContext(ctx,
		`INSERT INTO deployments (org_id, application_id, image, revision, created_by) VALUES ($1, $2, 'nginx:latest', 1, $3) RETURNING id`,
		orgID, applicationID, userID,
	).Scan(&deploymentID); err != nil {
		t.Fatalf("seeding deployment: %v", err)
	}
	return orgID, userID, deploymentID, applicationID
}

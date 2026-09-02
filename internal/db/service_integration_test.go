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

// TestServiceAndServiceInstanceRepositories covers Task 1's acceptance
// (phase-4-service-discovery-lb.md): the 0008_services_service_instances.sql
// migration applies cleanly on top of 0001-0007, and
// ServiceRepository/ServiceInstanceRepository work against real Postgres.
// Neither table carries RLS, so this runs against the platform_app role
// directly, no WithTx session variables needed.
func TestServiceAndServiceInstanceRepositories(t *testing.T) {
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

	_, _, deploymentID, applicationID := seedDeployment(t, ctx, adminDB)

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
	services := NewServiceRepository(pool.Conn())
	instances := NewServiceInstanceRepository(pool.Conn())

	nodeID := uuid.New()
	if _, err := nodes.Register(ctx, nodeID, "worker-1", "172.20.0.3", 2000, 2048, nil); err != nil {
		t.Fatalf("registering node: %v", err)
	}
	c, err := containers.Create(ctx, deploymentID, nodeID)
	if err != nil {
		t.Fatalf("creating container: %v", err)
	}

	svc, err := services.GetOrCreate(ctx, applicationID, "demo-app.internal")
	if err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}
	if svc.ApplicationID != applicationID || svc.DNSName != "demo-app.internal" {
		t.Fatalf("unexpected service after GetOrCreate: %+v", svc)
	}

	// A second GetOrCreate for the same application is idempotent: same row
	// returned, dns_name from the first call preserved (Task 3's lazy-create
	// only ever runs once per application in practice, but the controller's
	// reconcile tick calls this every tick regardless).
	svcAgain, err := services.GetOrCreate(ctx, applicationID, "should-be-ignored.internal")
	if err != nil {
		t.Fatalf("GetOrCreate (again): %v", err)
	}
	if svcAgain.ID != svc.ID || svcAgain.DNSName != "demo-app.internal" {
		t.Fatalf("expected GetOrCreate to be idempotent, got %+v", svcAgain)
	}

	byApp, err := services.GetByApplication(ctx, applicationID)
	if err != nil {
		t.Fatalf("GetByApplication: %v", err)
	}
	if byApp.ID != svc.ID {
		t.Fatalf("expected GetByApplication to return the same service, got %+v", byApp)
	}

	if _, err := services.GetByApplication(ctx, uuid.New()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for unknown application, got %v", err)
	}

	inst, err := instances.Upsert(ctx, svc.ID, c.ID, "172.20.0.5", 32768)
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if !inst.Healthy || inst.IP != "172.20.0.5" || inst.Port != 32768 {
		t.Fatalf("unexpected instance after Upsert: %+v", inst)
	}

	healthy, err := instances.ListHealthyByService(ctx, svc.ID)
	if err != nil {
		t.Fatalf("ListHealthyByService: %v", err)
	}
	if len(healthy) != 1 || healthy[0].ContainerID != c.ID {
		t.Fatalf("expected one healthy instance, got %v", healthy)
	}

	// Re-upserting the same container (the next reconcile tick observing the
	// same running container) refreshes the row in place rather than
	// duplicating it.
	if _, err := instances.Upsert(ctx, svc.ID, c.ID, "172.20.0.5", 40000); err != nil {
		t.Fatalf("re-Upsert: %v", err)
	}
	healthy, err = instances.ListHealthyByService(ctx, svc.ID)
	if err != nil {
		t.Fatalf("ListHealthyByService (after re-Upsert): %v", err)
	}
	if len(healthy) != 1 || healthy[0].Port != 40000 {
		t.Fatalf("expected re-Upsert to update the existing row in place, got %v", healthy)
	}

	if err := instances.MarkUnhealthy(ctx, c.ID); err != nil {
		t.Fatalf("MarkUnhealthy: %v", err)
	}
	healthy, err = instances.ListHealthyByService(ctx, svc.ID)
	if err != nil {
		t.Fatalf("ListHealthyByService (after MarkUnhealthy): %v", err)
	}
	if len(healthy) != 0 {
		t.Fatalf("expected no healthy instances after MarkUnhealthy, got %v", healthy)
	}

	// A subsequent Upsert (the instance came back healthy) revives it.
	if _, err := instances.Upsert(ctx, svc.ID, c.ID, "172.20.0.5", 40000); err != nil {
		t.Fatalf("re-Upsert (revive): %v", err)
	}
	healthy, err = instances.ListHealthyByService(ctx, svc.ID)
	if err != nil {
		t.Fatalf("ListHealthyByService (after revive): %v", err)
	}
	if len(healthy) != 1 {
		t.Fatalf("expected instance revived to healthy, got %v", healthy)
	}

	if err := instances.DeleteByContainer(ctx, c.ID); err != nil {
		t.Fatalf("DeleteByContainer: %v", err)
	}
	healthy, err = instances.ListHealthyByService(ctx, svc.ID)
	if err != nil {
		t.Fatalf("ListHealthyByService (after delete): %v", err)
	}
	if len(healthy) != 0 {
		t.Fatalf("expected no instances after delete, got %v", healthy)
	}

	if err := instances.DeleteByContainer(ctx, c.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound deleting an already-deleted instance, got %v", err)
	}
	if err := instances.MarkUnhealthy(ctx, uuid.New()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound marking an unknown container's instance unhealthy, got %v", err)
	}
}

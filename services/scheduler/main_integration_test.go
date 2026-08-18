//go:build integration

// Package main's integration test covers Task 5's acceptance
// (phase-2-multi-node.md): against a real Postgres, a real NATS instance,
// and two real worker processes (each with its own real Docker daemon
// access), a placement.requested event results in exactly one containers
// row pointing at a healthy node, and that node actually receives and
// starts the assignment.
package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go/modules/nats"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"platform/internal/db"
	"platform/internal/eventbus"
	"platform/internal/runtime"
)

func TestScheduler_PlacesDeploymentOnHealthyWorker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	rt, err := runtime.NewDockerRuntime()
	if err != nil {
		t.Skipf("docker daemon not available, skipping: %v", err)
	}
	defer func() { _ = rt.Close() }()

	dbURL, adminDB, pool := startTestPostgres(t, ctx)
	defer func() { _ = adminDB.Close() }()

	deploymentID := seedDeployment(t, ctx, adminDB)

	natsContainer, err := nats.Run(ctx, "nats:2.11.7")
	if err != nil {
		t.Fatalf("starting nats container: %v", err)
	}
	t.Cleanup(func() { _ = natsContainer.Terminate(context.Background()) })

	natsURL, err := natsContainer.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("nats connection string: %v", err)
	}

	bus, err := eventbus.Connect(natsURL)
	if err != nil {
		t.Fatalf("connecting eventbus: %v", err)
	}
	t.Cleanup(func() { _ = bus.Close() })

	registrations := make(chan map[string]any, 8)
	regSub, err := bus.Subscribe("node.*.register", func(msg eventbus.Message) {
		var payload map[string]any
		if err := json.Unmarshal(msg.Data, &payload); err == nil {
			registrations <- payload
		}
	})
	if err != nil {
		t.Fatalf("subscribing to node.*.register: %v", err)
	}
	defer func() { _ = regSub.Unsubscribe() }()

	startTestScheduler(t, ctx, dbURL, natsURL)
	startTestWorker(t, ctx, natsURL)
	startTestWorker(t, ctx, natsURL)

	nodeIDs := map[string]bool{}
	deadline := time.After(20 * time.Second)
	for len(nodeIDs) < 2 {
		select {
		case reg := <-registrations:
			if id, _ := reg["node_id"].(string); id != "" {
				nodeIDs[id] = true
			}
		case <-deadline:
			t.Fatalf("timed out waiting for 2 node registrations, only saw %d", len(nodeIDs))
		}
	}

	// The scheduler ensures PLACEMENT/NODE_ASSIGNMENTS on startup
	// (subscribePlacement); this test's own EnsureStream is an idempotent
	// no-op layered on top, closing the race the same way Task 4's test
	// closed its NODE_STATUS race.
	if err := bus.EnsureStream(ctx, eventbus.StreamConfig{
		Name:     eventbus.PlacementStream,
		Subjects: []string{eventbus.PlacementStreamFilter},
	}); err != nil {
		t.Fatalf("ensuring %s stream: %v", eventbus.PlacementStream, err)
	}

	placementData, err := json.Marshal(map[string]any{
		"deployment_id":  deploymentID.String(),
		"application_id": uuid.New().String(),
		"image":          "nginx:latest",
		"ports":          []map[string]any{{"container_port": 80}},
	})
	if err != nil {
		t.Fatalf("marshaling placement.requested message: %v", err)
	}
	if err := bus.PublishDurable(ctx, eventbus.PlacementRequestedSubject, placementData); err != nil {
		t.Fatalf("publishing placement.requested: %v", err)
	}

	containers := db.NewContainerRepository(pool.Conn())

	var placed db.Container
	deadline = time.After(20 * time.Second)
	for {
		rows, err := containers.ListByDeployment(ctx, deploymentID)
		if err != nil {
			t.Fatalf("ListByDeployment: %v", err)
		}
		if len(rows) == 1 {
			placed = rows[0]
			break
		}
		if len(rows) > 1 {
			t.Fatalf("expected exactly one container for deployment, got %d: %+v", len(rows), rows)
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for a containers row to appear")
		case <-time.After(200 * time.Millisecond):
		}
	}

	if !nodeIDs[placed.NodeID.String()] {
		t.Fatalf("container placed on node %s, want one of the registered workers %v", placed.NodeID, nodeIDs)
	}

	deadline = time.After(30 * time.Second)
	for placed.Status != db.ContainerStatusRunning {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for container to reach running, last status %q", placed.Status)
		case <-time.After(200 * time.Millisecond):
		}
		placed, err = containers.Get(ctx, placed.ID)
		if err != nil {
			t.Fatalf("Get container: %v", err)
		}
	}

	if placed.ContainerRuntimeID == nil || *placed.ContainerRuntimeID == "" {
		t.Fatalf("expected a container_runtime_id once running, got %+v", placed)
	}

	containerID := *placed.ContainerRuntimeID
	t.Cleanup(func() {
		_ = rt.StopContainer(context.Background(), containerID, 5*time.Second)
		_ = rt.RemoveContainer(context.Background(), containerID)
	})

	info, err := rt.ContainerStatus(ctx, containerID)
	if err != nil {
		t.Fatalf("inspecting container %s on the real docker daemon: %v", containerID, err)
	}
	if info.Status != runtime.StatusRunning {
		t.Fatalf("expected container %s to actually be running on the docker daemon, got status %q", containerID, info.Status)
	}
}

// startTestPostgres starts a real Postgres container, applies migrations
// 0001-0007, and returns the app-role connection string (for the
// subprocesses under test), an admin *sql.DB (for seeding), and an
// app-role *db.Pool (for reading back scheduler results).
func startTestPostgres(t *testing.T, ctx context.Context) (string, *sql.DB, *db.Pool) {
	t.Helper()

	container, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("platform"),
		postgres.WithUsername("platform"),
		postgres.WithPassword("platform"),
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

	var pingErr error
	for i := 0; i < 30; i++ {
		if pingErr = adminDB.PingContext(ctx); pingErr == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if pingErr != nil {
		t.Fatalf("waiting for postgres to accept connections: %v", pingErr)
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
	appConnStr := fmt.Sprintf("postgres://platform_app:platform_app@%s/platform?sslmode=disable", endpoint)

	pool, err := db.Open(ctx, appConnStr)
	if err != nil {
		t.Fatalf("opening app-role pool: %v", err)
	}
	t.Cleanup(pool.Close)

	return appConnStr, adminDB, pool
}

// seedDeployment inserts one organization/user/project/application/deployment
// row (as the admin, bypassing RLS) so a placement.requested event has a
// real deployment_id to point at. Mirrors internal/db's own
// node_container_integration_test.go seedDeployment — duplicated here
// rather than imported, since that helper lives in an unexported _test.go
// file in a different package.
func seedDeployment(t *testing.T, ctx context.Context, adminDB *sql.DB) uuid.UUID {
	t.Helper()

	var orgID, userID, projectID, applicationID, deploymentID uuid.UUID
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
	return deploymentID
}

// startTestScheduler builds and runs the real scheduler binary as a
// separate OS process (ADR-0012), pointed at dbURL/natsURL with fast
// liveness-sweep settings.
func startTestScheduler(t *testing.T, ctx context.Context, dbURL, natsURL string) {
	t.Helper()

	goBin, err := goBinary()
	if err != nil {
		t.Fatalf("locating go toolchain: %v", err)
	}

	binPath := filepath.Join(t.TempDir(), "scheduler-under-test.exe")
	// #nosec G204 -- goBin is resolved by this file's own goBinary(), never
	// from request/environment-controlled input; this is test setup, not a
	// request-handling path.
	build := exec.CommandContext(ctx, goBin, "build", "-o", binPath, "platform/services/scheduler")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building scheduler binary: %v\n%s", err, out)
	}

	// #nosec G204 -- binPath is the binary this same test just built into
	// t.TempDir(), not external input.
	cmd := exec.CommandContext(ctx, binPath)
	cmd.Env = append(os.Environ(),
		"APP_DATABASE_URL="+dbURL,
		"SCHEDULER_NATS_URL="+natsURL,
		"SCHEDULER_HEARTBEAT_TIMEOUT=5s",
		"SCHEDULER_LIVENESS_SWEEP_INTERVAL=1s",
	)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting scheduler process: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if t.Failed() {
			t.Logf("scheduler process output:\n%s", output.String())
		}
	})
}

// startTestWorker builds and runs a real worker binary as a separate OS
// process, pointed at natsURL with its own node-id file so multiple calls
// in the same test produce distinct nodes.
func startTestWorker(t *testing.T, ctx context.Context, natsURL string) string {
	t.Helper()

	goBin, err := goBinary()
	if err != nil {
		t.Fatalf("locating go toolchain: %v", err)
	}

	binPath := filepath.Join(t.TempDir(), "worker-under-test.exe")
	// #nosec G204 -- see startTestScheduler's identical justification above.
	build := exec.CommandContext(ctx, goBin, "build", "-o", binPath, "platform/services/worker")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building worker binary: %v\n%s", err, out)
	}

	nodeIDFile := filepath.Join(t.TempDir(), "worker-node-id")

	// #nosec G204 -- binPath is the binary this same test just built into
	// t.TempDir(), not external input.
	cmd := exec.CommandContext(ctx, binPath)
	cmd.Env = append(os.Environ(),
		"WORKER_LISTEN_ADDR=127.0.0.1:0",
		"WORKER_NATS_URL="+natsURL,
		"WORKER_NODE_ID_FILE="+nodeIDFile,
		"WORKER_HEARTBEAT_INTERVAL=1s",
		"WORKER_CPU_CAPACITY_MILLICORES=1500",
	)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting worker process: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if t.Failed() {
			t.Logf("worker process output:\n%s", output.String())
		}
	})
	return nodeIDFile
}

// goBinary locates the go toolchain. Mirrors services/worker's own copy of
// the same helper (this shell's PATH doesn't reliably include Go's bin
// directory).
func goBinary() (string, error) {
	if p, err := exec.LookPath("go"); err == nil {
		return p, nil
	}

	name := "go"
	if goruntime.GOOS == "windows" {
		name = "go.exe"
	}
	if goroot := os.Getenv("GOROOT"); goroot != "" {
		candidate := filepath.Join(goroot, "bin", name)
		if fileExists(candidate) {
			return candidate, nil
		}
	}
	for _, candidate := range []string{`C:\Program Files\Go\bin\go.exe`, "/usr/local/go/bin/go"} {
		if fileExists(candidate) {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("go toolchain not found on PATH, GOROOT, or a known install location")
}

func fileExists(path string) bool {
	// #nosec G703 -- path is always either $GOROOT/bin/go(.exe) or one of
	// this function's own hardcoded candidates, never external input.
	_, err := os.Stat(path)
	return err == nil
}

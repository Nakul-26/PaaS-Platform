//go:build integration

// Package main's integration test covers Task 4's acceptance
// (phase-3-controllers.md), the phase's literal exit-criteria scenario:
// against a real Postgres, a real NATS instance, a real scheduler, a real
// worker (with real Docker daemon access), and a real controller-manager,
// seed an application/deployment already at steady state (1 replica
// running, via the existing Phase 2 placement path), `docker kill` that
// container directly, confirm the worker's crash detection (Task 1)
// reports it, then confirm the controller notices actual(0) < desired(1)
// on its next tick and a replacement gets placed and reaches running.
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

func TestControllerManager_ReplacesCrashedContainer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	rt, err := runtime.NewDockerRuntime()
	if err != nil {
		t.Skipf("docker daemon not available, skipping: %v", err)
	}
	defer func() { _ = rt.Close() }()

	adminDSN, appDSN, adminDB, pool := startTestPostgres(t, ctx)
	defer func() { _ = adminDB.Close() }()

	deploymentID := seedRunningDeployment(t, ctx, adminDB)

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

	registrations := make(chan map[string]any, 4)
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

	startTestScheduler(t, ctx, appDSN, natsURL)
	startTestWorker(t, ctx, natsURL)
	startTestControllerManager(t, ctx, adminDSN, natsURL)

	var registration map[string]any
	select {
	case registration = <-registrations:
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for node registration message")
	}
	nodeID, _ := registration["node_id"].(string)
	if nodeID == "" {
		t.Fatalf("registration message missing node_id: %+v", registration)
	}

	// The scheduler ensures PLACEMENT/NODE_ASSIGNMENTS on startup
	// (subscribePlacement); this test's own EnsureStream is an idempotent
	// no-op layered on top, same as scheduler's own integration test.
	if err := bus.EnsureStream(ctx, eventbus.StreamConfig{
		Name:     eventbus.PlacementStream,
		Subjects: []string{eventbus.PlacementStreamFilter},
	}); err != nil {
		t.Fatalf("ensuring %s stream: %v", eventbus.PlacementStream, err)
	}

	// Seed the 1-replica steady state via the existing Phase 2 placement
	// path — the exact mechanism a real deploy uses, not a shortcut insert.
	applicationID := uuid.New()
	placementData, err := json.Marshal(map[string]any{
		"deployment_id":  deploymentID.String(),
		"application_id": applicationID.String(),
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

	original := waitForRunningContainer(t, ctx, containers, deploymentID, "", 30*time.Second)
	if original.ContainerRuntimeID == nil || *original.ContainerRuntimeID == "" {
		t.Fatalf("expected a container_runtime_id once running, got %+v", original)
	}
	originalRuntimeID := *original.ContainerRuntimeID
	t.Cleanup(func() {
		_ = rt.StopContainer(context.Background(), originalRuntimeID, 5*time.Second)
		_ = rt.RemoveContainer(context.Background(), originalRuntimeID)
	})

	// Kill the container directly against the real docker daemon — not the
	// worker process — so only the worker's health-check poll loop (Task
	// 1) and then the controller's reconcile tick (Task 4) can notice.
	if err := killContainer(ctx, originalRuntimeID); err != nil {
		t.Fatalf("killing container %s out-of-band: %v", originalRuntimeID, err)
	}

	deadline := time.After(30 * time.Second)
	for {
		c, err := containers.Get(ctx, original.ID)
		if err != nil {
			t.Fatalf("Get original container: %v", err)
		}
		if c.Status == db.ContainerStatusCrashed {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for worker to report the killed container as crashed, last status %q", c.Status)
		case <-time.After(200 * time.Millisecond):
		}
	}

	// The controller should now see actual(0) < desired(1) for this
	// deployment on its next tick and request a replacement.
	replacement := waitForRunningContainer(t, ctx, containers, deploymentID, original.ID.String(), 30*time.Second)
	if replacement.ID == original.ID {
		t.Fatalf("expected a new container distinct from the crashed one, got the same id %s", replacement.ID)
	}
	if replacement.ContainerRuntimeID == nil || *replacement.ContainerRuntimeID == "" {
		t.Fatalf("expected the replacement to have a container_runtime_id once running, got %+v", replacement)
	}

	replacementRuntimeID := *replacement.ContainerRuntimeID
	t.Cleanup(func() {
		_ = rt.StopContainer(context.Background(), replacementRuntimeID, 5*time.Second)
		_ = rt.RemoveContainer(context.Background(), replacementRuntimeID)
	})

	info, err := rt.ContainerStatus(ctx, replacementRuntimeID)
	if err != nil {
		t.Fatalf("inspecting replacement container %s on the real docker daemon: %v", replacementRuntimeID, err)
	}
	if info.Status != runtime.StatusRunning {
		t.Fatalf("expected replacement container %s to actually be running on the docker daemon, got status %q", replacementRuntimeID, info.Status)
	}
}

// waitForRunningContainer polls deploymentID's containers until it finds
// one that has reached 'running' whose id isn't excludeID (used to
// distinguish a replacement from the container it replaced).
func waitForRunningContainer(t *testing.T, ctx context.Context, containers db.ContainerRepository, deploymentID uuid.UUID, excludeID string, timeout time.Duration) db.Container {
	t.Helper()

	deadline := time.After(timeout)
	for {
		rows, err := containers.ListByDeployment(ctx, deploymentID)
		if err != nil {
			t.Fatalf("ListByDeployment: %v", err)
		}
		for _, c := range rows {
			if c.ID.String() == excludeID {
				continue
			}
			if c.Status == db.ContainerStatusRunning {
				return c
			}
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for a running container for deployment %s (excluding %s), last seen: %+v", deploymentID, excludeID, rows)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// killContainer kills a container directly via the docker CLI, bypassing
// the worker entirely — mirrors services/worker/main_integration_test.go's
// own copy (duplicated rather than imported, ADR-0012: no service depends
// on another's package tree).
func killContainer(ctx context.Context, containerID string) error {
	// #nosec G204 -- containerID comes from this test's own status read,
	// produced by the worker it just started, not external input.
	cmd := exec.CommandContext(ctx, "docker", "kill", containerID)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker kill: %w: %s", err, out)
	}
	return nil
}

// startTestPostgres starts a real Postgres container, applies migrations
// 0001-0007, and returns the admin (superuser) connection string
// (controller-manager connects as this — db.ReconcileRepository's doc
// comment explains why), the app-role connection string (scheduler/worker
// connect as this, unchanged from Phase 2), an admin *sql.DB (for seeding),
// and an app-role *db.Pool (for reading back results, mirroring
// services/scheduler's own copy of this helper).
func startTestPostgres(t *testing.T, ctx context.Context) (adminDSN, appDSN string, adminDB *sql.DB, pool *db.Pool) {
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
	adminDB, err = sql.Open("pgx", adminConnStr)
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

	pool, err = db.Open(ctx, appConnStr)
	if err != nil {
		t.Fatalf("opening app-role pool: %v", err)
	}
	t.Cleanup(pool.Close)

	return adminConnStr, appConnStr, adminDB, pool
}

// seedRunningDeployment inserts one organization/user/project/application/
// deployment row (as the admin, bypassing RLS) with replicas_desired left
// at its default of 1 — a steady-state deployment the controller should
// maintain. Mirrors internal/db's own node_container_integration_test.go
// seedDeployment (duplicated rather than imported, ADR-0012).
func seedRunningDeployment(t *testing.T, ctx context.Context, adminDB *sql.DB) uuid.UUID {
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
// liveness-sweep settings. Mirrors services/scheduler's own copy.
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
// process, pointed at natsURL, with a fast health-check poll interval so
// Task 1's crash detection doesn't dominate this test's timeout budget.
// Mirrors services/scheduler's own copy.
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
		"WORKER_HEALTH_CHECK_INTERVAL=1s",
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

// startTestControllerManager builds and runs the real controller-manager
// binary under test as a separate OS process (ADR-0012), pointed at the
// admin DSN (db.ReconcileRepository requires an RLS-bypass connection) with
// a fast reconcile interval so this test's timeout budget isn't dominated
// by it.
func startTestControllerManager(t *testing.T, ctx context.Context, adminDSN, natsURL string) {
	t.Helper()

	goBin, err := goBinary()
	if err != nil {
		t.Fatalf("locating go toolchain: %v", err)
	}

	binPath := filepath.Join(t.TempDir(), "controller-manager-under-test.exe")
	// #nosec G204 -- goBin is resolved by this file's own goBinary(), never
	// from request/environment-controlled input; this is test setup, not a
	// request-handling path.
	build := exec.CommandContext(ctx, goBin, "build", "-o", binPath, "platform/services/controller-manager")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building controller-manager binary: %v\n%s", err, out)
	}

	// #nosec G204 -- binPath is the binary this same test just built into
	// t.TempDir(), not external input.
	cmd := exec.CommandContext(ctx, binPath)
	cmd.Env = append(os.Environ(),
		"DATABASE_URL="+adminDSN,
		"CONTROLLER_NATS_URL="+natsURL,
		"CONTROLLER_RECONCILE_INTERVAL=1s",
	)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting controller-manager process: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if t.Failed() {
			t.Logf("controller-manager process output:\n%s", output.String())
		}
	})
}

// goBinary locates the go toolchain. Mirrors services/scheduler's and
// services/worker's own identical copies of this helper (this shell's PATH
// doesn't reliably include Go's bin directory).
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

//go:build integration

package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
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

	"platform/internal/auth"
	"platform/internal/db"
	"platform/internal/eventbus"
	"platform/internal/runtime"
	"platform/services/apiserver/internal/workerclient"
)

// TestAPIServer_CoreCRUDFlow is the Task 5 (phase-1-mvp.md) and Task 6
// (phase-2-multi-node.md) acceptance test: real routes, against a real
// (test-container) Postgres, a real (test-container) NATS instance, a real
// scheduler process, and a real worker agent, covering signup -> create
// project -> create application -> deploy -> (placement.requested event) ->
// scheduler places it -> get deployments shows the placement -> logs ->
// delete, plus the required cross-tenant denial checks
// (rbac-multitenancy.md §5). The apiserver itself runs in-process (this file
// is that service's own test), but the scheduler and worker run as actual
// separate OS processes (their own compiled binaries) — importing their
// package trees from here would violate ADR-0012 (network-only service
// boundaries), the exact thing this test is meant to exercise honestly.
func TestAPIServer_CoreCRUDFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	_, appDBURL, pool := startTestPostgres(t, ctx)
	natsURL := startTestNATS(t, ctx)
	// Scheduler must be up (and its core-NATS node.*.register/heartbeat
	// subscriptions live) before the worker starts — the worker's
	// registration is a one-shot core-NATS publish with no persistence, so
	// starting the worker first would lose it forever (mirrors
	// services/scheduler's own integration test's ordering).
	startTestScheduler(t, ctx, appDBURL, natsURL)
	// Two workers/nodes, not one: phase-3-controllers.md Task 6's acceptance
	// criterion is that deleting an application actually stops its container
	// on whichever node it landed on, not just the one address a hardcoded
	// WORKER_ADDR happens to point at — a single-worker test can't exercise
	// that distinction at all, since there'd be nowhere else for the bug to
	// misroute to.
	worker1Addr := startTestWorker(t, ctx, natsURL)
	startTestWorker(t, ctx, natsURL)

	bus, err := eventbus.Connect(natsURL)
	if err != nil {
		t.Fatalf("connecting eventbus: %v", err)
	}
	t.Cleanup(func() { _ = bus.Close() })
	// The apiserver's own main.go does this at startup (Task 6); the
	// scheduler subprocess does its own idempotent EnsureStream too — doing
	// it here as well closes the same "stream not found" startup race
	// Task 4/5's tests already hit and fixed.
	if err := bus.EnsureStream(ctx, eventbus.StreamConfig{
		Name:     eventbus.PlacementStream,
		Subjects: []string{eventbus.PlacementStreamFilter},
	}); err != nil {
		t.Fatalf("ensuring %s stream: %v", eventbus.PlacementStream, err)
	}

	issuer, err := auth.NewTokenIssuer("test-signing-key")
	if err != nil {
		t.Fatalf("NewTokenIssuer: %v", err)
	}
	// worker1Addr only backs the logs route's direct worker HTTP call (a
	// separate, already-known gap — see handlers_logs.go's own doc comment);
	// handleDeleteApplication no longer talks to this address at all (Task 6).
	worker := workerclient.New(worker1Addr)
	srv := New(pool, issuer, worker, bus, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(srv.Routes())
	// Registered via t.Cleanup (not a plain defer) so it runs before the
	// app-deletion safety-net cleanup below only if registered first —
	// t.Cleanup runs LIFO, and that cleanup is registered after this one,
	// so it fires while ts is still serving. A plain `defer ts.Close()`
	// would run during this function's own return/Goexit unwind, i.e.
	// before any t.Cleanup callback, closing ts out from under the
	// delete-on-failure cleanup and silently no-oping it.
	t.Cleanup(ts.Close)

	client := &http.Client{Timeout: 30 * time.Second}

	// --- org A: signup, create project, create application ---
	signupA := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "owner-a@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	tokenA := signupA["access_token"].(string)
	orgA := signupA["org"].(map[string]any)["id"].(string)

	project := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/orgs/"+orgA+"/projects", tokenA,
		map[string]string{"name": "Demo Project"}, http.StatusCreated)
	projectID := project["id"].(string)

	app := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/projects/"+projectID+"/applications", tokenA,
		map[string]any{
			"name":  "demo",
			"image": "nginx:latest",
			"ports": []map[string]any{{"container_port": 80, "protocol": "tcp"}},
		}, http.StatusCreated)
	appID := app["id"].(string)

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		req, _ := http.NewRequestWithContext(cleanupCtx, http.MethodDelete, ts.URL+"/v1/applications/"+appID, nil)
		req.Header.Set("Authorization", "Bearer "+tokenA)
		if resp, err := client.Do(req); err == nil {
			_ = resp.Body.Close()
		}
	})

	// --- org B: signup, attempt cross-tenant access to org A's resources ---
	signupB := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "owner-b@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	tokenB := signupB["access_token"].(string)

	assertStatus(t, ctx, client, http.MethodPost, ts.URL+"/v1/projects/"+projectID+"/applications", tokenB,
		map[string]any{"name": "sneaky", "image": "nginx:latest"}, http.StatusNotFound)
	assertStatus(t, ctx, client, http.MethodGet, ts.URL+"/v1/applications/"+appID+"/deployments", tokenB,
		nil, http.StatusNotFound)
	assertStatus(t, ctx, client, http.MethodDelete, ts.URL+"/v1/applications/"+appID, tokenB,
		nil, http.StatusNotFound)

	// --- org A: deploy — Task 6: this only records the deployment and
	// publishes placement.requested now, so it comes back "pending" with no
	// placement yet; the scheduler places it asynchronously.
	deployment := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/applications/"+appID+"/deployments", tokenA,
		nil, http.StatusCreated)
	if got := deployment["status"]; got != "pending" {
		t.Fatalf("deployment status = %v, want pending (placement is async as of Task 6, full response: %+v)", got, deployment)
	}
	if got, _ := deployment["revision"].(float64); got != 1 {
		t.Fatalf("deployment revision = %v, want 1", deployment["revision"])
	}
	if got, _ := deployment["replicas_running"].(float64); got != 0 {
		t.Fatalf("expected replicas_running = 0 immediately after deploy (placement hasn't happened yet), got %+v", deployment)
	}
	deploymentID := deployment["id"].(string)

	// Poll get-deployments until the scheduler has placed it and the worker
	// has reported it running — this is Task 6's actual acceptance:
	// create -> deploy -> (event) -> placement -> visible in reads.
	var placed map[string]any
	deadline := time.Now().Add(30 * time.Second)
	for {
		listResp := doJSON(t, ctx, client, http.MethodGet, ts.URL+"/v1/applications/"+appID+"/deployments", tokenA,
			nil, http.StatusOK)
		data, _ := listResp["data"].([]any)
		if len(data) != 1 {
			t.Fatalf("expected exactly 1 deployment listed, got %d (%+v)", len(data), listResp)
		}
		d := data[0].(map[string]any)
		if d["replicas_running"] == float64(1) {
			placed = d
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for deployment to be placed and running, last seen: %+v", d)
		}
		time.Sleep(300 * time.Millisecond)
	}
	placedNodeID := firstContainerNodeID(t, placed)

	// Confirm the resulting container is actually running on the real
	// Docker daemon, and clean it up — killing the worker subprocess later
	// (t.Cleanup) doesn't stop containers it started.
	depUUID, err := uuid.Parse(deploymentID)
	if err != nil {
		t.Fatalf("parsing deployment id: %v", err)
	}
	rows, err := db.NewContainerRepository(pool.Conn()).ListByDeployment(ctx, depUUID)
	if err != nil {
		t.Fatalf("ListByDeployment: %v", err)
	}
	if len(rows) != 1 || rows[0].ContainerRuntimeID == nil {
		t.Fatalf("expected exactly one container row with a runtime id, got %+v", rows)
	}
	containerRuntimeID := *rows[0].ContainerRuntimeID

	rt, err := runtime.NewDockerRuntime()
	if err != nil {
		t.Fatalf("connecting to docker daemon: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.StopContainer(context.Background(), containerRuntimeID, 5*time.Second)
		_ = rt.RemoveContainer(context.Background(), containerRuntimeID)
		_ = rt.Close()
	})
	if info, err := rt.ContainerStatus(ctx, containerRuntimeID); err != nil {
		t.Fatalf("inspecting container %s on the real docker daemon: %v", containerRuntimeID, err)
	} else if info.Status != runtime.StatusRunning {
		t.Fatalf("expected container %s to actually be running on the docker daemon, got status %q", containerRuntimeID, info.Status)
	}

	// --- org A: second application ("demo2") — the scheduler's least-loaded
	// scoring puts it on the *other* node, since "demo"'s node now has load 1.
	// This is what actually exercises phase-3-controllers.md Task 6: with the
	// old hardcoded-WORKER_ADDR delete path, stopping demo2 would silently
	// no-op against the wrong worker (worker1) instead of actually stopping
	// its container on worker2.
	app2 := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/projects/"+projectID+"/applications", tokenA,
		map[string]any{
			"name":  "demo2",
			"image": "nginx:latest",
			"ports": []map[string]any{{"container_port": 80, "protocol": "tcp"}},
		}, http.StatusCreated)
	app2ID := app2["id"].(string)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		req, _ := http.NewRequestWithContext(cleanupCtx, http.MethodDelete, ts.URL+"/v1/applications/"+app2ID, nil)
		req.Header.Set("Authorization", "Bearer "+tokenA)
		if resp, err := client.Do(req); err == nil {
			_ = resp.Body.Close()
		}
	})

	deployment2 := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/applications/"+app2ID+"/deployments", tokenA,
		nil, http.StatusCreated)
	deployment2ID := deployment2["id"].(string)

	var placed2 map[string]any
	deadline = time.Now().Add(30 * time.Second)
	for {
		listResp := doJSON(t, ctx, client, http.MethodGet, ts.URL+"/v1/applications/"+app2ID+"/deployments", tokenA,
			nil, http.StatusOK)
		data, _ := listResp["data"].([]any)
		if len(data) != 1 {
			t.Fatalf("expected exactly 1 deployment listed for demo2, got %d (%+v)", len(data), listResp)
		}
		d := data[0].(map[string]any)
		if d["replicas_running"] == float64(1) {
			placed2 = d
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for demo2's deployment to be placed and running, last seen: %+v", d)
		}
		time.Sleep(300 * time.Millisecond)
	}
	placed2NodeID := firstContainerNodeID(t, placed2)
	if placed2NodeID == placedNodeID {
		t.Fatalf("expected demo2 to land on a different node than demo (least-loaded scoring), demo node_id=%v demo2 node_id=%v", placedNodeID, placed2NodeID)
	}

	dep2UUID, err := uuid.Parse(deployment2ID)
	if err != nil {
		t.Fatalf("parsing demo2 deployment id: %v", err)
	}
	rows2, err := db.NewContainerRepository(pool.Conn()).ListByDeployment(ctx, dep2UUID)
	if err != nil {
		t.Fatalf("ListByDeployment for demo2: %v", err)
	}
	if len(rows2) != 1 || rows2[0].ContainerRuntimeID == nil {
		t.Fatalf("expected exactly one container row with a runtime id for demo2, got %+v", rows2)
	}
	container2RuntimeID := *rows2[0].ContainerRuntimeID
	t.Cleanup(func() {
		_ = rt.StopContainer(context.Background(), container2RuntimeID, 5*time.Second)
		_ = rt.RemoveContainer(context.Background(), container2RuntimeID)
	})
	if info, err := rt.ContainerStatus(ctx, container2RuntimeID); err != nil {
		t.Fatalf("inspecting container %s on the real docker daemon: %v", container2RuntimeID, err)
	} else if info.Status != runtime.StatusRunning {
		t.Fatalf("expected container %s to actually be running on the docker daemon, got status %q", container2RuntimeID, info.Status)
	}

	// --- org A: logs (Task 7) — proxied through to the real worker/container.
	// nginx's entrypoint flushes its startup log lines to the container's log
	// driver very shortly after start, but not necessarily within the instant
	// deploy's own ContainerStatus call observed it as "running" — so poll
	// briefly rather than asserting non-empty on the first read.
	var logsBody []byte
	deadline = time.Now().Add(10 * time.Second)
	for {
		logsReq, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/v1/applications/"+appID+"/logs", nil)
		logsReq.Header.Set("Authorization", "Bearer "+tokenA)
		logsResp, err := client.Do(logsReq)
		if err != nil {
			t.Fatalf("GET .../logs: %v", err)
		}
		body, err := io.ReadAll(logsResp.Body)
		_ = logsResp.Body.Close()
		if err != nil {
			t.Fatalf("reading logs response body: %v", err)
		}
		if logsResp.StatusCode != http.StatusOK {
			t.Fatalf("GET .../logs: status = %d, want 200 (body=%s)", logsResp.StatusCode, body)
		}
		if ct := logsResp.Header.Get("Content-Type"); ct != "text/plain; charset=utf-8" {
			t.Fatalf("GET .../logs: Content-Type = %q, want text/plain", ct)
		}
		logsBody = body
		if len(logsBody) > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if len(logsBody) == 0 {
		t.Fatalf("GET .../logs: expected nginx's startup log lines within 10s, got an empty body")
	}

	// org B must not be able to read org A's logs, same as every other
	// application-scoped route (rbac-multitenancy.md §5).
	assertStatus(t, ctx, client, http.MethodGet, ts.URL+"/v1/applications/"+appID+"/logs", tokenB,
		nil, http.StatusNotFound)

	// --- org A: delete "demo" (node1) and "demo2" (node2), confirm both
	// applications and their containers are actually gone — Task 6's literal
	// acceptance: the delete route must stop each container on whichever
	// node it really landed on, not just whichever address WORKER_ADDR
	// happens to be. waitForContainerGone polls the real Docker daemon
	// directly, independent of which worker process the delete's
	// node.<id>.unassign publish actually reached.
	assertStatus(t, ctx, client, http.MethodDelete, ts.URL+"/v1/applications/"+appID, tokenA, nil, http.StatusNoContent)
	assertStatus(t, ctx, client, http.MethodGet, ts.URL+"/v1/applications/"+appID+"/deployments", tokenA, nil, http.StatusNotFound)
	waitForContainerGone(t, ctx, rt, containerRuntimeID, 30*time.Second)

	assertStatus(t, ctx, client, http.MethodDelete, ts.URL+"/v1/applications/"+app2ID, tokenA, nil, http.StatusNoContent)
	assertStatus(t, ctx, client, http.MethodGet, ts.URL+"/v1/applications/"+app2ID+"/deployments", tokenA, nil, http.StatusNotFound)
	waitForContainerGone(t, ctx, rt, container2RuntimeID, 30*time.Second)
}

// TestAPIServer_ScaleApplication is phase-3-controllers.md Task 5's
// acceptance test: PATCH .../applications/:appId only writes desired state
// (applications.replicas_desired); it's the real controller-manager process
// (Task 4) that reconciles actual running-container count toward it on its
// own tick, with no special-cased "scale" code path distinguishing it from
// crash recovery. Scales 1 -> 3 (poll until 3 running), then 3 -> 1 (poll
// until the excess 2 are stopped) — the phase doc's literal Task 5 script.
func TestAPIServer_ScaleApplication(t *testing.T) {
	// 8 minutes, longer than TestAPIServer_CoreCRUDFlow's 4: this test starts
	// a 4th real process (controller-manager) on top of
	// postgres+nats+scheduler+worker, and does two full scale-and-converge
	// round trips instead of one placement.
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	adminDBURL, appDBURL, pool := startTestPostgres(t, ctx)
	natsURL := startTestNATS(t, ctx)
	startTestScheduler(t, ctx, appDBURL, natsURL)
	workerAddr := startTestWorker(t, ctx, natsURL)
	startTestControllerManager(t, ctx, adminDBURL, natsURL)

	bus, err := eventbus.Connect(natsURL)
	if err != nil {
		t.Fatalf("connecting eventbus: %v", err)
	}
	t.Cleanup(func() { _ = bus.Close() })
	if err := bus.EnsureStream(ctx, eventbus.StreamConfig{
		Name:     eventbus.PlacementStream,
		Subjects: []string{eventbus.PlacementStreamFilter},
	}); err != nil {
		t.Fatalf("ensuring %s stream: %v", eventbus.PlacementStream, err)
	}

	issuer, err := auth.NewTokenIssuer("test-signing-key")
	if err != nil {
		t.Fatalf("NewTokenIssuer: %v", err)
	}
	worker := workerclient.New(workerAddr)
	srv := New(pool, issuer, worker, bus, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(srv.Routes())
	t.Cleanup(ts.Close)

	client := &http.Client{Timeout: 30 * time.Second}

	signup := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "scale-owner@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	token := signup["access_token"].(string)
	orgID := signup["org"].(map[string]any)["id"].(string)

	project := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/orgs/"+orgID+"/projects", token,
		map[string]string{"name": "Scale Project"}, http.StatusCreated)
	projectID := project["id"].(string)

	app := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/projects/"+projectID+"/applications", token,
		map[string]any{
			"name":  "scale-demo",
			"image": "nginx:latest",
			"ports": []map[string]any{{"container_port": 80, "protocol": "tcp"}},
		}, http.StatusCreated)
	appID := app["id"].(string)
	if got, _ := app["replicas_desired"].(float64); got != 1 {
		t.Fatalf("newly created application replicas_desired = %v, want 1", app["replicas_desired"])
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		req, _ := http.NewRequestWithContext(cleanupCtx, http.MethodDelete, ts.URL+"/v1/applications/"+appID, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		if resp, err := client.Do(req); err == nil {
			_ = resp.Body.Close()
		}
	})

	deployment := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/applications/"+appID+"/deployments", token,
		nil, http.StatusCreated)
	deploymentID, err := uuid.Parse(deployment["id"].(string))
	if err != nil {
		t.Fatalf("parsing deployment id: %v", err)
	}
	containers := db.NewContainerRepository(pool.Conn())

	waitForReplicaCount(t, ctx, client, ts, token, appID, deploymentID, 1, 30*time.Second)

	// --- scale 1 -> 3 ---
	scaled := doJSON(t, ctx, client, http.MethodPatch, ts.URL+"/v1/applications/"+appID, token,
		map[string]any{"replicas_desired": 3}, http.StatusOK)
	if got, _ := scaled["replicas_desired"].(float64); got != 3 {
		t.Fatalf("PATCH replicas_desired response = %v, want 3", scaled["replicas_desired"])
	}
	waitForReplicaCount(t, ctx, client, ts, token, appID, deploymentID, 3, 30*time.Second)

	// Confirm all 3 are genuinely running on the real Docker daemon, and
	// track them for cleanup — the controller-manager subprocess started
	// them, but killing that subprocess later doesn't stop the containers it
	// asked the worker to start.
	rt, err := runtime.NewDockerRuntime()
	if err != nil {
		t.Fatalf("connecting to docker daemon: %v", err)
	}
	t.Cleanup(func() { _ = rt.Close() })
	rows, err := containers.ListByDeployment(ctx, deploymentID)
	if err != nil {
		t.Fatalf("ListByDeployment: %v", err)
	}
	for _, cn := range rows {
		if cn.ContainerRuntimeID == nil {
			continue
		}
		runtimeID := *cn.ContainerRuntimeID
		t.Cleanup(func() {
			_ = rt.StopContainer(context.Background(), runtimeID, 5*time.Second)
			_ = rt.RemoveContainer(context.Background(), runtimeID)
		})
		if info, err := rt.ContainerStatus(ctx, runtimeID); err != nil {
			t.Fatalf("inspecting container %s on the real docker daemon: %v", runtimeID, err)
		} else if info.Status != runtime.StatusRunning {
			t.Fatalf("expected container %s to actually be running on the docker daemon, got status %q", runtimeID, info.Status)
		}
	}

	// --- scale 3 -> 1 ---
	scaledDown := doJSON(t, ctx, client, http.MethodPatch, ts.URL+"/v1/applications/"+appID, token,
		map[string]any{"replicas_desired": 1}, http.StatusOK)
	if got, _ := scaledDown["replicas_desired"].(float64); got != 1 {
		t.Fatalf("PATCH replicas_desired response = %v, want 1", scaledDown["replicas_desired"])
	}
	waitForReplicaCount(t, ctx, client, ts, token, appID, deploymentID, 1, 30*time.Second)

	// The 2 excess containers must actually have been stopped, not merely
	// forgotten about — confirm their rows read back as 'stopped'.
	rows, err = containers.ListByDeployment(ctx, deploymentID)
	if err != nil {
		t.Fatalf("ListByDeployment after scale-down: %v", err)
	}
	var stoppedCount, activeCount int
	for _, cn := range rows {
		switch cn.Status {
		case db.ContainerStatusStopped:
			stoppedCount++
		case db.ContainerStatusRunning, db.ContainerStatusPending:
			activeCount++
		}
	}
	if stoppedCount != 2 || activeCount != 1 {
		t.Fatalf("after scale-down: stoppedCount=%d activeCount=%d (want 2 stopped, 1 active), rows=%+v", stoppedCount, activeCount, rows)
	}
}

// waitForReplicaCount polls GET .../deployments — the same replica-level
// state Task 7 (phase-3-controllers.md) added to the API response, and
// what `platform get deployments` itself now shows — until the named
// deployment's replicas_running reaches want. This is the actual
// observable surface a caller polls after a scale, not an internal
// repository read.
func waitForReplicaCount(t *testing.T, ctx context.Context, client *http.Client, ts *httptest.Server, token, appID string, deploymentID uuid.UUID, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		listResp := doJSON(t, ctx, client, http.MethodGet, ts.URL+"/v1/applications/"+appID+"/deployments", token, nil, http.StatusOK)
		data, _ := listResp["data"].([]any)
		var d map[string]any
		for _, raw := range data {
			row, _ := raw.(map[string]any)
			if row["id"] == deploymentID.String() {
				d = row
				break
			}
		}
		if d == nil {
			t.Fatalf("deployment %s not found in listing: %+v", deploymentID, listResp)
		}
		if running, _ := d["replicas_running"].(float64); int(running) == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for replicas_running = %d on deployment %s, last seen: %+v", want, deploymentID, d)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// waitForContainerGone polls the real Docker daemon until runtimeID is no
// longer inspectable — worker's handleUnassign (docs/nats-contract.md)
// stops *and removes* the container, so "gone" (an inspect error), not
// merely "exited", is the converged end state.
func waitForContainerGone(t *testing.T, ctx context.Context, rt runtime.ContainerRuntime, runtimeID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		info, err := rt.ContainerStatus(ctx, runtimeID)
		if err != nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for container %s to be stopped/removed, last seen status %q", runtimeID, info.Status)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// firstContainerNodeID extracts containers[0].node_id from a decoded
// deployment response map — Task 7's replacement for the old single
// top-level node_id field (a deployment can now list more than one
// container, but this test only ever places one replica per app).
func firstContainerNodeID(t *testing.T, d map[string]any) string {
	t.Helper()
	containers, _ := d["containers"].([]any)
	if len(containers) == 0 {
		t.Fatalf("expected at least one container in deployment response, got %+v", d)
	}
	cn, _ := containers[0].(map[string]any)
	nodeID, _ := cn["node_id"].(string)
	if nodeID == "" {
		t.Fatalf("expected a non-empty node_id in first container, got %+v", cn)
	}
	return nodeID
}

func doJSON(t *testing.T, ctx context.Context, client *http.Client, method, url, token string, body any, wantStatus int) map[string]any {
	t.Helper()
	resp, respBody := do(t, ctx, client, method, url, token, body)
	if resp.StatusCode != wantStatus {
		t.Fatalf("%s %s: status = %d, want %d (body=%s)", method, url, resp.StatusCode, wantStatus, respBody)
	}
	var decoded map[string]any
	if len(respBody) > 0 {
		if err := json.Unmarshal(respBody, &decoded); err != nil {
			t.Fatalf("%s %s: decoding response %s: %v", method, url, respBody, err)
		}
	}
	return decoded
}

func assertStatus(t *testing.T, ctx context.Context, client *http.Client, method, url, token string, body any, wantStatus int) {
	t.Helper()
	resp, respBody := do(t, ctx, client, method, url, token, body)
	if resp.StatusCode != wantStatus {
		t.Fatalf("%s %s: status = %d, want %d (body=%s)", method, url, resp.StatusCode, wantStatus, respBody)
	}
}

func do(t *testing.T, ctx context.Context, client *http.Client, method, url, token string, body any) (*http.Response, []byte) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshaling request body: %v", err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading response body: %v", err)
	}
	return resp, respBody
}

// startTestPostgres mirrors internal/db's TestRLS_CrossTenantIsolation
// setup: a real Postgres testcontainer with all migrations applied,
// returning the superuser connection string (for controller-manager's own
// DATABASE_URL — Task 4/5's documented RLS-bypass exception), the app-role
// connection string (for the scheduler subprocess's own APP_DATABASE_URL),
// and a *db.Pool connected as the non-superuser platform_app role so RLS is
// actually enforced (ADR-0010).
func startTestPostgres(t *testing.T, ctx context.Context) (adminDSN, appDSN string, pool *db.Pool) {
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
	defer func() { _ = adminDB.Close() }()

	// The postgres image restarts once internally after initdb; the
	// container's "ready" log line (and even a first successful TCP
	// connect) can be observed just before that restart, so tolerate
	// several failed pings before giving up (mirrors internal/db's
	// rls_integration_test.go waitForPing).
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

	migrationsDir := filepath.Join("..", "..", "..", "..", "infrastructure", "postgres", "migrations")
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
	return adminConnStr, appConnStr, pool
}

// startTestNATS starts a real NATS/JetStream testcontainer, mirroring
// services/scheduler's own integration test, and returns its connection
// URL.
func startTestNATS(t *testing.T, ctx context.Context) string {
	t.Helper()

	natsContainer, err := nats.Run(ctx, "nats:2.11.7")
	if err != nil {
		t.Fatalf("starting nats container: %v", err)
	}
	t.Cleanup(func() { _ = natsContainer.Terminate(context.Background()) })

	natsURL, err := natsContainer.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("nats connection string: %v", err)
	}
	return natsURL
}

// startTestScheduler builds and runs the real scheduler binary as a
// separate OS process (ADR-0012), pointed at dbURL/natsURL with fast
// liveness-sweep settings — mirrors services/scheduler's own integration
// test's startTestScheduler.
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

// startTestWorker builds and runs the real worker binary as a separate OS
// process (ADR-0012 — see the test's doc comment for why), returning its
// base URL once it's accepting connections. Requires a real Docker daemon,
// same as services/worker's own integration test. natsURL wires up the same
// node-agent registration/heartbeat/assignment loop the scheduler depends on
// (phase-2-multi-node.md Tasks 3-4) — without it, the scheduler would have
// no healthy node to place the deployment on.
func startTestWorker(t *testing.T, ctx context.Context, natsURL string) string {
	t.Helper()

	goBin, err := goBinary()
	if err != nil {
		t.Fatalf("locating go toolchain: %v", err)
	}

	binPath := filepath.Join(t.TempDir(), "worker-under-test.exe")
	// #nosec G204 -- goBin is resolved by this file's own goBinary(), never
	// from request/environment-controlled input; this is test setup, not a
	// request-handling path.
	build := exec.CommandContext(ctx, goBin, "build", "-o", binPath, "platform/services/worker")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building worker binary: %v\n%s", err, out)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocating a free port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	// #nosec G204 -- binPath is the binary this same test just built into
	// t.TempDir(), not external input.
	cmd := exec.CommandContext(ctx, binPath)
	cmd.Env = append(os.Environ(),
		"WORKER_LISTEN_ADDR="+addr,
		"WORKER_NATS_URL="+natsURL,
		"WORKER_NODE_ID_FILE="+filepath.Join(t.TempDir(), "worker-node-id"),
		"WORKER_HEARTBEAT_INTERVAL=1s",
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

	baseURL := "http://" + addr
	deadline := time.Now().Add(20 * time.Second)
	client := &http.Client{Timeout: 2 * time.Second}
	for time.Now().Before(deadline) {
		if resp, err := client.Get(baseURL + "/v1/containers/readiness-probe"); err == nil {
			_ = resp.Body.Close()
			return baseURL
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("worker process at %s did not become ready in time", baseURL)
	return ""
}

// startTestControllerManager builds and runs the real controller-manager
// binary as a separate OS process (ADR-0012), pointed at adminDSN (its
// documented RLS-bypass connection, db.ReconcileRepository's doc comment) and
// natsURL, with a fast reconcile interval so scale tests don't have to wait
// out the 5s production default.
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

// goBinary locates the go toolchain. Tries PATH first (the documented way,
// per go/runtime's GOROOT() deprecation notice), then GOROOT if set, then
// this environment's known install location — this shell's PATH doesn't
// reliably include Go's bin directory, so PATH alone isn't enough here.
func goBinary() (string, error) {
	if p, err := exec.LookPath("go"); err == nil {
		return p, nil
	}

	name := "go"
	if goruntime.GOOS == "windows" {
		name = "go.exe"
	}
	if goroot := os.Getenv("GOROOT"); goroot != "" {
		if candidate := filepath.Join(goroot, "bin", name); fileExists(candidate) {
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

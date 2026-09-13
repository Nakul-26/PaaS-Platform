//go:build integration

package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
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

	adminDBURL, appDBURL, pool := startTestPostgres(t, ctx)
	natsURL := startTestNATS(t, ctx)
	// Scheduler must be up (and its core-NATS node.*.register/heartbeat
	// subscriptions live) before the worker starts — the worker's
	// registration is a one-shot core-NATS publish with no persistence, so
	// starting the worker first would lose it forever (mirrors
	// services/scheduler's own integration test's ordering).
	startTestScheduler(t, ctx, appDBURL, adminDBURL, natsURL)
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
	startTestScheduler(t, ctx, appDBURL, adminDBURL, natsURL)
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

	// Task 3's Service Instance controller (phase-4-service-discovery-lb.md)
	// runs inside this same controller-manager process and should have
	// turned these 3 running containers into a service by now — confirm
	// Task 6's read path surfaces it: the exact value `platform get
	// deployments` shows and the load balancer's X-Platform-Service header
	// expects.
	dnsName := waitForServiceDNSName(t, ctx, client, ts, token, appID, deploymentID, 30*time.Second)
	if !strings.HasSuffix(dnsName, ".internal") {
		t.Fatalf("expected service dns_name to end in .internal, got %q", dnsName)
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

// TestAPIServer_Domains is Task 2's (phase-5-networking-ingress.md)
// acceptance test: real CRUD routes for domains, wiring the already-
// specified domains.manage permission (rbac-multitenancy.md §2) to actual
// enforcement for the first time. Domains never touch a worker or the
// event bus, so unlike the tests above this only needs Postgres — no
// NATS/scheduler/worker subprocess.
func TestAPIServer_Domains(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	_, _, pool := startTestPostgres(t, ctx)

	issuer, err := auth.NewTokenIssuer("test-signing-key")
	if err != nil {
		t.Fatalf("NewTokenIssuer: %v", err)
	}
	srv := New(pool, issuer, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(srv.Routes())
	t.Cleanup(ts.Close)

	client := &http.Client{Timeout: 30 * time.Second}

	// --- org A: owner, two projects, one application in the first ---
	signupA := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "domains-owner@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	tokenA := signupA["access_token"].(string)
	orgA := signupA["org"].(map[string]any)["id"].(string)
	orgAID, err := uuid.Parse(orgA)
	if err != nil {
		t.Fatalf("parsing org A id: %v", err)
	}
	ownerAID, err := uuid.Parse(signupA["user"].(map[string]any)["id"].(string))
	if err != nil {
		t.Fatalf("parsing owner A id: %v", err)
	}

	project := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/orgs/"+orgA+"/projects", tokenA,
		map[string]string{"name": "Domains Project"}, http.StatusCreated)
	projectID := project["id"].(string)

	app := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/projects/"+projectID+"/applications", tokenA,
		map[string]any{"name": "web", "image": "nginx:latest"}, http.StatusCreated)
	appID := app["id"].(string)

	otherProject := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/orgs/"+orgA+"/projects", tokenA,
		map[string]string{"name": "Other Project"}, http.StatusCreated)
	otherProjectID := otherProject["id"].(string)

	// --- developer and viewer memberships in org A, seeded directly (no
	// invite route exists yet) for the domains.manage permission-matrix
	// check (rbac-multitenancy.md §5: "developer allowed, viewer denied").
	// is_org_admin(orgAID) admits this insert because it runs as ownerA's
	// own session (0001_organizations_users_memberships.sql).
	signupDev := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "domains-dev@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	tokenDev := signupDev["access_token"].(string)
	devID, err := uuid.Parse(signupDev["user"].(map[string]any)["id"].(string))
	if err != nil {
		t.Fatalf("parsing developer id: %v", err)
	}

	signupViewer := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "domains-viewer@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	tokenViewer := signupViewer["access_token"].(string)
	viewerID, err := uuid.Parse(signupViewer["user"].(map[string]any)["id"].(string))
	if err != nil {
		t.Fatalf("parsing viewer id: %v", err)
	}

	if err := pool.WithTx(ctx, ownerAID, orgAID, func(ctx context.Context, conn db.Conn) error {
		memberships := db.NewMembershipRepository(conn)
		if _, err := memberships.Create(ctx, orgAID, devID, db.MembershipRoleDeveloper); err != nil {
			return err
		}
		_, err := memberships.Create(ctx, orgAID, viewerID, db.MembershipRoleViewer)
		return err
	}); err != nil {
		t.Fatalf("seeding org A developer/viewer memberships: %v", err)
	}

	// --- org B: unrelated org, for cross-tenant denial checks ---
	signupB := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "domains-b@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	tokenB := signupB["access_token"].(string)

	// --- permission matrix: viewer denied, developer allowed ---
	assertStatus(t, ctx, client, http.MethodPost, ts.URL+"/v1/projects/"+projectID+"/domains", tokenViewer,
		map[string]any{"hostname": "viewer-should-not-create.example.com", "application_id": appID}, http.StatusForbidden)

	devDomain := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/projects/"+projectID+"/domains", tokenDev,
		map[string]any{"hostname": "dev.example.com", "application_id": appID}, http.StatusCreated)
	if got := devDomain["hostname"]; got != "dev.example.com" {
		t.Fatalf("developer-created domain hostname = %v, want dev.example.com", got)
	}

	// --- owner: the domain under test for the rest of this function ---
	domain := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/projects/"+projectID+"/domains", tokenA,
		map[string]any{"hostname": "app.example.com", "application_id": appID}, http.StatusCreated)
	domainID := domain["id"].(string)
	if domain["application_id"] != appID {
		t.Fatalf("domain application_id = %v, want %v", domain["application_id"], appID)
	}
	if domain["project_id"] != projectID {
		t.Fatalf("domain project_id = %v, want %v", domain["project_id"], projectID)
	}
	// tls_status is 'active' immediately, not 'pending' (open decision 3,
	// phase-5-networking-ingress.md) — handleCreateDomain flips it
	// synchronously right after Create.
	if domain["tls_status"] != "active" {
		t.Fatalf("newly created domain tls_status = %v, want active", domain["tls_status"])
	}

	// --- duplicate hostname -> 409, matching handleCreateApplication's own
	// unique-name conflict precedent ---
	assertStatus(t, ctx, client, http.MethodPost, ts.URL+"/v1/projects/"+projectID+"/domains", tokenA,
		map[string]any{"hostname": "app.example.com", "application_id": appID}, http.StatusConflict)

	// --- application_id valid but not in *this* project -> 404, not a
	// silently-accepted cross-project domain ---
	assertStatus(t, ctx, client, http.MethodPost, ts.URL+"/v1/projects/"+otherProjectID+"/domains", tokenA,
		map[string]any{"hostname": "cross-project.example.com", "application_id": appID}, http.StatusNotFound)

	// --- list: owner and viewer (read-only, no domains.manage needed) both
	// see both domains created above ---
	listA := doJSON(t, ctx, client, http.MethodGet, ts.URL+"/v1/projects/"+projectID+"/domains", tokenA, nil, http.StatusOK)
	if data, _ := listA["data"].([]any); len(data) != 2 {
		t.Fatalf("owner's domain list length = %d, want 2 (%+v)", len(data), listA)
	}
	listViewer := doJSON(t, ctx, client, http.MethodGet, ts.URL+"/v1/projects/"+projectID+"/domains", tokenViewer, nil, http.StatusOK)
	if data, _ := listViewer["data"].([]any); len(data) != 2 {
		t.Fatalf("viewer's domain list length = %d, want 2 (%+v)", len(data), listViewer)
	}

	// --- cross-tenant denial (rbac-multitenancy.md §5): org B, a complete
	// stranger to org A, gets 404 on every domains route touching org A's
	// resources, never 403 (existence must not be confirmable) ---
	assertStatus(t, ctx, client, http.MethodPost, ts.URL+"/v1/projects/"+projectID+"/domains", tokenB,
		map[string]any{"hostname": "sneaky.example.com", "application_id": appID}, http.StatusNotFound)
	assertStatus(t, ctx, client, http.MethodGet, ts.URL+"/v1/projects/"+projectID+"/domains", tokenB, nil, http.StatusNotFound)
	assertStatus(t, ctx, client, http.MethodDelete, ts.URL+"/v1/domains/"+domainID, tokenB, nil, http.StatusNotFound)

	// --- delete: viewer denied, owner allowed, already-deleted -> 404 ---
	assertStatus(t, ctx, client, http.MethodDelete, ts.URL+"/v1/domains/"+domainID, tokenViewer, nil, http.StatusForbidden)
	assertStatus(t, ctx, client, http.MethodDelete, ts.URL+"/v1/domains/"+domainID, tokenA, nil, http.StatusNoContent)
	assertStatus(t, ctx, client, http.MethodDelete, ts.URL+"/v1/domains/"+domainID, tokenA, nil, http.StatusNotFound)
}

// TestAPIServer_Organizations is Task 3's acceptance
// (phase-6-multi-tenant-saas.md): GET/PATCH/DELETE /v1/orgs/:orgId against
// real Postgres — the permission-matrix check for both organization.manage
// (owner/admin allowed, developer/viewer denied) and organization.delete
// (owner only, per rbac-multitenancy.md §2), cross-tenant denial (404, not
// 403) on all three routes, and confirming DELETE's cascade actually runs
// (a post-delete GET even from the former owner's own still-valid token
// gets 404, since its membership row is gone along with everything else).
func TestAPIServer_Organizations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	_, _, pool := startTestPostgres(t, ctx)

	issuer, err := auth.NewTokenIssuer("test-signing-key")
	if err != nil {
		t.Fatalf("NewTokenIssuer: %v", err)
	}
	srv := New(pool, issuer, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(srv.Routes())
	t.Cleanup(ts.Close)

	client := &http.Client{Timeout: 30 * time.Second}

	// --- org A: owner (from signup) plus seeded admin/developer/viewer
	// memberships, covering every role the matrix distinguishes ---
	signupA := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "org-owner@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	tokenOwner := signupA["access_token"].(string)
	orgA := signupA["org"].(map[string]any)["id"].(string)
	orgAID, err := uuid.Parse(orgA)
	if err != nil {
		t.Fatalf("parsing org A id: %v", err)
	}
	ownerID, err := uuid.Parse(signupA["user"].(map[string]any)["id"].(string))
	if err != nil {
		t.Fatalf("parsing owner id: %v", err)
	}

	signupAdmin := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "org-admin@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	tokenAdmin := signupAdmin["access_token"].(string)
	adminID, err := uuid.Parse(signupAdmin["user"].(map[string]any)["id"].(string))
	if err != nil {
		t.Fatalf("parsing admin id: %v", err)
	}

	signupDev := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "org-dev@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	tokenDev := signupDev["access_token"].(string)
	devID, err := uuid.Parse(signupDev["user"].(map[string]any)["id"].(string))
	if err != nil {
		t.Fatalf("parsing developer id: %v", err)
	}

	signupViewer := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "org-viewer@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	tokenViewer := signupViewer["access_token"].(string)
	viewerID, err := uuid.Parse(signupViewer["user"].(map[string]any)["id"].(string))
	if err != nil {
		t.Fatalf("parsing viewer id: %v", err)
	}

	if err := pool.WithTx(ctx, ownerID, orgAID, func(ctx context.Context, conn db.Conn) error {
		memberships := db.NewMembershipRepository(conn)
		if _, err := memberships.Create(ctx, orgAID, adminID, db.MembershipRoleAdmin); err != nil {
			return err
		}
		if _, err := memberships.Create(ctx, orgAID, devID, db.MembershipRoleDeveloper); err != nil {
			return err
		}
		_, err := memberships.Create(ctx, orgAID, viewerID, db.MembershipRoleViewer)
		return err
	}); err != nil {
		t.Fatalf("seeding org A admin/developer/viewer memberships: %v", err)
	}

	// --- org B: unrelated org, for cross-tenant denial checks ---
	signupB := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "org-b@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	tokenB := signupB["access_token"].(string)

	// --- GET: membership-only, every role in org A can view (no separate
	// organization.view row in the matrix) ---
	got := doJSON(t, ctx, client, http.MethodGet, ts.URL+"/v1/orgs/"+orgA, tokenOwner, nil, http.StatusOK)
	if got["id"] != orgA || got["slug"] == "" {
		t.Fatalf("GET org (owner) = %+v, want id=%s and a non-empty slug", got, orgA)
	}
	gotViewer := doJSON(t, ctx, client, http.MethodGet, ts.URL+"/v1/orgs/"+orgA, tokenViewer, nil, http.StatusOK)
	if gotViewer["id"] != orgA {
		t.Fatalf("GET org (viewer) = %+v, want id=%s", gotViewer, orgA)
	}

	// --- PATCH permission matrix (organization.manage: owner/admin allowed,
	// developer/viewer denied) ---
	assertStatus(t, ctx, client, http.MethodPatch, ts.URL+"/v1/orgs/"+orgA, tokenViewer,
		map[string]string{"name": "Viewer Should Not Rename"}, http.StatusForbidden)
	assertStatus(t, ctx, client, http.MethodPatch, ts.URL+"/v1/orgs/"+orgA, tokenDev,
		map[string]string{"name": "Developer Should Not Rename"}, http.StatusForbidden)
	patchedByAdmin := doJSON(t, ctx, client, http.MethodPatch, ts.URL+"/v1/orgs/"+orgA, tokenAdmin,
		map[string]string{"name": "Renamed By Admin"}, http.StatusOK)
	if patchedByAdmin["name"] != "Renamed By Admin" {
		t.Fatalf("PATCH org (admin) name = %v, want %q", patchedByAdmin["name"], "Renamed By Admin")
	}
	patchedByOwner := doJSON(t, ctx, client, http.MethodPatch, ts.URL+"/v1/orgs/"+orgA, tokenOwner,
		map[string]string{"name": "Renamed By Owner"}, http.StatusOK)
	if patchedByOwner["name"] != "Renamed By Owner" || patchedByOwner["slug"] != got["slug"] {
		t.Fatalf("PATCH org (owner) = %+v, want name=%q and slug unchanged (%v)", patchedByOwner, "Renamed By Owner", got["slug"])
	}
	// empty name -> 422, same missing_field precedent as project/application create
	assertStatus(t, ctx, client, http.MethodPatch, ts.URL+"/v1/orgs/"+orgA, tokenOwner,
		map[string]string{"name": ""}, http.StatusUnprocessableEntity)

	// --- cross-tenant denial (rbac-multitenancy.md §5): org B, a complete
	// stranger to org A, gets 404 on GET/PATCH, never 403 (existence must
	// not be confirmable) ---
	assertStatus(t, ctx, client, http.MethodGet, ts.URL+"/v1/orgs/"+orgA, tokenB, nil, http.StatusNotFound)
	assertStatus(t, ctx, client, http.MethodPatch, ts.URL+"/v1/orgs/"+orgA, tokenB,
		map[string]string{"name": "Sneaky Rename"}, http.StatusNotFound)

	// --- DELETE permission matrix (organization.delete: owner only —
	// admin, despite passing organization.manage above, is denied here) ---
	assertStatus(t, ctx, client, http.MethodDelete, ts.URL+"/v1/orgs/"+orgA, tokenViewer, nil, http.StatusForbidden)
	assertStatus(t, ctx, client, http.MethodDelete, ts.URL+"/v1/orgs/"+orgA, tokenDev, nil, http.StatusForbidden)
	assertStatus(t, ctx, client, http.MethodDelete, ts.URL+"/v1/orgs/"+orgA, tokenAdmin, nil, http.StatusForbidden)
	// cross-tenant denial applies to DELETE too, checked before the real
	// delete below since afterward orgA no longer exists for anyone.
	assertStatus(t, ctx, client, http.MethodDelete, ts.URL+"/v1/orgs/"+orgA, tokenB, nil, http.StatusNotFound)

	assertStatus(t, ctx, client, http.MethodDelete, ts.URL+"/v1/orgs/"+orgA, tokenOwner, nil, http.StatusNoContent)

	// --- cascade actually ran: even the former owner's own still-valid
	// JWT gets 404 now, since their memberships row (and everything else
	// FK-chained from organizations) is gone (OrganizationRepository.Delete's
	// own doc comment) ---
	assertStatus(t, ctx, client, http.MethodGet, ts.URL+"/v1/orgs/"+orgA, tokenOwner, nil, http.StatusNotFound)
	assertStatus(t, ctx, client, http.MethodDelete, ts.URL+"/v1/orgs/"+orgA, tokenOwner, nil, http.StatusNotFound)
}

// TestAPIServer_Members is Task 4's acceptance
// (phase-6-multi-tenant-saas.md): GET/POST/PATCH/DELETE
// /v1/orgs/:orgId/members against real Postgres — the full invite -> list ->
// change-role -> remove flow, the permission-matrix check for
// members.invite/remove/change_role (owner/admin allowed, developer/viewer
// denied), cross-tenant denial (404), the last-owner guard on both remove
// and change-role, and inviting an unregistered email's 404 error path.
func TestAPIServer_Members(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	_, _, pool := startTestPostgres(t, ctx)

	issuer, err := auth.NewTokenIssuer("test-signing-key")
	if err != nil {
		t.Fatalf("NewTokenIssuer: %v", err)
	}
	srv := New(pool, issuer, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(srv.Routes())
	t.Cleanup(ts.Close)

	client := &http.Client{Timeout: 30 * time.Second}

	// --- org A: owner (from signup) plus seeded admin/developer/viewer
	// memberships, covering every role the matrix distinguishes ---
	signupA := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "members-owner@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	tokenOwner := signupA["access_token"].(string)
	orgA := signupA["org"].(map[string]any)["id"].(string)
	orgAID, err := uuid.Parse(orgA)
	if err != nil {
		t.Fatalf("parsing org A id: %v", err)
	}
	ownerID, err := uuid.Parse(signupA["user"].(map[string]any)["id"].(string))
	if err != nil {
		t.Fatalf("parsing owner id: %v", err)
	}

	signupAdmin := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "members-admin@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	tokenAdmin := signupAdmin["access_token"].(string)
	adminID, err := uuid.Parse(signupAdmin["user"].(map[string]any)["id"].(string))
	if err != nil {
		t.Fatalf("parsing admin id: %v", err)
	}

	signupDev := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "members-dev@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	tokenDev := signupDev["access_token"].(string)
	devID, err := uuid.Parse(signupDev["user"].(map[string]any)["id"].(string))
	if err != nil {
		t.Fatalf("parsing developer id: %v", err)
	}

	signupViewer := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "members-viewer@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	tokenViewer := signupViewer["access_token"].(string)
	viewerID, err := uuid.Parse(signupViewer["user"].(map[string]any)["id"].(string))
	if err != nil {
		t.Fatalf("parsing viewer id: %v", err)
	}

	if err := pool.WithTx(ctx, ownerID, orgAID, func(ctx context.Context, conn db.Conn) error {
		memberships := db.NewMembershipRepository(conn)
		if _, err := memberships.Create(ctx, orgAID, adminID, db.MembershipRoleAdmin); err != nil {
			return err
		}
		if _, err := memberships.Create(ctx, orgAID, devID, db.MembershipRoleDeveloper); err != nil {
			return err
		}
		_, err := memberships.Create(ctx, orgAID, viewerID, db.MembershipRoleViewer)
		return err
	}); err != nil {
		t.Fatalf("seeding org A admin/developer/viewer memberships: %v", err)
	}

	// --- a registered-but-not-yet-a-member user, for the invite happy path
	// --- and an org B, unrelated, for cross-tenant denial checks ---
	signupInvitee := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "members-invitee@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	inviteeEmail := signupInvitee["user"].(map[string]any)["email"].(string)

	signupB := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "members-b@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	tokenB := signupB["access_token"].(string)

	// --- POST permission matrix (members.invite: owner/admin allowed,
	// developer/viewer denied) ---
	assertStatus(t, ctx, client, http.MethodPost, ts.URL+"/v1/orgs/"+orgA+"/members", tokenViewer,
		map[string]any{"email": inviteeEmail, "role": "viewer"}, http.StatusForbidden)
	assertStatus(t, ctx, client, http.MethodPost, ts.URL+"/v1/orgs/"+orgA+"/members", tokenDev,
		map[string]any{"email": inviteeEmail, "role": "viewer"}, http.StatusForbidden)
	invited := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/orgs/"+orgA+"/members", tokenAdmin,
		map[string]any{"email": inviteeEmail, "role": "viewer"}, http.StatusCreated)
	if invited["email"] != inviteeEmail || invited["role"] != "viewer" {
		t.Fatalf("invited member = %+v, want email=%s role=viewer", invited, inviteeEmail)
	}

	// --- inviting an unregistered email -> 404, not a silent no-op (open
	// decision 3) ---
	assertStatus(t, ctx, client, http.MethodPost, ts.URL+"/v1/orgs/"+orgA+"/members", tokenOwner,
		map[string]any{"email": "no-such-account@example.com", "role": "viewer"}, http.StatusNotFound)

	// --- inviting an already-existing member again -> 409 ---
	assertStatus(t, ctx, client, http.MethodPost, ts.URL+"/v1/orgs/"+orgA+"/members", tokenOwner,
		map[string]any{"email": inviteeEmail, "role": "developer"}, http.StatusConflict)

	// --- cross-tenant denial (rbac-multitenancy.md §5): org B gets 404 on
	// every members route touching org A, never 403 ---
	assertStatus(t, ctx, client, http.MethodGet, ts.URL+"/v1/orgs/"+orgA+"/members", tokenB, nil, http.StatusNotFound)
	assertStatus(t, ctx, client, http.MethodPost, ts.URL+"/v1/orgs/"+orgA+"/members", tokenB,
		map[string]any{"email": inviteeEmail, "role": "viewer"}, http.StatusNotFound)
	assertStatus(t, ctx, client, http.MethodPatch, ts.URL+"/v1/orgs/"+orgA+"/members/"+viewerID.String(), tokenB,
		map[string]any{"role": "admin"}, http.StatusNotFound)
	assertStatus(t, ctx, client, http.MethodDelete, ts.URL+"/v1/orgs/"+orgA+"/members/"+viewerID.String(), tokenB,
		nil, http.StatusNotFound)

	// --- GET: RLS, not a permission gate, decides what comes back
	// (MembershipRepository.ListByOrg's own doc comment) — owner/admin see
	// the full 5-row roster (owner, admin, dev, viewer, invitee), a
	// developer/viewer session sees only its own single row ---
	listOwner := doJSON(t, ctx, client, http.MethodGet, ts.URL+"/v1/orgs/"+orgA+"/members", tokenOwner, nil, http.StatusOK)
	if data, _ := listOwner["data"].([]any); len(data) != 5 {
		t.Fatalf("owner's member list length = %d, want 5 (%+v)", len(data), listOwner)
	}
	listDev := doJSON(t, ctx, client, http.MethodGet, ts.URL+"/v1/orgs/"+orgA+"/members", tokenDev, nil, http.StatusOK)
	if data, _ := listDev["data"].([]any); len(data) != 1 {
		t.Fatalf("developer's member list length = %d, want 1 (own row only, %+v)", len(data), listDev)
	}

	// --- PATCH permission matrix (members.change_role: owner/admin
	// allowed, developer/viewer denied) ---
	assertStatus(t, ctx, client, http.MethodPatch, ts.URL+"/v1/orgs/"+orgA+"/members/"+viewerID.String(), tokenViewer,
		map[string]any{"role": "admin"}, http.StatusForbidden)
	assertStatus(t, ctx, client, http.MethodPatch, ts.URL+"/v1/orgs/"+orgA+"/members/"+viewerID.String(), tokenDev,
		map[string]any{"role": "admin"}, http.StatusForbidden)
	changed := doJSON(t, ctx, client, http.MethodPatch, ts.URL+"/v1/orgs/"+orgA+"/members/"+viewerID.String(), tokenAdmin,
		map[string]any{"role": "developer"}, http.StatusOK)
	if changed["role"] != "developer" {
		t.Fatalf("changed member role = %v, want developer", changed["role"])
	}
	// invalid role string -> 422
	assertStatus(t, ctx, client, http.MethodPatch, ts.URL+"/v1/orgs/"+orgA+"/members/"+viewerID.String(), tokenOwner,
		map[string]any{"role": "superuser"}, http.StatusUnprocessableEntity)
	// unknown member id -> 404
	assertStatus(t, ctx, client, http.MethodPatch, ts.URL+"/v1/orgs/"+orgA+"/members/"+uuid.New().String(), tokenOwner,
		map[string]any{"role": "admin"}, http.StatusNotFound)

	// --- DELETE permission matrix (members.remove: owner/admin allowed,
	// developer/viewer denied) ---
	assertStatus(t, ctx, client, http.MethodDelete, ts.URL+"/v1/orgs/"+orgA+"/members/"+viewerID.String(), tokenViewer,
		nil, http.StatusForbidden)
	assertStatus(t, ctx, client, http.MethodDelete, ts.URL+"/v1/orgs/"+orgA+"/members/"+viewerID.String(), tokenDev,
		nil, http.StatusForbidden)
	assertStatus(t, ctx, client, http.MethodDelete, ts.URL+"/v1/orgs/"+orgA+"/members/"+viewerID.String(), tokenAdmin,
		nil, http.StatusNoContent)
	assertStatus(t, ctx, client, http.MethodDelete, ts.URL+"/v1/orgs/"+orgA+"/members/"+viewerID.String(), tokenAdmin,
		nil, http.StatusNotFound)

	// --- last-owner guard: org A has exactly one owner right now
	// (ownerID) — neither demoting nor removing them may succeed ---
	assertStatus(t, ctx, client, http.MethodPatch, ts.URL+"/v1/orgs/"+orgA+"/members/"+ownerID.String(), tokenAdmin,
		map[string]any{"role": "admin"}, http.StatusUnprocessableEntity)
	assertStatus(t, ctx, client, http.MethodDelete, ts.URL+"/v1/orgs/"+orgA+"/members/"+ownerID.String(), tokenAdmin,
		nil, http.StatusUnprocessableEntity)

	// --- promoting a second member to owner lifts the guard: the
	// now-former-sole-owner can be demoted, and the newly promoted owner can
	// later be removed, once there are two owners again ---
	promoted := doJSON(t, ctx, client, http.MethodPatch, ts.URL+"/v1/orgs/"+orgA+"/members/"+devID.String(), tokenOwner,
		map[string]any{"role": "owner"}, http.StatusOK)
	if promoted["role"] != "owner" {
		t.Fatalf("promoted member role = %v, want owner", promoted["role"])
	}
	assertStatus(t, ctx, client, http.MethodPatch, ts.URL+"/v1/orgs/"+orgA+"/members/"+ownerID.String(), tokenAdmin,
		map[string]any{"role": "admin"}, http.StatusOK)
	// org A now has exactly one owner again (devID) — the guard re-engages
	assertStatus(t, ctx, client, http.MethodDelete, ts.URL+"/v1/orgs/"+orgA+"/members/"+devID.String(), tokenAdmin,
		nil, http.StatusUnprocessableEntity)
}

// TestAPIServer_APIKeys is the Task 5 (phase-6-multi-tenant-saas.md)
// acceptance test: create/list/revoke, the api_keys.create/revoke
// permission matrix, cross-tenant denial, a revoked key's requests being
// rejected, and the auth-path wiring itself — an existing route
// (GET .../deployments) authenticated end to end via a plaintext API key
// instead of a JWT, including the org-scope-narrowing check
// (requireAPIKeyOrgMatch, permission.go) that keeps a key confined to its
// own org even when its creator legitimately belongs to others too.
func TestAPIServer_APIKeys(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	_, _, pool := startTestPostgres(t, ctx)

	issuer, err := auth.NewTokenIssuer("test-signing-key")
	if err != nil {
		t.Fatalf("NewTokenIssuer: %v", err)
	}
	srv := New(pool, issuer, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(srv.Routes())
	t.Cleanup(ts.Close)

	client := &http.Client{Timeout: 30 * time.Second}

	// --- org A: owner (from signup) plus seeded admin/developer/viewer
	// memberships, covering every role the matrix distinguishes ---
	signupA := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "api-keys-owner@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	tokenOwner := signupA["access_token"].(string)
	orgA := signupA["org"].(map[string]any)["id"].(string)
	orgAID, err := uuid.Parse(orgA)
	if err != nil {
		t.Fatalf("parsing org A id: %v", err)
	}
	ownerID, err := uuid.Parse(signupA["user"].(map[string]any)["id"].(string))
	if err != nil {
		t.Fatalf("parsing owner id: %v", err)
	}

	signupAdmin := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "api-keys-admin@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	tokenAdmin := signupAdmin["access_token"].(string)
	adminID, err := uuid.Parse(signupAdmin["user"].(map[string]any)["id"].(string))
	if err != nil {
		t.Fatalf("parsing admin id: %v", err)
	}

	signupDev := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "api-keys-dev@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	tokenDev := signupDev["access_token"].(string)
	devID, err := uuid.Parse(signupDev["user"].(map[string]any)["id"].(string))
	if err != nil {
		t.Fatalf("parsing developer id: %v", err)
	}

	signupViewer := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "api-keys-viewer@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	tokenViewer := signupViewer["access_token"].(string)
	viewerID, err := uuid.Parse(signupViewer["user"].(map[string]any)["id"].(string))
	if err != nil {
		t.Fatalf("parsing viewer id: %v", err)
	}

	if err := pool.WithTx(ctx, ownerID, orgAID, func(ctx context.Context, conn db.Conn) error {
		memberships := db.NewMembershipRepository(conn)
		if _, err := memberships.Create(ctx, orgAID, adminID, db.MembershipRoleAdmin); err != nil {
			return err
		}
		if _, err := memberships.Create(ctx, orgAID, devID, db.MembershipRoleDeveloper); err != nil {
			return err
		}
		_, err := memberships.Create(ctx, orgAID, viewerID, db.MembershipRoleViewer)
		return err
	}); err != nil {
		t.Fatalf("seeding org A admin/developer/viewer memberships: %v", err)
	}

	// --- org B: unrelated, for cross-tenant denial ---
	signupB := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "api-keys-b@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	tokenB := signupB["access_token"].(string)
	orgB := signupB["org"].(map[string]any)["id"].(string)
	orgBID, err := uuid.Parse(orgB)
	if err != nil {
		t.Fatalf("parsing org B id: %v", err)
	}

	// --- POST permission matrix (api_keys.create: owner/admin allowed,
	// developer/viewer denied) ---
	assertStatus(t, ctx, client, http.MethodPost, ts.URL+"/v1/orgs/"+orgA+"/api-keys", tokenViewer,
		map[string]any{"name": "viewer-attempt"}, http.StatusForbidden)
	assertStatus(t, ctx, client, http.MethodPost, ts.URL+"/v1/orgs/"+orgA+"/api-keys", tokenDev,
		map[string]any{"name": "dev-attempt"}, http.StatusForbidden)
	created := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/orgs/"+orgA+"/api-keys", tokenAdmin,
		map[string]any{"name": "ci-key"}, http.StatusCreated)
	if created["name"] != "ci-key" {
		t.Fatalf("created key name = %v, want ci-key", created["name"])
	}
	revokedKeyID, _ := created["id"].(string)
	revokedKeyPlain, _ := created["key"].(string)
	if revokedKeyID == "" || !strings.HasPrefix(revokedKeyPlain, "pk_live_") {
		t.Fatalf("created key = %+v, want an id and a pk_live_-prefixed key", created)
	}

	// --- GET permission matrix: api_keys.create gates the list too (matrix
	// has no separate list permission — handleListAPIKeys' own doc
	// comment) — and the plaintext key is never present in a list response,
	// only on the one-time create response above ---
	assertStatus(t, ctx, client, http.MethodGet, ts.URL+"/v1/orgs/"+orgA+"/api-keys", tokenViewer, nil, http.StatusForbidden)
	assertStatus(t, ctx, client, http.MethodGet, ts.URL+"/v1/orgs/"+orgA+"/api-keys", tokenDev, nil, http.StatusForbidden)
	listOwner := doJSON(t, ctx, client, http.MethodGet, ts.URL+"/v1/orgs/"+orgA+"/api-keys", tokenOwner, nil, http.StatusOK)
	data, _ := listOwner["data"].([]any)
	if len(data) != 1 {
		t.Fatalf("org A key list length = %d, want 1 (%+v)", len(data), listOwner)
	}
	if entry, ok := data[0].(map[string]any); !ok || entry["key"] != nil {
		t.Fatalf("list entry leaked a plaintext key: %+v", data[0])
	}

	// --- cross-tenant denial (rbac-multitenancy.md §5): org B gets 404 on
	// every api-keys route touching org A, never 403 ---
	assertStatus(t, ctx, client, http.MethodPost, ts.URL+"/v1/orgs/"+orgA+"/api-keys", tokenB,
		map[string]any{"name": "sneaky"}, http.StatusNotFound)
	assertStatus(t, ctx, client, http.MethodGet, ts.URL+"/v1/orgs/"+orgA+"/api-keys", tokenB, nil, http.StatusNotFound)
	assertStatus(t, ctx, client, http.MethodDelete, ts.URL+"/v1/api-keys/"+revokedKeyID, tokenB, nil, http.StatusNotFound)

	// --- DELETE permission matrix (api_keys.revoke: owner/admin allowed,
	// developer/viewer denied); a second revoke of the same key is a 404,
	// not a no-op 204 (Revoke only matches revoked_at IS NULL rows) ---
	assertStatus(t, ctx, client, http.MethodDelete, ts.URL+"/v1/api-keys/"+revokedKeyID, tokenViewer, nil, http.StatusForbidden)
	assertStatus(t, ctx, client, http.MethodDelete, ts.URL+"/v1/api-keys/"+revokedKeyID, tokenDev, nil, http.StatusForbidden)
	assertStatus(t, ctx, client, http.MethodDelete, ts.URL+"/v1/api-keys/"+revokedKeyID, tokenAdmin, nil, http.StatusNoContent)
	assertStatus(t, ctx, client, http.MethodDelete, ts.URL+"/v1/api-keys/"+revokedKeyID, tokenAdmin, nil, http.StatusNotFound)

	// --- a revoked key's requests are rejected outright (401), same as any
	// other invalid bearer credential — not merely denied by a downstream
	// permission check ---
	assertStatus(t, ctx, client, http.MethodGet, ts.URL+"/v1/orgs/"+orgA+"/api-keys", revokedKeyPlain, nil, http.StatusUnauthorized)

	// --- auth-path wiring: a fresh, active key authenticates an existing,
	// unrelated route end to end (GET .../deployments), not just the
	// api-keys CRUD routes themselves ---
	activeKey := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/orgs/"+orgA+"/api-keys", tokenOwner,
		map[string]any{"name": "e2e-key"}, http.StatusCreated)
	activeKeyPlain, _ := activeKey["key"].(string)
	if !strings.HasPrefix(activeKeyPlain, "pk_live_") {
		t.Fatalf("active key = %+v, want a pk_live_-prefixed key", activeKey)
	}

	project := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/orgs/"+orgA+"/projects", tokenOwner,
		map[string]string{"name": "Demo Project"}, http.StatusCreated)
	projectID := project["id"].(string)
	app := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/projects/"+projectID+"/applications", tokenOwner,
		map[string]any{"name": "demo", "image": "nginx:latest"}, http.StatusCreated)
	appID := app["id"].(string)

	deployments := doJSON(t, ctx, client, http.MethodGet, ts.URL+"/v1/applications/"+appID+"/deployments", activeKeyPlain, nil, http.StatusOK)
	if depData, _ := deployments["data"].([]any); len(depData) != 0 {
		t.Fatalf("deployments via API key = %+v, want an empty list (none created yet)", deployments)
	}

	// --- org-scope narrowing (permission.go's requireAPIKeyOrgMatch): the
	// key's creator (ownerID) is now also a legitimate developer member of
	// org B — a real scenario, one person belonging to multiple orgs — but
	// the key itself was created scoped to org A only, and must not reach
	// org B's resources even though ownerID's own JWT now legitimately can.
	if err := pool.WithTx(ctx, ownerID, orgBID, func(ctx context.Context, conn db.Conn) error {
		_, err := db.NewMembershipRepository(conn).Create(ctx, orgBID, ownerID, db.MembershipRoleDeveloper)
		return err
	}); err != nil {
		t.Fatalf("adding org A's owner as a member of org B: %v", err)
	}

	projectB := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/orgs/"+orgB+"/projects", tokenB,
		map[string]string{"name": "Other Project"}, http.StatusCreated)
	projectBID := projectB["id"].(string)
	appB := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/projects/"+projectBID+"/applications", tokenB,
		map[string]any{"name": "other", "image": "nginx:latest"}, http.StatusCreated)
	appBID := appB["id"].(string)

	// org A's API key: rejected, even though its creator is a real member of org B
	assertStatus(t, ctx, client, http.MethodGet, ts.URL+"/v1/applications/"+appBID+"/deployments", activeKeyPlain, nil, http.StatusNotFound)
	// contrast case: the same person's ordinary JWT succeeds, proving the
	// denial above is the API key's own org-scoping, not a membership gap
	assertStatus(t, ctx, client, http.MethodGet, ts.URL+"/v1/applications/"+appBID+"/deployments", tokenOwner, nil, http.StatusOK)
}

// TestAPIServer_Quotas is Layer 1's (API server) acceptance test for Task 6
// (phase-6-multi-tenant-saas.md, ARCHITECTURE.md §2.9): project.create,
// deploy, and scale each reject with 422 quota_exceeded exactly at their
// org's ceiling, and stay within it right up to the boundary. A real NATS
// bus backs this test (no scheduler/worker) so a quota-permitted deploy can
// actually reach handleDeploy's publish step and come back 201 'pending' —
// distinguishing "the quota check let this through" from "it was blocked"
// without needing the heavier scheduler+worker stack CoreCRUDFlow/
// ScaleApplication already cover placement with. Quota ceilings are lowered
// directly via SQL (mirroring internal/db's own seed-via-admin-connection
// precedent) rather than through any API/CLI route — there deliberately
// isn't one; quota limits are platform-operator-set, not self-service.
func TestAPIServer_Quotas(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	_, _, pool := startTestPostgres(t, ctx)
	natsURL := startTestNATS(t, ctx)

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
	srv := New(pool, issuer, nil, bus, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(srv.Routes())
	t.Cleanup(ts.Close)

	client := &http.Client{Timeout: 30 * time.Second}

	// --- org Proj: MaxProjects lowered to 1 — tests project.create's own
	// quota check in isolation ---
	signupProj := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "quota-proj-owner@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	tokenProj := signupProj["access_token"].(string)
	orgProj := signupProj["org"].(map[string]any)["id"].(string)
	orgProjID, err := uuid.Parse(orgProj)
	if err != nil {
		t.Fatalf("parsing org Proj id: %v", err)
	}
	userProjID, err := uuid.Parse(signupProj["user"].(map[string]any)["id"].(string))
	if err != nil {
		t.Fatalf("parsing org Proj's owner id: %v", err)
	}
	// resource_quotas carries RLS (0010_resource_quotas.sql) — the update
	// must run with app.current_org_id set to satisfy its policy, same as
	// every other write against an RLS-bound table in this codebase; a bare
	// pool.Conn().Exec with no session vars set would silently match zero
	// rows instead of erroring.
	if err := pool.WithTx(ctx, userProjID, orgProjID, func(ctx context.Context, conn db.Conn) error {
		_, err := conn.Exec(ctx, `UPDATE resource_quotas SET max_projects = 1 WHERE org_id = $1`, orgProjID)
		return err
	}); err != nil {
		t.Fatalf("lowering org Proj's max_projects: %v", err)
	}

	doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/orgs/"+orgProj+"/projects", tokenProj,
		map[string]string{"name": "First"}, http.StatusCreated)
	assertStatus(t, ctx, client, http.MethodPost, ts.URL+"/v1/orgs/"+orgProj+"/projects", tokenProj,
		map[string]string{"name": "Second"}, http.StatusUnprocessableEntity)

	// --- org Res: MaxContainers=1, MaxDeploymentsPerDay=2 — tests the
	// container-count check deploy and scale share, plus deploy's own
	// deployments-per-day check ---
	signupRes := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "quota-res-owner@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	tokenRes := signupRes["access_token"].(string)
	orgRes := signupRes["org"].(map[string]any)["id"].(string)
	orgResID, err := uuid.Parse(orgRes)
	if err != nil {
		t.Fatalf("parsing org Res id: %v", err)
	}
	userResID, err := uuid.Parse(signupRes["user"].(map[string]any)["id"].(string))
	if err != nil {
		t.Fatalf("parsing org Res's owner id: %v", err)
	}
	if err := pool.WithTx(ctx, userResID, orgResID, func(ctx context.Context, conn db.Conn) error {
		_, err := conn.Exec(ctx, `UPDATE resource_quotas SET max_containers = 1, max_deployments_per_day = 2 WHERE org_id = $1`, orgResID)
		return err
	}); err != nil {
		t.Fatalf("lowering org Res's quota: %v", err)
	}

	project := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/orgs/"+orgRes+"/projects", tokenRes,
		map[string]string{"name": "Demo Project"}, http.StatusCreated)
	projectID := project["id"].(string)
	app := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/projects/"+projectID+"/applications", tokenRes,
		map[string]any{"name": "demo", "image": "nginx:latest"}, http.StatusCreated)
	appID := app["id"].(string)

	// scaling to 2 replicas before any deploy exists yet: 0 actual
	// containers + 2 requested > MaxContainers(1) -> rejected
	assertStatus(t, ctx, client, http.MethodPatch, ts.URL+"/v1/applications/"+appID, tokenRes,
		map[string]any{"replicas_desired": 2}, http.StatusUnprocessableEntity)

	// first deploy: at the boundary (0 existing + replicas_desired(1) ==
	// MaxContainers(1)) -> succeeds, and DeploymentsToday(0) < 2 -> the
	// deployments-per-day check passes too
	deploy1 := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/applications/"+appID+"/deployments", tokenRes,
		nil, http.StatusCreated)
	if deploy1["status"] != "pending" {
		t.Fatalf("first deploy status = %v, want pending (%+v)", deploy1["status"], deploy1)
	}

	// second deploy: still at the container boundary (no scheduler is
	// running in this test, so this app has never actually had a container
	// placed regardless of which deployment revision is "latest"), and
	// DeploymentsToday(1) < 2 -> still succeeds
	doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/applications/"+appID+"/deployments", tokenRes,
		nil, http.StatusCreated)

	// third deploy: DeploymentsToday is now 2, >= MaxDeploymentsPerDay(2)
	// -> rejected before the container check is even reached
	assertStatus(t, ctx, client, http.MethodPost, ts.URL+"/v1/applications/"+appID+"/deployments", tokenRes,
		nil, http.StatusUnprocessableEntity)

	// scaling up to 2 replicas: still 0 actual containers (nothing ever
	// got placed) + 2 requested > MaxContainers(1) -> rejected
	assertStatus(t, ctx, client, http.MethodPatch, ts.URL+"/v1/applications/"+appID, tokenRes,
		map[string]any{"replicas_desired": 2}, http.StatusUnprocessableEntity)

	// scaling to exactly 1 (unchanged) stays at the boundary -> succeeds
	assertStatus(t, ctx, client, http.MethodPatch, ts.URL+"/v1/applications/"+appID, tokenRes,
		map[string]any{"replicas_desired": 1}, http.StatusOK)

	// scaling down is never blocked by quota, regardless of how tight it is
	assertStatus(t, ctx, client, http.MethodPatch, ts.URL+"/v1/applications/"+appID, tokenRes,
		map[string]any{"replicas_desired": 0}, http.StatusOK)

	// --- GET .../quota reflects the lowered ceilings and live usage ---
	quota := doJSON(t, ctx, client, http.MethodGet, ts.URL+"/v1/orgs/"+orgRes+"/quota", tokenRes, nil, http.StatusOK)
	if got, _ := quota["max_containers"].(float64); got != 1 {
		t.Fatalf("quota max_containers = %v, want 1 (%+v)", quota["max_containers"], quota)
	}
	if got, _ := quota["used_projects"].(float64); got != 1 {
		t.Fatalf("quota used_projects = %v, want 1 (%+v)", quota["used_projects"], quota)
	}
	// cross-tenant denial on the quota route, same as every other org-in-URL
	// route (rbac-multitenancy.md §5)
	assertStatus(t, ctx, client, http.MethodGet, ts.URL+"/v1/orgs/"+orgRes+"/quota", tokenProj, nil, http.StatusNotFound)
}

// TestAPIServer_QuotaThreeLayers is Task 6's (phase-6-multi-tenant-saas.md)
// acceptance requirement that all three quota layers agree on the same
// boundary — see TestAPIServer_Quotas (Layer 1, this package),
// TestScheduler_RejectsPlacementOverQuota (Layer 2,
// services/scheduler/main_integration_test.go), and
// TestQuotaEnforcementTriggers (Layer 3, internal/db) for each layer's own
// isolated coverage; this is the one test where all three run against the
// very same org, at the very same MaxContainers=1 ceiling, in sequence —
// against a real Postgres, real NATS, a real scheduler, and a real worker,
// so the first deploy actually lands a real container to be "at the
// boundary" against.
func TestAPIServer_QuotaThreeLayers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	adminDBURL, appDBURL, pool := startTestPostgres(t, ctx)
	natsURL := startTestNATS(t, ctx)
	startTestScheduler(t, ctx, appDBURL, adminDBURL, natsURL)
	startTestWorker(t, ctx, natsURL)

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
	srv := New(pool, issuer, nil, bus, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(srv.Routes())
	t.Cleanup(ts.Close)

	client := &http.Client{Timeout: 30 * time.Second}

	signup := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "quota-3layer-owner@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	token := signup["access_token"].(string)
	org := signup["org"].(map[string]any)["id"].(string)
	orgID, err := uuid.Parse(org)
	if err != nil {
		t.Fatalf("parsing org id: %v", err)
	}
	userID, err := uuid.Parse(signup["user"].(map[string]any)["id"].(string))
	if err != nil {
		t.Fatalf("parsing user id: %v", err)
	}

	// The one boundary all three layers are being asked to agree on.
	if err := pool.WithTx(ctx, userID, orgID, func(ctx context.Context, conn db.Conn) error {
		_, err := conn.Exec(ctx, `UPDATE resource_quotas SET max_containers = 1 WHERE org_id = $1`, orgID)
		return err
	}); err != nil {
		t.Fatalf("lowering max_containers: %v", err)
	}

	project := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/orgs/"+org+"/projects", token,
		map[string]string{"name": "Demo"}, http.StatusCreated)
	projectID := project["id"].(string)
	app := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/projects/"+projectID+"/applications", token,
		map[string]any{"name": "demo", "image": "nginx:latest"}, http.StatusCreated)
	appID := app["id"].(string)

	// --- Layer 1 (API server), first deploy: exactly at the boundary
	// (0 existing + replicas_desired(1) == MaxContainers(1)) -> succeeds,
	// and the real scheduler+worker actually place it.
	deployment := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/applications/"+appID+"/deployments", token,
		nil, http.StatusCreated)
	deploymentID, err := uuid.Parse(deployment["id"].(string))
	if err != nil {
		t.Fatalf("parsing deployment id: %v", err)
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

	containers := db.NewContainerRepository(pool.Conn())
	var placed db.Container
	deadline := time.Now().Add(30 * time.Second)
	for {
		rows, err := containers.ListByDeployment(ctx, deploymentID)
		if err != nil {
			t.Fatalf("ListByDeployment: %v", err)
		}
		if len(rows) == 1 {
			placed = rows[0]
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for the boundary container to be placed, rows=%+v", rows)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// --- Layer 1 (API server) again: scaling to 2 replicas would need a
	// 2nd container -> rejected with 422 quota_exceeded, the real API's own
	// ordinary response.
	assertStatus(t, ctx, client, http.MethodPatch, ts.URL+"/v1/applications/"+appID, token,
		map[string]any{"replicas_desired": 2}, http.StatusUnprocessableEntity)

	// --- Layer 2 (scheduler): bypass the API entirely — publish a raw
	// placement.requested for the same deployment directly, exactly what
	// the Deployment controller would send for one more replica, and
	// exactly what a stale Layer 1 check could have let through. The real
	// scheduler is watching this same subject; give it a real window to
	// (wrongly) act before asserting it didn't.
	placementData, err := json.Marshal(map[string]any{
		"deployment_id":  deploymentID.String(),
		"application_id": appID,
		"image":          "nginx:latest",
	})
	if err != nil {
		t.Fatalf("marshaling placement.requested: %v", err)
	}
	if err := bus.PublishDurable(ctx, eventbus.PlacementRequestedSubject, placementData); err != nil {
		t.Fatalf("publishing placement.requested: %v", err)
	}
	time.Sleep(3 * time.Second)
	rows, err := containers.ListByDeployment(ctx, deploymentID)
	if err != nil {
		t.Fatalf("ListByDeployment after bypassing Layer 1: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("Layer 2 should have rejected the extra placement, got %d containers: %+v", len(rows), rows)
	}

	// --- Layer 3 (Postgres): bypass both — insert directly into
	// containers, exactly as a raw SQL statement from any other source
	// would, and confirm the trigger itself rejects it.
	_, err = pool.Conn().Exec(ctx, `INSERT INTO containers (deployment_id, node_id) VALUES ($1, $2)`, deploymentID, placed.NodeID)
	if err == nil {
		t.Fatal("expected the direct over-quota container insert to be rejected by the trigger, got no error")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != pgQuotaCheckViolation {
		t.Fatalf("expected a check_violation (%s) from the trigger, got: %v", pgQuotaCheckViolation, err)
	}
}

// pgQuotaCheckViolation is the SQLSTATE
// 0013_quota_enforcement_trigger.sql's triggers raise on rejection —
// mirrors internal/db's own quota_trigger_integration_test.go constant
// (unexported there, in a different package).
const pgQuotaCheckViolation = "23514"

// TestAPIServer_AuditLogs covers Task 7's acceptance
// (phase-6-multi-tenant-saas.md): a representative mutating action from
// each of projects/applications/domains/orgs/members/api-keys produces a
// matching audit_logs row (written synchronously, per handler, over the
// request's own connection — not through any NATS path, so a lightweight
// Postgres-only server here is enough, same posture as TestAPIServer_Quotas),
// the read route's permission matrix and cross-tenant denial, and a direct
// UPDATE/DELETE against audit_logs failing regardless of caller role —
// re-confirming Task 1's DB-level guarantee end-to-end through the API
// server's own (non-superuser) connection, not just internal/db's own test.
func TestAPIServer_AuditLogs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	_, _, pool := startTestPostgres(t, ctx)

	issuer, err := auth.NewTokenIssuer("test-signing-key")
	if err != nil {
		t.Fatalf("NewTokenIssuer: %v", err)
	}
	srv := New(pool, issuer, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(srv.Routes())
	t.Cleanup(ts.Close)

	client := &http.Client{Timeout: 30 * time.Second}

	// --- org A: owner (from signup) plus seeded admin/developer/viewer
	// memberships, covering every role audit_logs.view's matrix row
	// distinguishes (owner/admin allowed, developer/viewer denied) ---
	signupA := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "audit-owner@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	tokenOwner := signupA["access_token"].(string)
	orgA := signupA["org"].(map[string]any)["id"].(string)
	orgAID, err := uuid.Parse(orgA)
	if err != nil {
		t.Fatalf("parsing org A id: %v", err)
	}
	ownerID, err := uuid.Parse(signupA["user"].(map[string]any)["id"].(string))
	if err != nil {
		t.Fatalf("parsing owner id: %v", err)
	}

	signupAdmin := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "audit-admin@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	tokenAdmin := signupAdmin["access_token"].(string)
	adminID, err := uuid.Parse(signupAdmin["user"].(map[string]any)["id"].(string))
	if err != nil {
		t.Fatalf("parsing admin id: %v", err)
	}

	signupDev := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "audit-dev@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	tokenDev := signupDev["access_token"].(string)
	devID, err := uuid.Parse(signupDev["user"].(map[string]any)["id"].(string))
	if err != nil {
		t.Fatalf("parsing developer id: %v", err)
	}

	signupViewer := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "audit-viewer@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	tokenViewer := signupViewer["access_token"].(string)
	viewerID, err := uuid.Parse(signupViewer["user"].(map[string]any)["id"].(string))
	if err != nil {
		t.Fatalf("parsing viewer id: %v", err)
	}

	// A separate, already-registered user who isn't a member of org A yet —
	// handleInviteMember (open decision 3) only works against an existing
	// account, looked up by email.
	signupInvitee := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "audit-invitee@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)

	if err := pool.WithTx(ctx, ownerID, orgAID, func(ctx context.Context, conn db.Conn) error {
		memberships := db.NewMembershipRepository(conn)
		if _, err := memberships.Create(ctx, orgAID, adminID, db.MembershipRoleAdmin); err != nil {
			return err
		}
		if _, err := memberships.Create(ctx, orgAID, devID, db.MembershipRoleDeveloper); err != nil {
			return err
		}
		_, err := memberships.Create(ctx, orgAID, viewerID, db.MembershipRoleViewer)
		return err
	}); err != nil {
		t.Fatalf("seeding org A admin/developer/viewer memberships: %v", err)
	}

	// --- org B: unrelated, for cross-tenant denial ---
	signupB := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "audit-b@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	tokenB := signupB["access_token"].(string)

	// --- one representative mutating action each from
	// projects/applications/domains/orgs/members/api-keys ---
	project := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/orgs/"+orgA+"/projects", tokenOwner,
		map[string]string{"name": "Demo Project"}, http.StatusCreated)
	projectID := project["id"].(string)

	app := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/projects/"+projectID+"/applications", tokenOwner,
		map[string]any{"name": "demo", "image": "nginx:latest"}, http.StatusCreated)
	appID := app["id"].(string)

	domain := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/projects/"+projectID+"/domains", tokenOwner,
		map[string]any{"hostname": "demo.example.com", "application_id": appID}, http.StatusCreated)
	domainID := domain["id"].(string)

	doJSON(t, ctx, client, http.MethodPatch, ts.URL+"/v1/orgs/"+orgA, tokenOwner,
		map[string]string{"name": "Renamed Org"}, http.StatusOK)

	invitee := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/orgs/"+orgA+"/members", tokenOwner,
		map[string]string{"email": "audit-invitee@example.com", "role": "developer"}, http.StatusCreated)
	inviteeUserID := invitee["user_id"].(string)
	if want := signupInvitee["user"].(map[string]any)["id"].(string); inviteeUserID != want {
		t.Fatalf("invited membership user_id = %v, want %v", inviteeUserID, want)
	}

	apiKey := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/orgs/"+orgA+"/api-keys", tokenOwner,
		map[string]any{"name": "ci-key"}, http.StatusCreated)
	apiKeyID := apiKey["id"].(string)

	// --- GET permission matrix (audit_logs.view: owner/admin allowed,
	// developer/viewer denied) ---
	assertStatus(t, ctx, client, http.MethodGet, ts.URL+"/v1/orgs/"+orgA+"/audit-logs", tokenDev, nil, http.StatusForbidden)
	assertStatus(t, ctx, client, http.MethodGet, ts.URL+"/v1/orgs/"+orgA+"/audit-logs", tokenViewer, nil, http.StatusForbidden)
	assertStatus(t, ctx, client, http.MethodGet, ts.URL+"/v1/orgs/"+orgA+"/audit-logs", tokenAdmin, nil, http.StatusOK)

	// --- cross-tenant denial (rbac-multitenancy.md §5): org B gets 404, not
	// 403, even though the route exists and org B has its own audit log ---
	assertStatus(t, ctx, client, http.MethodGet, ts.URL+"/v1/orgs/"+orgA+"/audit-logs", tokenB, nil, http.StatusNotFound)

	// --- the actual log: every action above appears, targeting the right
	// resource, with limit=100 comfortably covering all of them on one page ---
	logResp := doJSON(t, ctx, client, http.MethodGet, ts.URL+"/v1/orgs/"+orgA+"/audit-logs?limit=100", tokenOwner, nil, http.StatusOK)
	entries, _ := logResp["data"].([]any)
	seen := make(map[string]string) // action -> target_id
	for _, raw := range entries {
		e, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("unexpected audit log entry shape: %+v", raw)
		}
		action, _ := e["action"].(string)
		targetID, _ := e["target_id"].(string)
		seen[action] = targetID
		if e["org_id"] != orgA {
			t.Fatalf("audit log entry %+v belongs to org %v, want %v", e, e["org_id"], orgA)
		}
	}
	wantTargets := map[string]string{
		"organization.create": orgA,
		"project.create":      projectID,
		"application.create":  appID,
		"domain.create":       domainID,
		"organization.update": orgA,
		"member.invite":       inviteeUserID,
		"api_key.create":      apiKeyID,
	}
	for action, wantTarget := range wantTargets {
		gotTarget, ok := seen[action]
		if !ok {
			t.Fatalf("expected an audit log entry for action %q, got entries %v", action, seen)
		}
		if gotTarget != wantTarget {
			t.Fatalf("audit log entry for %q has target_id %v, want %v", action, gotTarget, wantTarget)
		}
	}

	// --- append-only, enforced at the DB layer regardless of caller code —
	// re-confirms Task 1's guarantee through the API server's own pool,
	// which runs as platform_app (never as the superuser platform role),
	// exactly like every other request this server ever serves. ---
	const insufficientPrivilege = "42501"
	if _, err := pool.Conn().Exec(ctx, `UPDATE audit_logs SET action = 'tampered' WHERE id = $1`, uuid.New()); err == nil {
		t.Fatal("expected UPDATE against audit_logs to be rejected, it succeeded")
	} else {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != insufficientPrivilege {
			t.Fatalf("expected insufficient_privilege (%s) rejecting UPDATE, got %v", insufficientPrivilege, err)
		}
	}
	if _, err := pool.Conn().Exec(ctx, `DELETE FROM audit_logs WHERE id = $1`, uuid.New()); err == nil {
		t.Fatal("expected DELETE against audit_logs to be rejected, it succeeded")
	} else {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != insufficientPrivilege {
			t.Fatalf("expected insufficient_privilege (%s) rejecting DELETE, got %v", insufficientPrivilege, err)
		}
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

// waitForServiceDNSName polls GET .../deployments — Task 6's read path —
// until the named deployment's service.dns_name is populated. Task 3's
// controller creates the service lazily on the application's first healthy
// instance, the same eventually-consistent shape waitForReplicaCount above
// already polls for.
func waitForServiceDNSName(t *testing.T, ctx context.Context, client *http.Client, ts *httptest.Server, token, appID string, deploymentID uuid.UUID, timeout time.Duration) string {
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
		if svc, ok := d["service"].(map[string]any); ok {
			if dnsName, _ := svc["dns_name"].(string); dnsName != "" {
				return dnsName
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for a service dns_name on deployment %s, last seen: %+v", deploymentID, d)
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
// separate OS process (ADR-0012), pointed at dbURL/adminDBURL/natsURL with
// fast liveness-sweep settings — mirrors services/scheduler's own
// integration test's startTestScheduler. adminDBURL backs Layer 2's quota
// check (phase-6-multi-tenant-saas.md Task 6).
func startTestScheduler(t *testing.T, ctx context.Context, dbURL, adminDBURL, natsURL string) {
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
		"SCHEDULER_ADMIN_DATABASE_URL="+adminDBURL,
		"SCHEDULER_NATS_URL="+natsURL,
		"SCHEDULER_HEARTBEAT_TIMEOUT=60s",
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
		"CONTROLLER_SERVICE_RECONCILE_INTERVAL=1s",
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

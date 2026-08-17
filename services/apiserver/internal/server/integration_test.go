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

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"platform/internal/auth"
	"platform/internal/db"
	"platform/services/apiserver/internal/workerclient"
)

// TestAPIServer_CoreCRUDFlow is the Task 5 acceptance test
// (phase-1-mvp.md): real routes, against a real (test-container) Postgres
// and a real worker agent, covering signup -> create project -> create
// application -> deploy -> get deployments -> delete, plus the required
// cross-tenant denial checks (rbac-multitenancy.md §5). The worker runs as
// an actual separate OS process (its own compiled binary), not a Go
// import — importing services/worker's package tree from here would
// violate ADR-0012 (network-only service boundaries), the exact thing this
// test is meant to exercise honestly.
func TestAPIServer_CoreCRUDFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	pool := startTestPostgres(t, ctx)
	workerAddr := startTestWorker(t, ctx)

	issuer, err := auth.NewTokenIssuer("test-signing-key")
	if err != nil {
		t.Fatalf("NewTokenIssuer: %v", err)
	}
	worker := workerclient.New(workerAddr)
	srv := New(pool, issuer, worker, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

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

	// --- org A: deploy, verify it actually started a container ---
	deployment := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/applications/"+appID+"/deployments", tokenA,
		nil, http.StatusCreated)
	if got := deployment["status"]; got != "running" {
		t.Fatalf("deployment status = %v, want running (full response: %+v)", got, deployment)
	}
	if got, _ := deployment["revision"].(float64); got != 1 {
		t.Fatalf("deployment revision = %v, want 1", deployment["revision"])
	}
	if deployment["worker_container_id"] == nil || deployment["worker_container_id"] == "" {
		t.Fatalf("expected a worker_container_id on a running deployment, got %+v", deployment)
	}

	listResp := doJSON(t, ctx, client, http.MethodGet, ts.URL+"/v1/applications/"+appID+"/deployments", tokenA,
		nil, http.StatusOK)
	data, _ := listResp["data"].([]any)
	if len(data) != 1 {
		t.Fatalf("expected exactly 1 deployment listed, got %d (%+v)", len(data), listResp)
	}

	// --- org A: delete application, confirm it and its deployments are gone ---
	assertStatus(t, ctx, client, http.MethodDelete, ts.URL+"/v1/applications/"+appID, tokenA, nil, http.StatusNoContent)
	assertStatus(t, ctx, client, http.MethodGet, ts.URL+"/v1/applications/"+appID+"/deployments", tokenA, nil, http.StatusNotFound)
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
// returning a *db.Pool connected as the non-superuser platform_app role so
// RLS is actually enforced (ADR-0010).
func startTestPostgres(t *testing.T, ctx context.Context) *db.Pool {
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
	pool, err := db.Open(ctx, appConnStr)
	if err != nil {
		t.Fatalf("opening app-role pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// startTestWorker builds and runs the real worker binary as a separate OS
// process (ADR-0012 — see the test's doc comment for why), returning its
// base URL once it's accepting connections. Requires a real Docker daemon,
// same as services/worker's own integration test.
func startTestWorker(t *testing.T, ctx context.Context) string {
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
	cmd.Env = append(os.Environ(), "WORKER_LISTEN_ADDR="+addr)
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

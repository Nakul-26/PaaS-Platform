//go:build e2e

// Package e2e automates the exit-criteria script at the top of
// phase-1-mvp.md (phase-1-mvp.md Task 8) — this test running green is the
// literal Phase 1 exit-criteria check (testing-strategy.md §1, E2E tier),
// not a description of it. It drives the real compiled `platform` CLI
// binary as a subprocess making real HTTP calls against real, separately
// compiled worker and apiserver processes and a real Postgres instance —
// never through internal Go function calls or by importing another
// service's package tree, since the point of this tier is verifying the
// actual external contract a user experiences, the same boundary ADR-0012
// draws for the services themselves.
//
// Requires a real Docker daemon: for the Postgres testcontainer, and for
// the nginx container the deployed application itself runs as. Run with:
//
//	go test -tags=e2e ./e2e/...
package e2e

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

const testTimeout = 5 * time.Minute

// TestE2E_Phase1ExitCriteria runs the exact script from the top of
// phase-1-mvp.md end to end:
//
//	platform login
//	platform create project demo
//	platform deploy demo --image nginx:latest
//	platform get deployments          # shows running
//	curl <worker-host>:<mapped-port>  # serves nginx
//	platform logs demo
//	platform delete demo              # container stops and is removed
//
// "login" is satisfied by signup: Phase 1 has no separate account-creation
// flow (ARCHITECTURE.md §7) — signup both creates the account and the
// session, exactly as login does for a returning user, and login itself
// has no bootstrapping step left to exercise beyond what signup already
// does for a brand-new account.
func TestE2E_Phase1ExitCriteria(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	goBin := goBinary(t)
	binDir := t.TempDir()

	dbURL := startPostgres(t, ctx)
	workerURL := startService(t, ctx, goBin, binDir, "worker", "platform/services/worker",
		"WORKER_LISTEN_ADDR", nil, "/v1/containers/readiness-probe")
	apiURL := startService(t, ctx, goBin, binDir, "apiserver", "platform/services/apiserver",
		"APISERVER_LISTEN_ADDR", []string{
			"APP_DATABASE_URL=" + dbURL,
			"WORKER_ADDR=" + workerURL,
			"JWT_SIGNING_KEY=e2e-test-signing-key",
		}, "/")

	cliPath := filepath.Join(binDir, "platform"+exeSuffix())
	buildBinary(t, ctx, goBin, cliPath, "platform/apps/cli")
	run := newCLIRunner(t, ctx, cliPath, t.TempDir(), apiURL)

	hostPort := mustFreePort(t)

	// platform login
	email := fmt.Sprintf("e2e-%d@example.com", time.Now().UnixNano())
	out := run("signup", "--email", email, "--password", "hunter22-hunter22")
	requireContains(t, out, "Signed up as "+email)

	// platform create project demo
	out = run("create", "project", "demo")
	requireContains(t, out, "Created project demo")

	// platform deploy demo --image nginx:latest
	out = run("deploy", "demo", "--image", "nginx:latest", "--port", fmt.Sprintf("80:%d", hostPort))
	requireContains(t, out, "revision 1")

	// platform get deployments # shows running
	pollUntilContains(t, 30*time.Second, "platform get deployments", func() string {
		return run("get", "deployments", "demo")
	}, "running")

	// curl <worker-host>:<mapped-port> # serves nginx
	appURL := fmt.Sprintf("http://127.0.0.1:%d/", hostPort)
	pollUntilHTTPStatus(t, 15*time.Second, appURL, http.StatusOK)

	// platform logs demo
	pollUntilContains(t, 10*time.Second, "platform logs", func() string {
		return run("logs", "demo")
	}, "nginx")

	// platform delete demo # container stops and is removed
	out = run("delete", "demo")
	requireContains(t, out, "Deleted application demo")

	// Confirm the container was actually stopped and removed, not just
	// that the CLI printed success — the mapped port should stop
	// answering.
	pollUntilUnreachable(t, 15*time.Second, appURL)
}

// startPostgres spins up a real Postgres testcontainer with all migrations
// applied, returning a connection string for the platform_app role — the
// same role the apiserver process connects as in production — so RLS is
// actually enforced end-to-end (ADR-0010), exactly as it would be for a
// real deployment.
func startPostgres(t *testing.T, ctx context.Context) string {
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

	// The postgres image restarts once internally after initdb; tolerate
	// several failed pings before giving up (mirrors the Task 5/7
	// integration test's own startTestPostgres).
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

	migrationsDir := filepath.Join("..", "infrastructure", "postgres", "migrations")
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
	return fmt.Sprintf("postgres://platform_app:platform_app@%s/platform?sslmode=disable", endpoint)
}

// startService builds pkg into binDir and runs it as a real, separate OS
// process — never as a Go import, mirroring the ADR-0012 boundary the
// services themselves are held to — setting listenAddrEnvKey to a freshly
// allocated address plus any extraEnv, then polling readinessPath until
// the process accepts HTTP connections. Returns the process's base URL.
func startService(t *testing.T, ctx context.Context, goBin, binDir, name, pkg, listenAddrEnvKey string, extraEnv []string, readinessPath string) string {
	t.Helper()

	binPath := filepath.Join(binDir, name+exeSuffix())
	buildBinary(t, ctx, goBin, binPath, pkg)

	listenAddr := mustFreeAddr(t)
	// #nosec G204 -- binPath is a binary this same test just built into
	// t.TempDir(), not external input.
	cmd := exec.CommandContext(ctx, binPath)
	cmd.Env = append(append(os.Environ(), listenAddrEnvKey+"="+listenAddr), extraEnv...)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting %s process: %v", name, err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if t.Failed() {
			t.Logf("%s process output:\n%s", name, output.String())
		}
	})

	baseURL := "http://" + listenAddr
	deadline := time.Now().Add(20 * time.Second)
	client := &http.Client{Timeout: 2 * time.Second}
	for time.Now().Before(deadline) {
		if resp, err := client.Get(baseURL + readinessPath); err == nil {
			_ = resp.Body.Close()
			return baseURL
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("%s process at %s did not become ready in time", name, baseURL)
	return ""
}

func buildBinary(t *testing.T, ctx context.Context, goBin, outPath, pkg string) {
	t.Helper()
	// #nosec G204 -- goBin is resolved by goBinary() below, never from
	// request/environment-controlled input; this is test setup.
	cmd := exec.CommandContext(ctx, goBin, "build", "-o", outPath, pkg)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("building %s: %v\n%s", pkg, err, out)
	}
}

// newCLIRunner returns a function that runs the compiled CLI binary as a
// real subprocess against apiURL, with its local session state isolated to
// home (a fresh ~/.platform for this test run only) — mirroring exactly
// how a user's shell would invoke `platform`, never calling into the CLI's
// Go packages directly.
func newCLIRunner(t *testing.T, ctx context.Context, cliPath, home, apiURL string) func(args ...string) string {
	t.Helper()
	return func(args ...string) string {
		t.Helper()
		fullArgs := append([]string{"--api-url", apiURL}, args...)
		// #nosec G204 -- cliPath is the binary this test built into
		// t.TempDir(); args are this test's own hardcoded script steps, not
		// external input.
		cmd := exec.CommandContext(ctx, cliPath, fullArgs...)
		cmd.Env = append(os.Environ(), "HOME="+home, "USERPROFILE="+home)
		var output bytes.Buffer
		cmd.Stdout = &output
		cmd.Stderr = &output
		if err := cmd.Run(); err != nil {
			t.Fatalf("platform %s: %v\n%s", strings.Join(args, " "), err, output.String())
		}
		return output.String()
	}
}

func requireContains(t *testing.T, output, want string) {
	t.Helper()
	if !strings.Contains(output, want) {
		t.Fatalf("expected output to contain %q, got:\n%s", want, output)
	}
}

// pollUntilContains re-runs fn (typically a CLI invocation) until its
// output contains want or timeout elapses — used where a step's visible
// effect isn't guaranteed synchronous with the command that triggered it
// (e.g. nginx's own log lines aren't guaranteed flushed to the container's
// log driver the instant the container is reported running).
func pollUntilContains(t *testing.T, timeout time.Duration, label string, fn func() string, want string) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for {
		last = fn()
		if strings.Contains(last, want) {
			return last
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: expected output to contain %q within %s, got:\n%s", label, want, timeout, last)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func pollUntilHTTPStatus(t *testing.T, timeout time.Duration, url string, want int) {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == want {
				return
			}
			lastErr = fmt.Errorf("status %d", resp.StatusCode)
		} else {
			lastErr = err
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("GET %s: expected status %d within %s, last error: %v", url, want, timeout, lastErr)
}

// pollUntilUnreachable confirms the deployed container was actually
// stopped and removed — the exit-criteria script's own "container stops
// and is removed" clause — not just that the CLI printed a success
// message, by checking that the real port it was bound to stops
// answering.
func pollUntilUnreachable(t *testing.T, timeout time.Duration, url string) {
	t.Helper()
	client := &http.Client{Timeout: 1 * time.Second}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err != nil {
			return
		}
		_ = resp.Body.Close()
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("GET %s: still reachable after %s past delete, expected the container to be stopped and removed", url, timeout)
}

func mustFreePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocating a free port: %v", err)
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().(*net.TCPAddr).Port
}

func mustFreeAddr(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("127.0.0.1:%d", mustFreePort(t))
}

func exeSuffix() string {
	if goruntime.GOOS == "windows" {
		return ".exe"
	}
	return ""
}

// goBinary locates the go toolchain. Tries PATH first (the documented way,
// per go/runtime's GOROOT() deprecation notice), then GOROOT if set, then
// this environment's known install location — mirrors the Task 5/7
// integration test's own goBinary helper, duplicated here rather than
// shared since it's a handful of lines of test-only infra, not worth a
// cross-package dependency for.
func goBinary(t *testing.T) string {
	t.Helper()
	if p, err := exec.LookPath("go"); err == nil {
		return p
	}

	name := "go"
	if goruntime.GOOS == "windows" {
		name = "go.exe"
	}
	if goroot := os.Getenv("GOROOT"); goroot != "" {
		if candidate := filepath.Join(goroot, "bin", name); fileExists(candidate) {
			return candidate
		}
	}
	for _, candidate := range []string{`C:\Program Files\Go\bin\go.exe`, "/usr/local/go/bin/go"} {
		if fileExists(candidate) {
			return candidate
		}
	}
	t.Fatalf("go toolchain not found on PATH, GOROOT, or a known install location")
	return ""
}

func fileExists(path string) bool {
	// #nosec G703 -- path is always $GOROOT/bin/go(.exe) or one of this
	// function's own hardcoded candidates, never external input.
	_, err := os.Stat(path)
	return err == nil
}

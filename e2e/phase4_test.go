//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestE2E_Phase4ExitCriteria automates the exit-criteria script at the top
// of phase-4-service-discovery-lb.md — this test running green is what
// actually certifies Phase 4 done, exactly as TestE2E_Phase1ExitCriteria
// through TestE2E_Phase3ExitCriteria did for their phases
// (phase-4-service-discovery-lb.md Task 7):
//
//	scale an app to 3 replicas, send repeated requests through the LB,
//	observe distribution across all 3
//	stop one replica, confirm it's removed from rotation within the
//	health-check interval
//
// Reuses phase1_test.go's/phase2_test.go's/phase3_test.go's process/testcontainer
// helpers (same package: startPostgres, startNATS, startScheduler,
// startWorker, startService, startControllerManager, buildBinary,
// newCLIRunner, pollUntilContains, dockerContainerIDs, removeNewContainers,
// dockerKill, ...) plus loadbalancer started here for the first time as a
// real OS process alongside apiserver/scheduler/worker/controller-manager/
// platform — the only new one Task 7 calls for, and it needs no bespoke
// startLoadBalancer helper since it's structurally identical to
// apiserver/worker's own HTTP-surface-plus-readiness-poll shape, so
// startService covers it directly.
func TestE2E_Phase4ExitCriteria(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	goBin := goBinary(t)
	binDir := t.TempDir()

	// Same leak-avoidance approach as Phase 2/3's exit-criteria tests: no
	// deterministic container name to filter cleanup by, so snapshot
	// ancestor-image container IDs before/after and diff. removeNewContainers
	// re-lists containers fresh at t.Cleanup time (not from a stale
	// snapshot), so it also catches any replacement container the
	// Deployment controller schedules after this test's own docker-kill
	// step below — the exact leak this phase's own Task 5 integration test
	// hit and fixed.
	containersBefore := dockerContainerIDs(t, "traefik/whoami")
	t.Cleanup(func() { removeNewContainers(t, "traefik/whoami", containersBefore) })

	adminDBURL, dbURL := startPostgres(t, ctx)
	natsURL := startNATS(t, ctx)

	startScheduler(t, ctx, goBin, binDir, dbURL, natsURL)

	workerBinPath := filepath.Join(binDir, "worker"+exeSuffix())
	buildBinary(t, ctx, goBin, workerBinPath, "platform/services/worker")
	worker := startWorker(t, ctx, workerBinPath, "e2e-worker-1", natsURL)

	// Sped-up reconcile ticks (Task 3/4's placeholder defaults are 5s) so
	// this test doesn't have to wait out production tuning: fast enough for
	// 3 replicas to provision quickly (deployment reconcile) and for
	// service_instances to stay in step with them (service reconcile).
	startControllerManager(t, ctx, goBin, binDir, adminDBURL, natsURL,
		"CONTROLLER_RECONCILE_INTERVAL=1s",
		"CONTROLLER_SERVICE_RECONCILE_INTERVAL=1s",
	)

	// Sped-up active health-check interval (Task 5's placeholder default is
	// 5s) — this is the exit criteria's own "within the health-check
	// interval" clock.
	const healthCheckInterval = 1 * time.Second
	lbURL := startService(t, ctx, goBin, binDir, "loadbalancer", "platform/services/loadbalancer",
		"LOADBALANCER_LISTEN_ADDR", []string{
			"APP_DATABASE_URL=" + dbURL,
			"LOADBALANCER_NATS_URL=" + natsURL,
			"LOADBALANCER_RESYNC_INTERVAL=1s",
			"LOADBALANCER_HEALTH_CHECK_INTERVAL=" + healthCheckInterval.String(),
		}, "/")

	apiURL := startService(t, ctx, goBin, binDir, "apiserver", "platform/services/apiserver",
		"APISERVER_LISTEN_ADDR", []string{
			"APP_DATABASE_URL=" + dbURL,
			// Task 6's event-driven deploy path never calls this; apiserver
			// just needs a well-formed value to start.
			"WORKER_ADDR=" + worker.baseURL,
			"APISERVER_NATS_URL=" + natsURL,
			"JWT_SIGNING_KEY=e2e-test-signing-key",
		}, "/")

	cliPath := filepath.Join(binDir, "platform"+exeSuffix())
	buildBinary(t, ctx, goBin, cliPath, "platform/apps/cli")
	run := newCLIRunner(t, ctx, cliPath, t.TempDir(), apiURL)

	email := fmt.Sprintf("e2e-phase4-%d@example.com", time.Now().UnixNano())
	out := run("signup", "--email", email, "--password", "hunter22-hunter22")
	requireContains(t, out, "Signed up as "+email)

	out = run("create", "project", "demo")
	requireContains(t, out, "Created project demo")

	// platform deploy demo --image traefik/whoami --port 80. traefik/whoami
	// is the same test image Task 4/5's own integration tests use: its
	// response body names the exact container that served a request, the
	// only way to observe from outside which of 3 identical backends the
	// load balancer picked. --port 80 with no host part gets an ephemeral
	// host port (Task 2), which is what the Service Instance controller
	// (Task 3) needs to populate service_instances.port.
	out = run("deploy", "demo", "--image", "traefik/whoami", "--port", "80")
	requireContains(t, out, "revision 1")

	// scale an app to 3 replicas
	out = run("scale", "demo", "--replicas", "3")
	requireContains(t, out, "scaled to 3 replicas")

	pollUntilContains(t, 30*time.Second, "platform get deployments", func() string {
		return run("get", "deployments", "demo")
	}, "3/3")

	// The exit-criteria demo needs somewhere to actually send requests
	// (Task 6): the SERVICE column now carries the dns_name to route on,
	// populated once the Service Instance controller lazily creates it on
	// this application's first healthy instance.
	dnsName := pollUntilServiceDNSName(t, 15*time.Second, run)

	// send repeated requests through the LB, observe distribution across all 3
	client := &http.Client{Timeout: 5 * time.Second}
	seen := make(map[string]bool)
	deadline := time.Now().Add(60 * time.Second)
	var lastErr error
	for len(seen) < 3 && time.Now().Before(deadline) {
		hostname, err := requestBackendHostname(client, lbURL, dnsName)
		if err != nil {
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		}
		seen[hostname] = true
	}
	if len(seen) != 3 {
		t.Fatalf("expected requests through the load balancer to hit 3 distinct backends, saw %d: %v (last error: %v)", len(seen), seen, lastErr)
	}

	// stop one replica, confirm it's removed from rotation within the
	// health-check interval — docker kill directly (not scale-down),
	// matching Task 5's own acceptance criteria ("stops a replica's
	// container process directly ... confirms the load balancer stops
	// routing to it within one health-check interval"). traefik/whoami's
	// reported "Hostname:" is Docker's own default container hostname,
	// which is exactly the short container ID docker kill accepts.
	var target string
	for hostname := range seen {
		target = hostname
		break
	}
	dockerKill(t, ctx, target)

	// Poll requests through the LB for a window several health-check
	// intervals wide, recording how long ago each backend was last seen —
	// mirrors services/loadbalancer/main_integration_test.go's own
	// TestLoadBalancer_EjectsDeadBackendViaActiveHealthCheck (Task 5),
	// duplicated here rather than imported: e2e drives only the real
	// external contract (ADR-0012's boundary applied to this test itself),
	// and internal/proxy isn't importable from outside services/loadbalancer
	// anyway.
	windowDuration := 8 * healthCheckInterval
	cutover := 3 * healthCheckInterval
	lastSeen := make(map[string]time.Duration)
	windowStart := time.Now()
	for time.Since(windowStart) < windowDuration {
		hostname, err := requestBackendHostname(client, lbURL, dnsName)
		if err == nil {
			lastSeen[hostname] = time.Since(windowStart)
		}
		time.Sleep(200 * time.Millisecond)
	}

	if elapsed, ok := lastSeen[target]; ok && windowDuration-elapsed < cutover {
		t.Fatalf("killed backend %s was still being routed to near the end of the %s window (last seen at %s): %v", target, windowDuration, elapsed, lastSeen)
	}
	survived := false
	for hostname, elapsed := range lastSeen {
		if hostname != target && windowDuration-elapsed < cutover {
			survived = true
		}
	}
	if !survived {
		t.Fatalf("expected at least one surviving backend to still be routed to near the end of the window, got: %v", lastSeen)
	}
}

// pollUntilServiceDNSName re-runs `platform get deployments` until the
// SERVICE column is populated (Task 3's controller lazily creates the
// service on the application's first healthy instance) or timeout elapses.
func pollUntilServiceDNSName(t *testing.T, timeout time.Duration, run func(args ...string) string) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		out := run("get", "deployments", "demo")
		if dnsName := serviceDNSNameColumn(t, out); dnsName != "-" {
			return dnsName
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected `platform get deployments` to show a service dns_name within %s, got:\n%s", timeout, out)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// serviceDNSNameColumn extracts the SERVICE column from `platform get
// deployments`'s single-revision row ("REVISION STATUS IMAGE REPLICAS NODES
// SERVICE CREATED_AT" — get_deployments.go, phase-4-service-discovery-lb.md
// Task 6).
func serviceDNSNameColumn(t *testing.T, out string) string {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) < 2 {
		t.Fatalf("expected at least one deployment row in output:\n%s", out)
	}
	fields := strings.Fields(lines[len(lines)-1])
	if len(fields) < 6 {
		t.Fatalf("malformed `get deployments` row %q in output:\n%s", lines[len(lines)-1], out)
	}
	return fields[5]
}

// platformServiceHeader mirrors services/loadbalancer/internal/proxy's own
// ServiceHeader constant — duplicated rather than imported, since
// internal/proxy isn't importable from outside services/loadbalancer and
// this test is held to the same "only the published external contract"
// boundary the services themselves are (ADR-0012).
const platformServiceHeader = "X-Platform-Service"

// requestBackendHostname sends one request through the load balancer at
// lbURL, targeting dnsName via the routing header, and returns
// traefik/whoami's "Hostname: <id>" line from the response body — the only
// way to observe, from outside, which of the identical backends actually
// served a given request. Mirrors
// services/loadbalancer/main_integration_test.go's own helper of the same
// name (duplicated, not imported, for the same reason as
// platformServiceHeader above).
func requestBackendHostname(client *http.Client, lbURL, dnsName string) (string, error) {
	req, err := http.NewRequest(http.MethodGet, lbURL+"/", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set(platformServiceHeader, dnsName)
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status %d: %s", resp.StatusCode, body)
	}
	for _, line := range strings.Split(string(body), "\n") {
		if hostname, ok := strings.CutPrefix(line, "Hostname: "); ok {
			return strings.TrimSpace(hostname), nil
		}
	}
	return "", fmt.Errorf("no Hostname line in response body: %s", body)
}

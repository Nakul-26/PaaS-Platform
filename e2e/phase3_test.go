//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// TestE2E_Phase3ExitCriteria automates the exit-criteria script at the top
// of phase-3-controllers.md — this test running green is what actually
// certifies Phase 3 done, exactly as TestE2E_Phase1ExitCriteria and
// TestE2E_Phase2ExitCriteria did for their phases (phase-3-controllers.md
// Task 8):
//
//	manually kill a container process (docker kill, not the worker process)
//	# the controller notices the deployment now has fewer running replicas than desired
//	# the controller schedules a replacement without any user action
//	platform get deployments demo   # within one reconcile interval, shows the replacement running
//
// Reuses phase1_test.go's/phase2_test.go's process/testcontainer helpers
// (same package: startPostgres, startNATS, startScheduler, startWorker,
// startService, buildBinary, newCLIRunner, pollUntilContains,
// dockerContainerIDs, removeNewContainers, ...) plus a new
// startControllerManager here, since this is the first phase that needs
// controller-manager running as a real OS process alongside
// apiserver/scheduler/worker/platform.
func TestE2E_Phase3ExitCriteria(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	goBin := goBinary(t)
	binDir := t.TempDir()

	// Same leak-avoidance approach as Phase 2's exit-criteria test: no
	// deterministic container name to filter cleanup by, so snapshot
	// ancestor-image container IDs before/after and diff.
	containersBefore := dockerContainerIDs(t, "nginx:latest")
	t.Cleanup(func() { removeNewContainers(t, "nginx:latest", containersBefore) })

	adminDBURL, dbURL := startPostgres(t, ctx)
	natsURL := startNATS(t, ctx)

	startScheduler(t, ctx, goBin, binDir, dbURL, natsURL)

	workerBinPath := filepath.Join(binDir, "worker"+exeSuffix())
	buildBinary(t, ctx, goBin, workerBinPath, "platform/services/worker")
	// Sped-up health-check interval (Task 1's placeholder default is 3s) so
	// this test doesn't have to wait out production tuning to observe a
	// crash get noticed.
	worker := startWorker(t, ctx, workerBinPath, "e2e-worker-1", natsURL,
		"WORKER_HEARTBEAT_INTERVAL=1s", "WORKER_HEALTH_CHECK_INTERVAL=1s")

	// Sped-up reconcile tick (Task 4's placeholder default is 5s), same
	// reasoning.
	startControllerManager(t, ctx, goBin, binDir, adminDBURL, natsURL, "CONTROLLER_RECONCILE_INTERVAL=1s")

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

	email := fmt.Sprintf("e2e-phase3-%d@example.com", time.Now().UnixNano())
	out := run("signup", "--email", email, "--password", "hunter22-hunter22")
	requireContains(t, out, "Signed up as "+email)

	out = run("create", "project", "demo")
	requireContains(t, out, "Created project demo")

	// platform deploy demo --image nginx:latest (replicas_desired defaults
	// to 1 — migration 0004 — deploy takes no --replicas flag, only `scale`
	// does, Task 5)
	out = run("deploy", "demo", "--image", "nginx:latest")
	requireContains(t, out, "revision 1")

	// Steady state reached: REPLICAS shows 1/1.
	pollUntilContains(t, 30*time.Second, "platform get deployments", func() string {
		return run("get", "deployments", "demo")
	}, "1/1")

	// Identify the actual container docker just created for this
	// deployment, the same ancestor-image-diff technique Phase 2's own
	// exit-criteria test uses for its cleanup bookkeeping — there's no CLI
	// surface that reports a raw docker container ID (NODES only ever
	// shows our own node UUID), so this is the only way to name the one
	// process to kill.
	containersAfterDeploy := dockerContainerIDs(t, "nginx:latest")
	originalID := onlyNewContainer(t, containersBefore, containersAfterDeploy)

	// manually kill a container process (docker kill, not the worker
	// process)
	dockerKill(t, ctx, originalID)

	// # the controller notices the deployment now has fewer running
	// replicas than desired — Task 1's health-check poll reports the crash
	// via node.<id>.status, which drops REPLICAS to 0/1.
	pollUntilContains(t, 15*time.Second, "platform get deployments", func() string {
		return run("get", "deployments", "demo")
	}, "0/1")

	// # the controller schedules a replacement without any user action
	// platform get deployments demo   # within one reconcile interval,
	// shows the replacement running
	pollUntilContains(t, 30*time.Second, "platform get deployments", func() string {
		return run("get", "deployments", "demo")
	}, "1/1")
}

// startControllerManager builds and runs the real controller-manager binary
// as a separate OS process. Like startScheduler, it has no HTTP surface to
// poll for readiness — it's entirely Postgres/NATS-driven — so this just
// starts it and lets the deploy step further down give it time to catch
// up. adminDBURL is the Postgres superuser connection string
// (startPostgres's first return value): controller-manager reads/writes
// across every tenant each tick, the same documented RLS-bypass exception
// db.ReconcileRepository's own doc comment explains.
func startControllerManager(t *testing.T, ctx context.Context, goBin, binDir, adminDBURL, natsURL string, extraEnv ...string) {
	t.Helper()

	binPath := filepath.Join(binDir, "controller-manager"+exeSuffix())
	buildBinary(t, ctx, goBin, binPath, "platform/services/controller-manager")

	// #nosec G204 -- binPath is a binary this same test just built into
	// t.TempDir(), not external input.
	cmd := exec.CommandContext(ctx, binPath)
	cmd.Env = append(append(os.Environ(), "DATABASE_URL="+adminDBURL, "CONTROLLER_NATS_URL="+natsURL), extraEnv...)
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

// onlyNewContainer returns the single container ID present in after but
// not before — used right after a deploy step, where exactly one new
// ancestor-image container is expected (this test only ever deploys once,
// at the default of 1 replica).
func onlyNewContainer(t *testing.T, before, after map[string]bool) string {
	t.Helper()
	var found []string
	for id := range after {
		if !before[id] {
			found = append(found, id)
		}
	}
	if len(found) != 1 {
		t.Fatalf("expected exactly one new container after deploy, found %d: %v", len(found), found)
	}
	return found[0]
}

// dockerKill kills containerID directly against the real docker daemon,
// bypassing the worker entirely — mirrors
// services/controller-manager/main_integration_test.go's own killContainer
// (duplicated rather than imported, ADR-0012: no service/test package
// depends on another's internal tree).
func dockerKill(t *testing.T, ctx context.Context, containerID string) {
	t.Helper()
	// #nosec G204 -- containerID comes from this test's own docker-ps-based
	// diff above, never external input.
	if out, err := exec.CommandContext(ctx, "docker", "kill", containerID).CombinedOutput(); err != nil {
		t.Fatalf("docker kill %s: %v\n%s", containerID, err, out)
	}
}

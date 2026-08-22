//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestE2E_Phase2ExitCriteria automates the exit-criteria script at the top
// of phase-2-multi-node.md — this test running green is what actually
// certifies Phase 2 done, exactly as TestE2E_Phase1ExitCriteria did for
// Phase 1 (phase-1-mvp.md Task 8):
//
//	platform deploy demo --image nginx:latest
//	# scheduler places the deployment on one of 3 running worker processes
//	platform get nodes                        # shows 3 healthy nodes, one carrying the deployment
//	docker-compose stop worker-2 (or kill -9)  # simulate node failure
//	platform get nodes                         # the killed node drops out / shows unreachable
//
// This reuses phase1_test.go's process/testcontainer helpers (same package:
// startPostgres, startNATS, startScheduler, startService, buildBinary,
// newCLIRunner, pollUntilContains, ...) — same "real binaries as real OS
// processes, never an internal Go import" discipline ADR-0012 holds the
// services themselves to. The 3 worker nodes run as bare OS processes here
// (not docker-compose containers, Task 8's setup) so this test doesn't need
// a nested compose invocation — same binary, same NATS contract, same
// scheduler on the other end either way.
func TestE2E_Phase2ExitCriteria(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	goBin := goBinary(t)
	binDir := t.TempDir()

	// Unlike Phase 1's exit-criteria script, this one has no delete step —
	// the nginx container it deploys would otherwise leak on every run.
	// Neither the scheduler's assignMessage nor the worker's ContainerSpec
	// thread a deterministic name through the placement pipeline yet (a
	// separate, out-of-scope gap), so there's no name to filter cleanup by;
	// snapshotting container IDs before/after and diffing is what identifies
	// exactly the one this test created.
	containersBefore := dockerContainerIDs(t, "nginx:latest")
	t.Cleanup(func() { removeNewContainers(t, "nginx:latest", containersBefore) })

	_, dbURL := startPostgres(t, ctx)
	natsURL := startNATS(t, ctx)

	// Speed up the liveness sweep the final assertion waits on — Task 5's
	// production defaults (15s heartbeat timeout / 5s sweep interval) would
	// still pass, just slowly; both are configurable precisely so a test
	// like this one isn't stuck paying that cost.
	startScheduler(t, ctx, goBin, binDir, dbURL, natsURL,
		"SCHEDULER_HEARTBEAT_TIMEOUT=3s", "SCHEDULER_LIVENESS_SWEEP_INTERVAL=1s")

	workerBinPath := filepath.Join(binDir, "worker"+exeSuffix())
	buildBinary(t, ctx, goBin, workerBinPath, "platform/services/worker")

	// 3 logical worker-node processes sharing this one binary and this
	// host's real Docker daemon (ADR-0009) — the same shape
	// infrastructure/docker-compose.yml's worker-1/2/3 services (Task 8)
	// give it, just as plain OS processes instead of containers.
	workers := make([]*workerProcess, 3)
	for i := range workers {
		workers[i] = startWorker(t, ctx, workerBinPath, fmt.Sprintf("e2e-worker-%d", i+1), natsURL,
			"WORKER_HEARTBEAT_INTERVAL=1s")
	}

	apiURL := startService(t, ctx, goBin, binDir, "apiserver", "platform/services/apiserver",
		"APISERVER_LISTEN_ADDR", []string{
			"APP_DATABASE_URL=" + dbURL,
			// Task 6's event-driven deploy path never calls this; apiserver
			// just needs a well-formed value to start.
			"WORKER_ADDR=" + workers[0].baseURL,
			"APISERVER_NATS_URL=" + natsURL,
			"JWT_SIGNING_KEY=e2e-test-signing-key",
		}, "/")

	cliPath := filepath.Join(binDir, "platform"+exeSuffix())
	buildBinary(t, ctx, goBin, cliPath, "platform/apps/cli")
	run := newCLIRunner(t, ctx, cliPath, t.TempDir(), apiURL)

	email := fmt.Sprintf("e2e-phase2-%d@example.com", time.Now().UnixNano())
	out := run("signup", "--email", email, "--password", "hunter22-hunter22")
	requireContains(t, out, "Signed up as "+email)

	out = run("create", "project", "demo")
	requireContains(t, out, "Created project demo")

	// platform deploy demo --image nginx:latest
	out = run("deploy", "demo", "--image", "nginx:latest")
	requireContains(t, out, "revision 1")

	// scheduler places the deployment on one of 3 running worker processes
	deploymentsOut := pollUntilContains(t, 30*time.Second, "platform get deployments", func() string {
		return run("get", "deployments", "demo")
	}, "running")
	placedNodeID := deploymentNodeID(t, deploymentsOut)

	// platform get nodes # shows 3 healthy nodes, one carrying the deployment
	nodesOut := run("get", "nodes")
	rows := nodeTableRows(t, nodesOut)
	if len(rows) != 3 {
		t.Fatalf("expected 3 nodes in `get nodes` output, got %d:\n%s", len(rows), nodesOut)
	}
	var placedHostname string
	for _, row := range rows {
		if row.status != "healthy" {
			t.Fatalf("expected all 3 nodes healthy before killing any, got:\n%s", nodesOut)
		}
		if row.id == placedNodeID {
			placedHostname = row.hostname
		}
	}
	if placedHostname == "" {
		t.Fatalf("placed node id %s (from `get deployments`) not found in `get nodes` output:\n%s", placedNodeID, nodesOut)
	}

	var killed *workerProcess
	for _, w := range workers {
		if w.hostname == placedHostname {
			killed = w
		}
	}
	if killed == nil {
		t.Fatalf("no started worker process matches placed hostname %q", placedHostname)
	}

	// docker-compose stop worker-2 (or kill -9) # simulate node failure
	killed.kill(t)

	// platform get nodes # the killed node drops out / shows unreachable
	pollUntilNodeStatus(t, 15*time.Second, run, placedNodeID, "unreachable")

	// The other two nodes were never touched — confirm the sweep is
	// selective, not a blanket "something died" fallback.
	finalOut := run("get", "nodes")
	for _, row := range nodeTableRows(t, finalOut) {
		if row.id == placedNodeID {
			continue
		}
		if row.status != "healthy" {
			t.Fatalf("expected untouched node %s to remain healthy, got:\n%s", row.id, finalOut)
		}
	}
}

// workerProcess is one logical worker node started as a real OS process
// (mirrors startService's process handling, but keeps the *exec.Cmd handle
// so the test can hard-kill a specific one mid-run instead of only at
// t.Cleanup).
type workerProcess struct {
	hostname string
	baseURL  string
	cmd      *exec.Cmd
}

// startWorker runs the already-built worker binary at binPath as a fresh OS
// process configured as one logical node (ADR-0009).
func startWorker(t *testing.T, ctx context.Context, binPath, hostname, natsURL string, extraEnv ...string) *workerProcess {
	t.Helper()

	listenAddr := mustFreeAddr(t)
	env := append([]string{
		"WORKER_LISTEN_ADDR=" + listenAddr,
		"WORKER_NATS_URL=" + natsURL,
		"WORKER_NODE_ID_FILE=" + filepath.Join(t.TempDir(), "worker-node-id"),
		"WORKER_HOSTNAME=" + hostname,
	}, extraEnv...)

	// #nosec G204 -- binPath is a binary this same test just built into
	// t.TempDir(), not external input.
	cmd := exec.CommandContext(ctx, binPath)
	cmd.Env = append(os.Environ(), env...)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting worker %s process: %v", hostname, err)
	}
	w := &workerProcess{hostname: hostname, baseURL: "http://" + listenAddr, cmd: cmd}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if t.Failed() {
			t.Logf("worker %s process output:\n%s", hostname, output.String())
		}
	})

	deadline := time.Now().Add(20 * time.Second)
	client := &http.Client{Timeout: 2 * time.Second}
	for time.Now().Before(deadline) {
		if resp, err := client.Get(w.baseURL + "/v1/containers/readiness-probe"); err == nil {
			_ = resp.Body.Close()
			return w
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("worker %s at %s did not become ready in time", hostname, w.baseURL)
	return nil
}

// kill simulates the exit-criteria script's "docker-compose stop worker-2
// (or kill -9)" step for this specific worker.
func (w *workerProcess) kill(t *testing.T) {
	t.Helper()
	if err := w.cmd.Process.Kill(); err != nil {
		t.Fatalf("killing worker %s: %v", w.hostname, err)
	}
	_ = w.cmd.Wait()
}

// nodeRow is one parsed row of `platform get nodes`'s
// "ID\tHOSTNAME\tSTATUS\tLAST_HEARTBEAT_AT" table.
type nodeRow struct {
	id       string
	hostname string
	status   string
}

func nodeTableRows(t *testing.T, out string) []nodeRow {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) < 2 {
		return nil
	}
	rows := make([]nodeRow, 0, len(lines)-1)
	for _, line := range lines[1:] {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			t.Fatalf("malformed `get nodes` row %q in output:\n%s", line, out)
		}
		rows = append(rows, nodeRow{id: fields[0], hostname: fields[1], status: fields[2]})
	}
	return rows
}

// deploymentNodeID extracts the NODES column from `platform get
// deployments`'s single-revision row (this test only ever deploys once, at
// the default of 1 replica, so NODES is always exactly one id — Task 7,
// phase-3-controllers.md, replaced the old singular NODE/CONTAINER_STATUS
// columns with REPLICAS/NODES to support more than one container per
// deployment).
func deploymentNodeID(t *testing.T, out string) string {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) < 2 {
		t.Fatalf("expected at least one deployment row in output:\n%s", out)
	}
	// REVISION STATUS IMAGE REPLICAS NODES CREATED_AT
	fields := strings.Fields(lines[len(lines)-1])
	if len(fields) < 5 {
		t.Fatalf("malformed `get deployments` row %q in output:\n%s", lines[len(lines)-1], out)
	}
	nodeID := fields[4]
	if nodeID == "-" {
		t.Fatalf("deployment has no node_id yet (placement hasn't landed) in output:\n%s", out)
	}
	return nodeID
}

// pollUntilNodeStatus re-runs `platform get nodes` until nodeID's row
// reports wantStatus or timeout elapses.
func pollUntilNodeStatus(t *testing.T, timeout time.Duration, run func(args ...string) string, nodeID, wantStatus string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for {
		last = run("get", "nodes")
		for _, row := range nodeTableRows(t, last) {
			if row.id == nodeID && row.status == wantStatus {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("platform get nodes: expected node %s to report status %q within %s, got:\n%s", nodeID, wantStatus, timeout, last)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// dockerContainerIDs shells out to the docker CLI (not the SDK — ADR-0011
// reserves direct Docker client imports for internal/runtime alone) to list
// every container, running or stopped, created from ancestor. Used only for
// this test's own before/after cleanup bookkeeping, never to drive the
// system under test.
func dockerContainerIDs(t *testing.T, ancestor string) map[string]bool {
	t.Helper()
	// #nosec G204 -- ancestor is always this file's own "nginx:latest"
	// constant, never external input.
	out, err := exec.Command("docker", "ps", "-aq", "--filter", "ancestor="+ancestor).Output()
	if err != nil {
		t.Logf("listing %s containers for cleanup bookkeeping: %v", ancestor, err)
		return nil
	}
	ids := make(map[string]bool)
	for _, id := range strings.Fields(string(out)) {
		ids[id] = true
	}
	return ids
}

// removeNewContainers force-removes any ancestor-image container that
// exists now but wasn't present in before — this test's own deploy step is
// the only thing that could have created one since. Unlike Phase 1's
// exit-criteria script, phase-2-multi-node.md's has no delete step, so
// without this every run of this test would leak a running nginx container.
func removeNewContainers(t *testing.T, ancestor string, before map[string]bool) {
	t.Helper()
	for id := range dockerContainerIDs(t, ancestor) {
		if before[id] {
			continue
		}
		// #nosec G204 -- id comes from this test's own `docker ps` listing
		// above, never external input.
		if out, err := exec.Command("docker", "rm", "-f", id).CombinedOutput(); err != nil {
			t.Logf("removing leftover container %s: %v\n%s", id, err, out)
		}
	}
}

//go:build integration

// Package main's integration tests cover Tasks 3-4's acceptance
// (phase-2-multi-node.md): a real worker process, run as a separate OS
// process against a real NATS instance, publishes a registration message on
// startup and keeps publishing heartbeats on schedule (Task 3); and, given a
// node.<id>.assign message published directly (no scheduler involved yet),
// actually starts the container against a real Docker daemon and publishes a
// node.<id>.status result (Task 4).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/nats"

	"platform/internal/eventbus"
	"platform/internal/runtime"
)

func TestWorker_RegistersAndHeartbeatsOverNATS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	if _, err := runtime.NewDockerRuntime(); err != nil {
		t.Skipf("docker daemon not available, skipping: %v", err)
	}

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

	heartbeats := make(chan map[string]any, 16)
	hbSub, err := bus.Subscribe("node.*.heartbeat", func(msg eventbus.Message) {
		var payload map[string]any
		if err := json.Unmarshal(msg.Data, &payload); err == nil {
			heartbeats <- payload
		}
	})
	if err != nil {
		t.Fatalf("subscribing to node.*.heartbeat: %v", err)
	}
	defer func() { _ = hbSub.Unsubscribe() }()

	startTestWorker(t, ctx, natsURL)

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
	if registration["hostname"] == "" || registration["hostname"] == nil {
		t.Fatalf("registration message missing hostname: %+v", registration)
	}
	if cpu, _ := registration["cpu_capacity_millicores"].(float64); cpu != 1500 {
		t.Fatalf("expected cpu_capacity_millicores 1500 (from WORKER_CPU_CAPACITY_MILLICORES), got %+v", registration)
	}

	seen := 0
	deadline := time.After(15 * time.Second)
	for seen < 2 {
		select {
		case hb := <-heartbeats:
			if hb["node_id"] != nodeID {
				t.Fatalf("heartbeat node_id %v does not match registration node_id %s", hb["node_id"], nodeID)
			}
			if hb["timestamp"] == "" || hb["timestamp"] == nil {
				t.Fatalf("heartbeat missing timestamp: %+v", hb)
			}
			seen++
		case <-deadline:
			t.Fatalf("timed out waiting for heartbeats, only saw %d", seen)
		}
	}
}

// testAssignMessage mirrors nodeagent's own (unexported) assignMessage —
// docs/nats-contract.md's node.<id>.assign schema — so this test can publish
// one without importing the worker's internal package (ADR-0012: only the
// published NATS contract, not the package tree).
type testAssignMessage struct {
	AssignmentID string            `json:"assignment_id"`
	DeploymentID string            `json:"deployment_id"`
	Image        string            `json:"image"`
	Env          map[string]string `json:"env,omitempty"`
	Ports        []testPortBinding `json:"ports,omitempty"`
	Command      []string          `json:"command,omitempty"`
}

type testPortBinding struct {
	ContainerPort int    `json:"container_port"`
	HostPort      int    `json:"host_port,omitempty"`
	Protocol      string `json:"protocol,omitempty"`
}

func TestWorker_ProcessesAssignmentOverNATS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	rt, err := runtime.NewDockerRuntime()
	if err != nil {
		t.Skipf("docker daemon not available, skipping: %v", err)
	}
	defer func() { _ = rt.Close() }()

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

	startTestWorker(t, ctx, natsURL)

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

	// The worker sets up its own durable NODE_ASSIGNMENTS/NODE_STATUS
	// streams on startup (subscribeAssignments); this test's EnsureStream
	// calls are idempotent no-ops layered on top, exercising the same path
	// the scheduler (Task 5) will use to publish for real.
	if err := bus.EnsureStream(ctx, eventbus.StreamConfig{
		Name:     eventbus.NodeAssignmentsStream,
		Subjects: []string{eventbus.NodeAssignmentsStreamFilter},
	}); err != nil {
		t.Fatalf("ensuring %s stream: %v", eventbus.NodeAssignmentsStream, err)
	}
	// The worker also calls EnsureStream for NODE_STATUS on startup
	// (subscribeAssignments, before it publishes any status), but this
	// test's own status subscription can race ahead of that — EnsureStream
	// is idempotent, so calling it here too just closes the race.
	if err := bus.EnsureStream(ctx, eventbus.StreamConfig{
		Name:     eventbus.NodeStatusStream,
		Subjects: []string{eventbus.NodeStatusStreamFilter},
	}); err != nil {
		t.Fatalf("ensuring %s stream: %v", eventbus.NodeStatusStream, err)
	}

	statuses := make(chan map[string]any, 4)
	statusSub, err := bus.SubscribeDurable(ctx, eventbus.NodeStatusStream, "test-status-watcher", eventbus.NodeStatusStreamFilter, func(msg eventbus.Message) error {
		var payload map[string]any
		if err := json.Unmarshal(msg.Data, &payload); err == nil {
			statuses <- payload
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribing to %s: %v", eventbus.NodeStatusStreamFilter, err)
	}
	defer func() { _ = statusSub.Unsubscribe() }()

	const assignmentID = "test-assignment-1"
	assignData, err := json.Marshal(testAssignMessage{
		AssignmentID: assignmentID,
		DeploymentID: "test-deployment-1",
		Image:        "nginx:latest",
		Ports:        []testPortBinding{{ContainerPort: 80}},
	})
	if err != nil {
		t.Fatalf("marshaling assignment message: %v", err)
	}
	if err := bus.PublishDurable(ctx, fmt.Sprintf("node.%s.assign", nodeID), assignData); err != nil {
		t.Fatalf("publishing assignment: %v", err)
	}

	var status map[string]any
	select {
	case status = <-statuses:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for node.<id>.status message")
	}

	if status["assignment_id"] != assignmentID {
		t.Fatalf("expected assignment_id %s, got %+v", assignmentID, status)
	}
	if status["status"] != "running" {
		t.Fatalf("expected status \"running\", got %+v", status)
	}
	containerID, _ := status["container_id"].(string)
	if containerID == "" {
		t.Fatalf("status message missing container_id: %+v", status)
	}
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

// startTestWorker builds and runs the real worker binary as a separate OS
// process (ADR-0012), pointed at natsURL with a fast heartbeat interval so
// the test doesn't wait on the 5s production default.
func startTestWorker(t *testing.T, ctx context.Context, natsURL string) {
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
}

// goBinary locates the go toolchain. Tries PATH first, then GOROOT, then a
// known install location — mirrors services/apiserver/internal/server's copy
// of the same helper (this shell's PATH doesn't reliably include Go's bin
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

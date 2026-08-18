//go:build integration

// Package main's integration test covers Task 3's acceptance
// (phase-2-multi-node.md): a real worker process, run as a separate OS
// process against a real NATS instance, publishes a registration message on
// startup and keeps publishing heartbeats on schedule.
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

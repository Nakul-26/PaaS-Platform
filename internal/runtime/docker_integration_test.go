//go:build integration

package runtime

import (
	"bufio"
	"context"
	"testing"
	"time"
)

// TestDockerRuntime_Lifecycle exercises the full container lifecycle against
// a real Docker daemon (ADR-0006 Task 3 acceptance: pull, start, confirm
// running, stream a log line, stop, remove). Run with:
//
//	go test -tags=integration ./internal/runtime/...
func TestDockerRuntime_Lifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	rt, err := NewDockerRuntime()
	if err != nil {
		t.Fatalf("connecting to docker daemon: %v", err)
	}
	defer func() { _ = rt.Close() }()

	const image = "nginx:latest"
	if err := rt.PullImage(ctx, image); err != nil {
		t.Fatalf("PullImage: %v", err)
	}

	containerID, err := rt.CreateContainer(ctx, ContainerSpec{
		Image: image,
		Name:  "platform-runtime-test-nginx",
		Ports: []PortBinding{{ContainerPort: 80}},
	})
	if err != nil {
		t.Fatalf("CreateContainer: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.StopContainer(context.Background(), containerID, 5*time.Second)
		_ = rt.RemoveContainer(context.Background(), containerID)
	})

	if err := rt.StartContainer(ctx, containerID); err != nil {
		t.Fatalf("StartContainer: %v", err)
	}

	var status ContainerStatusInfo
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); time.Sleep(500 * time.Millisecond) {
		status, err = rt.ContainerStatus(ctx, containerID)
		if err != nil {
			t.Fatalf("ContainerStatus: %v", err)
		}
		if status.Status == StatusRunning {
			break
		}
	}
	if status.Status != StatusRunning {
		t.Fatalf("expected container status %q, got %q", StatusRunning, status.Status)
	}

	logs, err := rt.StreamLogs(ctx, containerID, false)
	if err != nil {
		t.Fatalf("StreamLogs: %v", err)
	}
	defer func() { _ = logs.Close() }()

	sawLogLine := bufio.NewScanner(logs).Scan()
	// nginx logs its startup banner to stderr immediately; if none arrived
	// yet, retry once against a fresh (follow) stream that waits for it.
	if !sawLogLine {
		followCtx, followCancel := context.WithTimeout(ctx, 10*time.Second)
		defer followCancel()
		followLogs, err := rt.StreamLogs(followCtx, containerID, true)
		if err != nil {
			t.Fatalf("StreamLogs (follow): %v", err)
		}
		defer func() { _ = followLogs.Close() }()
		sawLogLine = bufio.NewScanner(followLogs).Scan()
	}
	if !sawLogLine {
		t.Fatal("expected at least one log line from the container")
	}

	if err := rt.StopContainer(ctx, containerID, 5*time.Second); err != nil {
		t.Fatalf("StopContainer: %v", err)
	}

	status, err = rt.ContainerStatus(ctx, containerID)
	if err != nil {
		t.Fatalf("ContainerStatus after stop: %v", err)
	}
	if status.Status != StatusExited {
		t.Fatalf("expected container status %q after stop, got %q", StatusExited, status.Status)
	}

	if err := rt.RemoveContainer(ctx, containerID); err != nil {
		t.Fatalf("RemoveContainer: %v", err)
	}

	if _, err := rt.ContainerStatus(ctx, containerID); err == nil {
		t.Fatal("expected ContainerStatus to error for a removed container")
	}
}

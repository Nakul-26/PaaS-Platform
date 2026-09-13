//go:build integration

package imagebuilder

import (
	"context"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"
	"time"

	"platform/internal/runtime"
)

// TestDockerBuilder_ClonesBuildsAndPushes covers Task 3's acceptance
// (phase-7-deployment-platform.md): build a real (locally-fixtured, no
// live network dependency — same hermeticity every other phase's tests
// hold to) Git repo against a real local registry container, confirm the
// resulting image is pullable by the tag Build returned and carries the
// right commit SHA; then confirm a nonexistent ref and a broken
// Dockerfile each produce a real, non-empty error rather than a hung or
// silently-successful build.
func TestDockerBuilder_ClonesBuildsAndPushes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	rt, err := runtime.NewDockerRuntime()
	if err != nil {
		t.Fatalf("connecting to docker daemon: %v", err)
	}
	defer func() { _ = rt.Close() }()

	registryAddr := startRegistry(t, ctx, rt)

	builder, err := NewDockerBuilder()
	if err != nil {
		t.Fatalf("NewDockerBuilder: %v", err)
	}
	defer func() { _ = builder.Close() }()

	// --- Happy path: a trivial fixture repo with a real Dockerfile. ---
	repoDir := initGitFixture(t, map[string]string{
		"Dockerfile": "FROM busybox:1\nCMD [\"echo\", \"hello from image-builder\"]\n",
	})

	result, err := builder.Build(ctx, Request{
		GitURL:          repoDir,
		GitRef:          "main",
		ImageRepository: registryAddr + "/it-test/app",
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if !regexp.MustCompile(`^[0-9a-f]{40}$`).MatchString(result.CommitSHA) {
		t.Fatalf("expected a 40-char hex commit SHA, got %q", result.CommitSHA)
	}
	wantImage := registryAddr + "/it-test/app:" + result.CommitSHA
	if result.Image != wantImage {
		t.Fatalf("expected image %q, got %q", wantImage, result.Image)
	}

	// The real round-trip proof: pull the exact tag Build returned back
	// down, confirming it actually landed in the registry, not just that
	// the local daemon still has it cached from the build step.
	if err := rt.PullImage(ctx, result.Image); err != nil {
		t.Fatalf("pulling back the pushed image %s: %v", result.Image, err)
	}

	// --- Nonexistent ref: the clone step itself must fail with a real
	// error, not hang or silently produce an empty commit SHA. ---
	if _, err := builder.Build(ctx, Request{
		GitURL:          repoDir,
		GitRef:          "does-not-exist-branch",
		ImageRepository: registryAddr + "/it-test/bad-ref",
	}); err == nil {
		t.Fatalf("expected an error building a nonexistent ref, got none")
	}

	// --- Broken Dockerfile: the daemon returns 200 with an inline
	// {"error": ...} in the JSON stream (drainDockerStream's own reason for
	// existing) — confirm that's actually surfaced as a Go error, not
	// swallowed. ---
	brokenRepoDir := initGitFixture(t, map[string]string{
		"Dockerfile": "FROM this-image-definitely-does-not-exist-anywhere-12345:latest\n",
	})
	if _, err := builder.Build(ctx, Request{
		GitURL:          brokenRepoDir,
		GitRef:          "main",
		ImageRepository: registryAddr + "/it-test/broken",
	}); err == nil {
		t.Fatalf("expected an error building a Dockerfile with a nonexistent base image, got none")
	}
}

// startRegistry runs a real registry:2 container (phase-7-deployment-
// platform.md Task 2's own image) via the same runtime.DockerRuntime
// direct-container convention every other service's integration tests
// already use for non-Postgres/NATS dependencies (e.g.
// services/loadbalancer/main_integration_test.go's whoami backends),
// rather than introducing a new testcontainers module just for this test.
// Returns "localhost:<ephemeral-host-port>" — Docker daemons treat
// localhost registries as insecure/plain-HTTP automatically, so no extra
// daemon config is needed (verified manually against this same image,
// ROADMAP.md's Phase 7 Task 2 entry).
func startRegistry(t *testing.T, ctx context.Context, rt *runtime.DockerRuntime) string {
	t.Helper()

	if err := rt.PullImage(ctx, "registry:2"); err != nil {
		t.Fatalf("pulling registry:2: %v", err)
	}
	// A fixed host port, not an ephemeral one (HostPort: 0): on Docker
	// Desktop's VM-based daemon, the *daemon's own* outbound push request
	// to a loopback-published port is what actually has to succeed here
	// (ImagePush executes inside dockerd, not this test process), and that
	// path reliably failed ("connection refused") against a freshly
	// ephemeral-assigned high port while this test's own HTTP client could
	// reach the identical address/port fine one line later — some
	// DNAT/hairpin rule inside the VM not yet covering the new ephemeral
	// port. A fixed port matches the exact configuration already verified
	// working end-to-end (docker-compose.yml's own `registry` service,
	// ROADMAP.md's Phase 7 Task 2 entry).
	containerID, err := rt.CreateContainer(ctx, runtime.ContainerSpec{
		Image: "registry:2",
		Name:  "image-builder-it-registry",
		Ports: []runtime.PortBinding{{ContainerPort: 5000, HostPort: 15050}},
	})
	if err != nil {
		t.Fatalf("creating registry container: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.StopContainer(context.Background(), containerID, 5*time.Second)
		_ = rt.RemoveContainer(context.Background(), containerID)
	})
	if err := rt.StartContainer(ctx, containerID); err != nil {
		t.Fatalf("starting registry container: %v", err)
	}

	var status runtime.ContainerStatusInfo
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		status, err = rt.ContainerStatus(ctx, containerID)
		if err == nil && status.Status == runtime.StatusRunning && len(status.Ports) == 1 && status.Ports[0].HostPort != 0 {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if status.Status != runtime.StatusRunning || len(status.Ports) != 1 {
		t.Fatalf("registry container never reported running with a bound port, last status: %+v (err: %v)", status, err)
	}

	// 127.0.0.1, not "localhost": Go's dialer tried the IPv6 loopback
	// ([::1]) first when this test used "localhost" against an earlier
	// (ephemeral-port) version of this container, which nothing was bound
	// on — the literal IPv4 address sidesteps that resolution ambiguity.
	// Docker's automatic insecure-registry allowance matches loopback IPs
	// either way, so this doesn't need an explicit daemon config change.
	addr := "127.0.0.1:" + strconv.Itoa(status.Ports[0].HostPort)

	// "container running" precedes "registry's own HTTP server actually
	// accepting connections" by a short but real margin — poll its own
	// readiness endpoint rather than guessing a fixed sleep long enough.
	waitDeadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(waitDeadline) {
		resp, err := http.Get("http://" + addr + "/v2/")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return addr
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("registry at %s never became reachable over HTTP", addr)
	return ""
}

// initGitFixture creates a fresh temp directory, writes files into it, and
// commits them as a real one-commit Git repo on branch "main" — the
// hermetic, no-live-network-dependency fixture cloneRepo's own doc comment
// (and phase-7-deployment-platform.md's e2e Task 6) call for. Returns the
// directory's OS path, which git accepts directly as a clone source (no
// file:// prefix needed, and Windows file:// URIs have their own quoting
// quirks this sidesteps entirely).
func initGitFixture(t *testing.T, files map[string]string) string {
	t.Helper()

	dir, err := os.MkdirTemp("", "image-builder-fixture-*")
	if err != nil {
		t.Fatalf("creating fixture dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatalf("writing fixture file %s: %v", name, err)
		}
	}

	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	run("init", "-b", "main")
	run("-c", "user.email=test@example.com", "-c", "user.name=test", "add", "-A")
	run("-c", "user.email=test@example.com", "-c", "user.name=test", "commit", "-m", "fixture")

	return dir
}

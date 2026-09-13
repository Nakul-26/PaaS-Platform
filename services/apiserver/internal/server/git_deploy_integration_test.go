//go:build integration

package server

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"platform/internal/auth"
	"platform/internal/eventbus"
)

// TestAPIServer_GitDeploy is phase-7-deployment-platform.md Task 4's
// acceptance: the git_url mode of POST /v1/applications/:appId/deployments,
// and the build.completed consumer that turns a successful build into an
// ordinary deployment.
//
// No image-builder process runs here. The build.requested publish is
// observed directly off NATS and the build.completed replies are synthesized
// by this test, which is the same "don't stand up every downstream service to
// test one service's own contract" posture TestAPIServer_Quotas already
// established for the scheduler — Task 6's e2e test is where a real builder
// and a real registry get exercised end to end.
func TestAPIServer_GitDeploy(t *testing.T) {
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
	const registryAddr = "test-registry:5000"
	srv := New(pool, issuer, nil, bus, slog.New(slog.NewTextHandler(io.Discard, nil))).
		WithImageRegistry(registryAddr)

	// The consumer under test. SubscribeBuilds also ensures the BUILDS
	// stream, which the publish side below then relies on.
	sub, err := srv.SubscribeBuilds(ctx)
	if err != nil {
		t.Fatalf("SubscribeBuilds: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	// Independent durable observers on the two subjects apiserver publishes.
	// Durable rather than core subscriptions so a message published before
	// the observer is fully attached is still replayed off the stream rather
	// than silently missed.
	buildRequests := watchSubject(t, ctx, bus, eventbus.BuildsStream, "test-build-requested", eventbus.BuildRequestedSubject)
	placements := watchSubject(t, ctx, bus, eventbus.PlacementStream, "test-placement-requested", eventbus.PlacementRequestedSubject)

	ts := httptest.NewServer(srv.Routes())
	t.Cleanup(ts.Close)
	client := &http.Client{Timeout: 30 * time.Second}

	signup := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "git-deploy-owner@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	token := signup["access_token"].(string)
	orgID := signup["org"].(map[string]any)["id"].(string)

	project := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/orgs/"+orgID+"/projects", token,
		map[string]string{"name": "Git Deploy"}, http.StatusCreated)
	app := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/projects/"+project["id"].(string)+"/applications", token,
		map[string]any{"name": "git-app", "image": "nginx:latest"}, http.StatusCreated)
	appID := app["id"].(string)

	// --- image and git_url are mutually exclusive ---
	assertStatus(t, ctx, client, http.MethodPost, ts.URL+"/v1/applications/"+appID+"/deployments", token,
		map[string]any{"image": "nginx:latest", "git_url": "https://example.com/repo.git"}, http.StatusBadRequest)

	// --- a git deploy request creates a pending build and publishes
	// build.requested, with no deployment yet ---
	const gitURL = "https://example.com/repo.git"
	build := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/applications/"+appID+"/deployments", token,
		map[string]any{"git_url": gitURL, "git_ref": "release"}, http.StatusAccepted)
	buildID := build["id"].(string)
	if got := build["status"].(string); got != "pending" {
		t.Fatalf("new build status = %q, want %q", got, "pending")
	}
	if got := build["git_ref"].(string); got != "release" {
		t.Fatalf("new build git_ref = %q, want %q", got, "release")
	}
	if _, ok := build["image"]; ok {
		t.Fatalf("a pending build should carry no image yet, got %v", build["image"])
	}

	var requested map[string]any
	select {
	case data := <-buildRequests:
		if err := json.Unmarshal(data, &requested); err != nil {
			t.Fatalf("decoding build.requested: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for a build.requested message")
	}
	if got := requested["build_id"]; got != buildID {
		t.Fatalf("build.requested build_id = %v, want %v", got, buildID)
	}
	if got := requested["org_id"]; got != orgID {
		t.Fatalf("build.requested org_id = %v, want %v", got, orgID)
	}
	if got := requested["application_id"]; got != appID {
		t.Fatalf("build.requested application_id = %v, want %v", got, appID)
	}
	if got := requested["git_url"]; got != gitURL {
		t.Fatalf("build.requested git_url = %v, want %v", got, gitURL)
	}
	// Open Decision 3's tag scheme, minus the :<commit_sha> image-builder
	// appends once the clone resolves it.
	wantRepo := registryAddr + "/" + orgID + "/" + appID
	if got := requested["image_repository"]; got != wantRepo {
		t.Fatalf("build.requested image_repository = %v, want %v", got, wantRepo)
	}

	if n := len(listDeployments(t, ctx, client, ts.URL, appID, token)); n != 0 {
		t.Fatalf("a build request alone should create no deployment, found %d", n)
	}

	// --- a successful build.completed records the outcome and deploys it ---
	builtImage := wantRepo + ":9f2c1ab"
	publishBuildCompleted(t, ctx, bus, map[string]any{
		"build_id": buildID, "org_id": orgID, "status": "succeeded",
		"commit_sha": "9f2c1ab", "image": builtImage,
	})

	succeeded := pollBuild(t, ctx, client, ts.URL, appID, token, buildID, "succeeded")
	if got := succeeded["image"]; got != builtImage {
		t.Fatalf("succeeded build image = %v, want %v", got, builtImage)
	}
	if got := succeeded["commit_sha"]; got != "9f2c1ab" {
		t.Fatalf("succeeded build commit_sha = %v, want %v", got, "9f2c1ab")
	}
	if _, ok := succeeded["error_message"]; ok {
		t.Fatalf("a succeeded build should carry no error_message, got %v", succeeded["error_message"])
	}

	deployments := pollDeployments(t, ctx, client, ts.URL, appID, token, 1)
	if got := deployments[0].(map[string]any)["image"]; got != builtImage {
		t.Fatalf("deployment created from the build has image %v, want %v", got, builtImage)
	}

	var placed map[string]any
	select {
	case data := <-placements:
		if err := json.Unmarshal(data, &placed); err != nil {
			t.Fatalf("decoding placement.requested: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for a placement.requested message")
	}
	if got := placed["image"]; got != builtImage {
		t.Fatalf("placement.requested image = %v, want %v", got, builtImage)
	}
	if got := placed["application_id"]; got != appID {
		t.Fatalf("placement.requested application_id = %v, want %v", got, appID)
	}

	// --- a failed build.completed updates only the build, never deploys ---
	failing := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/applications/"+appID+"/deployments", token,
		map[string]any{"git_url": gitURL, "git_ref": "broken"}, http.StatusAccepted)
	failingID := failing["id"].(string)
	select {
	case <-buildRequests:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for the second build.requested message")
	}

	publishBuildCompleted(t, ctx, bus, map[string]any{
		"build_id": failingID, "org_id": orgID, "status": "failed",
		"error": "no Dockerfile at repository root",
	})

	failed := pollBuild(t, ctx, client, ts.URL, appID, token, failingID, "failed")
	if got := failed["error_message"]; got != "no Dockerfile at repository root" {
		t.Fatalf("failed build error_message = %v, want the builder's reported reason", got)
	}
	if _, ok := failed["image"]; ok {
		t.Fatalf("a failed build should carry no image, got %v", failed["image"])
	}

	// Still exactly the one deployment the *successful* build produced. Gives
	// the consumer a moment to have done the wrong thing before asserting it
	// didn't — the failed-branch write has already been observed above, so
	// any erroneous deployment would have been written by now.
	if n := len(listDeployments(t, ctx, client, ts.URL, appID, token)); n != 1 {
		t.Fatalf("a failed build must not deploy: expected 1 deployment (from the successful build), found %d", n)
	}
}

// watchSubject attaches an independent durable consumer and forwards every
// message it sees onto the returned channel, so a test can assert on what
// apiserver published without competing with the real consumer for it.
func watchSubject(t *testing.T, ctx context.Context, bus eventbus.EventBus, stream, consumer, subject string) <-chan []byte {
	t.Helper()
	seen := make(chan []byte, 16)
	sub, err := bus.SubscribeDurable(ctx, stream, consumer, subject, func(msg eventbus.Message) error {
		select {
		case seen <- msg.Data:
		default: // never block the bus on a test that stopped reading
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribing to %s: %v", subject, err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	return seen
}

func publishBuildCompleted(t *testing.T, ctx context.Context, bus eventbus.EventBus, msg map[string]any) {
	t.Helper()
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshaling build.completed: %v", err)
	}
	if err := bus.PublishDurable(ctx, eventbus.BuildCompletedSubject, data); err != nil {
		t.Fatalf("publishing build.completed: %v", err)
	}
}

func listBuilds(t *testing.T, ctx context.Context, client *http.Client, baseURL, appID, token string) []any {
	t.Helper()
	resp := doJSON(t, ctx, client, http.MethodGet, baseURL+"/v1/applications/"+appID+"/builds", token, nil, http.StatusOK)
	data, _ := resp["data"].([]any)
	return data
}

func listDeployments(t *testing.T, ctx context.Context, client *http.Client, baseURL, appID, token string) []any {
	t.Helper()
	resp := doJSON(t, ctx, client, http.MethodGet, baseURL+"/v1/applications/"+appID+"/deployments", token, nil, http.StatusOK)
	data, _ := resp["data"].([]any)
	return data
}

// pollBuild re-reads GET .../builds until buildID reports wantStatus — the
// consumer acts asynchronously off NATS, so there is no response to wait on.
func pollBuild(t *testing.T, ctx context.Context, client *http.Client, baseURL, appID, token, buildID, wantStatus string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var last map[string]any
	for {
		for _, raw := range listBuilds(t, ctx, client, baseURL, appID, token) {
			b := raw.(map[string]any)
			if b["id"] != buildID {
				continue
			}
			last = b
			if b["status"] == wantStatus {
				return b
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("build %s never reached status %q, last saw: %v", buildID, wantStatus, last)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func pollDeployments(t *testing.T, ctx context.Context, client *http.Client, baseURL, appID, token string, want int) []any {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		data := listDeployments(t, ctx, client, baseURL, appID, token)
		if len(data) >= want {
			return data
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected %d deployment(s) within the timeout, found %d", want, len(data))
		}
		time.Sleep(250 * time.Millisecond)
	}
}

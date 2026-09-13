//go:build integration

package imagebuilder

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/nats"

	"platform/internal/eventbus"
)

// stubBuilder stands in for DockerBuilder so this test exercises the
// consumer's own NATS contract handling without spending a real clone and
// docker build on it — TestDockerBuilder_ClonesBuildsAndPushes already
// covers the real builder, and the two concerns are independent.
type stubBuilder struct {
	result Result
	err    error
	seen   chan Request
}

func (s *stubBuilder) Build(_ context.Context, req Request) (Result, error) {
	s.seen <- req
	return s.result, s.err
}

// TestConsumer_PublishesOutcomes covers the wire half of Task 3 that
// TestDockerBuilder_ClonesBuildsAndPushes doesn't reach: a build.requested
// message is decoded into a Request, and exactly one build.completed is
// published for it either way.
//
// The org_id echo is what this test exists to protect. apiserver's
// build.completed consumer (phase-7-deployment-platform.md Task 4) has no
// session to scope its RLS transaction by and uses that echoed value as its
// only hint for which org's row to read — so silently dropping it here
// would leave every git-based deploy unable to complete, in a way nothing
// short of the Task 6 e2e run would otherwise notice.
func TestConsumer_PublishesOutcomes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	container, err := nats.Run(ctx, "nats:2.11.7")
	if err != nil {
		t.Fatalf("starting nats container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })
	natsURL, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("nats connection string: %v", err)
	}

	bus, err := eventbus.Connect(natsURL)
	if err != nil {
		t.Fatalf("connecting eventbus: %v", err)
	}
	t.Cleanup(func() { _ = bus.Close() })

	const orgID = "3f1c9b6e-0000-4000-8000-000000000001"

	cases := []struct {
		name    string
		builder *stubBuilder
		buildID string
		assert  func(t *testing.T, msg buildCompletedMessage)
	}{
		{
			name:    "succeeded",
			buildID: "1111aaaa-0000-4000-8000-000000000001",
			builder: &stubBuilder{result: Result{CommitSHA: "9f2c1ab", Image: "reg:5000/org/app:9f2c1ab"}},
			assert: func(t *testing.T, msg buildCompletedMessage) {
				if msg.Status != "succeeded" {
					t.Fatalf("status = %q, want succeeded", msg.Status)
				}
				if msg.CommitSHA != "9f2c1ab" || msg.Image != "reg:5000/org/app:9f2c1ab" {
					t.Fatalf("success outcome lost the builder's result: %+v", msg)
				}
				if msg.Error != "" {
					t.Fatalf("a succeeded build must carry no error, got %q", msg.Error)
				}
			},
		},
		{
			name:    "failed",
			buildID: "1111aaaa-0000-4000-8000-000000000002",
			builder: &stubBuilder{err: errors.New("no Dockerfile at repository root")},
			assert: func(t *testing.T, msg buildCompletedMessage) {
				if msg.Status != "failed" {
					t.Fatalf("status = %q, want failed", msg.Status)
				}
				if msg.Error != "no Dockerfile at repository root" {
					t.Fatalf("failure outcome lost the builder's reason: %+v", msg)
				}
				if msg.CommitSHA != "" || msg.Image != "" {
					t.Fatalf("a failed build must carry neither commit_sha nor image, got %+v", msg)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.builder.seen = make(chan Request, 1)
			consumer := NewConsumer(bus, tc.builder, slog.New(slog.NewTextHandler(io.Discard, nil)))
			sub, err := consumer.Subscribe(ctx)
			if err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
			defer func() { _ = sub.Unsubscribe() }()

			// Each subtest attaches its own durable consumer, which replays
			// the stream from the start — so filter to this subtest's own
			// build rather than reading whatever the previous one left
			// behind.
			completed := make(chan buildCompletedMessage, 1)
			outSub, err := bus.SubscribeDurable(ctx, eventbus.BuildsStream, "test-"+tc.name, eventbus.BuildCompletedSubject, func(msg eventbus.Message) error {
				var decoded buildCompletedMessage
				if err := json.Unmarshal(msg.Data, &decoded); err != nil || decoded.BuildID != tc.buildID {
					return nil
				}
				select {
				case completed <- decoded:
				default:
				}
				return nil
			})
			if err != nil {
				t.Fatalf("subscribing to build.completed: %v", err)
			}
			defer func() { _ = outSub.Unsubscribe() }()

			request := buildRequestedMessage{
				BuildID:         tc.buildID,
				OrgID:           orgID,
				ApplicationID:   "2222bbbb-0000-4000-8000-000000000001",
				GitURL:          "https://example.com/repo.git",
				GitRef:          "release",
				ImageRepository: "reg:5000/org/app",
			}
			data, err := json.Marshal(request)
			if err != nil {
				t.Fatalf("marshaling build.requested: %v", err)
			}
			if err := bus.PublishDurable(ctx, eventbus.BuildRequestedSubject, data); err != nil {
				t.Fatalf("publishing build.requested: %v", err)
			}

			select {
			case got := <-tc.builder.seen:
				want := Request{GitURL: request.GitURL, GitRef: request.GitRef, ImageRepository: request.ImageRepository}
				if got != want {
					t.Fatalf("builder received %+v, want %+v", got, want)
				}
			case <-time.After(30 * time.Second):
				t.Fatal("timed out waiting for the builder to be invoked")
			}

			var msg buildCompletedMessage
			select {
			case msg = <-completed:
			case <-time.After(30 * time.Second):
				t.Fatal("timed out waiting for a build.completed message")
			}

			if msg.BuildID != tc.buildID {
				t.Fatalf("build.completed build_id = %q, want %q", msg.BuildID, tc.buildID)
			}
			if msg.OrgID != orgID {
				t.Fatalf("build.completed dropped the org_id echo: got %q, want %q", msg.OrgID, orgID)
			}
			tc.assert(t, msg)
		})
	}
}

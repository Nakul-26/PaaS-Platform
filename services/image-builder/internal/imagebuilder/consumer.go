package imagebuilder

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"platform/internal/eventbus"
)

// buildRequestedMessage/buildCompletedMessage mirror
// docs/nats-contract.md's build.requested/build.completed schema
// (phase-7-deployment-platform.md Task 3/4) — per ADR-0012, image-builder
// depends only on the published NATS contract, never apiserver's package
// tree, so these shapes are defined here rather than imported from
// anywhere.
type buildRequestedMessage struct {
	BuildID       string `json:"build_id"`
	OrgID         string `json:"org_id"`
	ApplicationID string `json:"application_id"`
	GitURL        string `json:"git_url"`
	GitRef        string `json:"git_ref"`
	// ImageRepository is the push target without a tag; Build appends
	// ":<commit_sha>" once the clone resolves it.
	ImageRepository string `json:"image_repository"`
}

// buildCompletedMessage's Status is "succeeded" or "failed" — one subject
// for both, see eventbus.BuildCompletedSubject's own doc comment for why.
//
// OrgID is echoed verbatim from the originating build.requested and is
// never interpreted here: apiserver's consumer needs some org to open an
// RLS-scoped transaction with before it can read the builds row that
// carries the real one (docs/nats-contract.md's build.completed section
// explains why that's a hint rather than an authority). image-builder
// itself never touches Postgres at all (ADR-0012).
type buildCompletedMessage struct {
	BuildID   string `json:"build_id"`
	OrgID     string `json:"org_id"`
	Status    string `json:"status"`
	CommitSHA string `json:"commit_sha,omitempty"`
	Image     string `json:"image,omitempty"`
	Error     string `json:"error,omitempty"`
}

// Consumer wires an ImageBuilder to the build.requested/build.completed
// NATS contract — the only thing image-builder's main.go constructs.
type Consumer struct {
	bus     eventbus.EventBus
	builder ImageBuilder
	logger  *slog.Logger
}

func NewConsumer(bus eventbus.EventBus, builder ImageBuilder, logger *slog.Logger) *Consumer {
	return &Consumer{bus: bus, builder: builder, logger: logger}
}

// Subscribe ensures the BUILDS stream exists, then subscribes durably to
// build.requested (docs/nats-contract.md: a lost build request strands a
// deploy attempt with nothing to retry it — same "unacceptable loss"
// reasoning placement.requested already established).
func (c *Consumer) Subscribe(ctx context.Context) (eventbus.Subscription, error) {
	if err := c.bus.EnsureStream(ctx, eventbus.StreamConfig{
		Name:     eventbus.BuildsStream,
		Subjects: eventbus.BuildsStreamSubjects,
	}); err != nil {
		return nil, fmt.Errorf("ensuring %s stream: %w", eventbus.BuildsStream, err)
	}

	return c.bus.SubscribeDurable(ctx, eventbus.BuildsStream, "image-builder-build", eventbus.BuildRequestedSubject, func(msg eventbus.Message) error {
		c.handleBuildRequested(ctx, msg.Data)
		return nil
	})
}

// handleBuildRequested drives one build attempt end to end and always
// publishes exactly one build.completed outcome for a parseable request —
// a bad payload (can't even identify which build to report failure
// against) is the sole case logged-and-dropped with nothing published,
// same posture handlePlacement/handleAssignment already establish for
// their own unparseable-payload case.
func (c *Consumer) handleBuildRequested(ctx context.Context, data []byte) {
	var msg buildRequestedMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		c.logger.Error("decoding build.requested message", "error", err)
		return
	}

	result, buildErr := c.builder.Build(ctx, Request{
		GitURL:          msg.GitURL,
		GitRef:          msg.GitRef,
		ImageRepository: msg.ImageRepository,
	})

	completed := buildCompletedMessage{BuildID: msg.BuildID, OrgID: msg.OrgID}
	if buildErr != nil {
		c.logger.Error("build failed", "build_id", msg.BuildID, "git_url", msg.GitURL, "git_ref", msg.GitRef, "error", buildErr)
		completed.Status = "failed"
		completed.Error = buildErr.Error()
	} else {
		c.logger.Info("build succeeded", "build_id", msg.BuildID, "image", result.Image)
		completed.Status = "succeeded"
		completed.CommitSHA = result.CommitSHA
		completed.Image = result.Image
	}

	payload, err := json.Marshal(completed)
	if err != nil {
		c.logger.Error("marshaling build.completed message", "build_id", msg.BuildID, "error", err)
		return
	}
	if err := c.bus.PublishDurable(ctx, eventbus.BuildCompletedSubject, payload); err != nil {
		c.logger.Error("publishing build.completed", "build_id", msg.BuildID, "error", err)
	}
}

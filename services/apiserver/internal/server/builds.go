package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"platform/internal/db"
	"platform/internal/eventbus"
)

// buildRequestedMessage/buildCompletedMessage mirror
// docs/nats-contract.md's build.requested/build.completed schema —
// apiserver's own copy, since per ADR-0012 it may depend only on the
// published NATS contract, never image-builder's package tree (the same
// reasoning placement.go's copies already document, and the same reason
// image-builder keeps its own copies of these two shapes).
type buildRequestedMessage struct {
	BuildID         string `json:"build_id"`
	OrgID           string `json:"org_id"`
	ApplicationID   string `json:"application_id"`
	GitURL          string `json:"git_url"`
	GitRef          string `json:"git_ref"`
	ImageRepository string `json:"image_repository"`
}

type buildCompletedMessage struct {
	BuildID   string `json:"build_id"`
	OrgID     string `json:"org_id"`
	Status    string `json:"status"`
	CommitSHA string `json:"commit_sha,omitempty"`
	Image     string `json:"image,omitempty"`
	Error     string `json:"error,omitempty"`
}

// SubscribeBuilds ensures the BUILDS stream exists and starts the durable
// build.completed consumer (phase-7-deployment-platform.md Task 4). This is
// the first subscription apiserver has ever held — every prior NATS
// interaction it has was publish-only — so unlike handleDeploy's nil-bus
// check, the caller (main.go) decides what a failure here means for the
// process; a nil bus makes this a no-op rather than an error, matching the
// "apiserver keeps serving every other route even if NATS was unreachable"
// posture Phase 2 Task 6 established.
func (s *Server) SubscribeBuilds(ctx context.Context) (eventbus.Subscription, error) {
	if s.bus == nil {
		return nil, nil
	}
	if err := s.bus.EnsureStream(ctx, eventbus.StreamConfig{
		Name:     eventbus.BuildsStream,
		Subjects: eventbus.BuildsStreamSubjects,
	}); err != nil {
		return nil, fmt.Errorf("ensuring %s stream: %w", eventbus.BuildsStream, err)
	}
	return s.bus.SubscribeDurable(ctx, eventbus.BuildsStream, "apiserver-build-completed", eventbus.BuildCompletedSubject, func(msg eventbus.Message) error {
		s.handleBuildCompleted(ctx, msg.Data)
		return nil
	})
}

// handleBuildCompleted records one build's terminal outcome and, on
// success, turns it into an ordinary deployment — the step that makes
// `deploy --git` a single user action rather than two
// (phase-7-deployment-platform.md Open Decision 2).
//
// There is no request-scoped session here, so the actor for the resulting
// deployment.create audit entry is the build's own created_by: the user
// who asked for this build is the user this deployment is on behalf of.
// Acting without a live session is the same posture controller-manager's
// reconcile loops already establish (internal/db.ReconcileRepository's own
// doc comment).
//
// A failed build only updates the builds row — no deployment, no audit
// entry. That draws Phase 6 Task 7's "system state transitions aren't
// audited, only the initiating user action is" line between "a user
// requested a build" (audited, at request time) and "the build finished"
// (not).
//
// Errors are logged and swallowed rather than returned: returning one
// would nak the message and redeliver it, and every failure mode reachable
// here (unparseable payload, a build id that resolves to nothing, a
// quota-trigger rejection at insert time) is deterministic — redelivery
// would loop on it forever rather than converge.
func (s *Server) handleBuildCompleted(ctx context.Context, data []byte) {
	var msg buildCompletedMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		s.logger.Error("decoding build.completed message", "error", err)
		return
	}
	buildID, err := uuid.Parse(msg.BuildID)
	if err != nil {
		s.logger.Error("build.completed carries an unparseable build_id", "build_id", msg.BuildID, "error", err)
		return
	}
	// org_id is only a scoping hint, never an authority — see
	// docs/nats-contract.md's build.completed section. A wrong or forged
	// value simply leaves RLS with no row to return for buildID below, so
	// this fails closed instead of reaching another tenant's data.
	orgHint, err := uuid.Parse(msg.OrgID)
	if err != nil {
		s.logger.Error("build.completed carries an unparseable org_id", "build_id", msg.BuildID, "error", err)
		return
	}

	var (
		build      db.Build
		app        db.Application
		deployment db.Deployment
		deployed   bool
	)
	err = s.pool.WithTx(ctx, uuid.Nil, orgHint, func(ctx context.Context, conn db.Conn) error {
		builds := db.NewBuildRepository(conn)
		var err error
		build, err = builds.Get(ctx, buildID)
		if err != nil {
			return err
		}
		// Everything from here on uses the row's own identifiers, not the
		// message's. Setting the build's creator as the current user
		// mid-transaction mirrors handleDeploy's own mid-tx SetCurrentOrg,
		// in reverse: the identity RLS keys off isn't known until the first
		// query has run.
		if err := db.SetCurrentUser(ctx, conn, build.CreatedBy); err != nil {
			return err
		}

		if msg.Status != "succeeded" {
			errMsg := msg.Error
			if errMsg == "" {
				errMsg = "build failed without a reported reason"
			}
			return builds.UpdateResult(ctx, buildID, db.BuildStatusFailed, nil, nil, &errMsg)
		}
		if msg.Image == "" {
			errMsg := "build reported success without an image reference"
			s.logger.Error(errMsg, "build_id", buildID)
			return builds.UpdateResult(ctx, buildID, db.BuildStatusFailed, nil, nil, &errMsg)
		}

		if err := builds.UpdateResult(ctx, buildID, db.BuildStatusSucceeded, nullableString(msg.CommitSHA), &msg.Image, nil); err != nil {
			return err
		}

		app, err = db.NewApplicationRepository(conn).Get(ctx, build.ApplicationID)
		if err != nil {
			return err
		}
		deployment, err = createDeploymentTx(ctx, conn, build.OrgID, build.ApplicationID, build.CreatedBy, &app, msg.Image)
		if err != nil {
			return err
		}
		deployed = true
		return nil
	})
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			s.logger.Error("build.completed names a build that doesn't exist in the org it claims", "build_id", buildID, "org_id", msg.OrgID)
			return
		}
		s.logger.Error("recording build.completed", "build_id", buildID, "error", err)
		return
	}
	if !deployed {
		return
	}

	s.logger.Info("build succeeded, deploying", "build_id", buildID, "deployment_id", deployment.ID, "image", msg.Image)
	if err := s.publishPlacement(ctx, build.OrgID, build.CreatedBy, app, deployment); err != nil {
		// publishPlacement has already marked the deployment failed and
		// logged the cause; nothing further to do here, and no caller to
		// report a status code to.
		s.logger.Error("deploying a successful build", "build_id", buildID, "deployment_id", deployment.ID, "error", err)
	}
}

func nullableString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

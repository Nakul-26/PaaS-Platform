package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"platform/internal/auth"
	"platform/internal/db"
	"platform/internal/eventbus"
)

// buildResponse mirrors db.Build (phase-7-deployment-platform.md Task 1).
// CommitSHA/Image are populated only once a build succeeds, ErrorMessage
// only once one fails — a build still in flight has none of the three.
type buildResponse struct {
	ID            string     `json:"id"`
	ApplicationID string     `json:"application_id"`
	GitURL        string     `json:"git_url"`
	GitRef        string     `json:"git_ref"`
	CommitSHA     *string    `json:"commit_sha,omitempty"`
	Status        string     `json:"status"`
	Image         *string    `json:"image,omitempty"`
	ErrorMessage  *string    `json:"error_message,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	CompletedAt   *time.Time `json:"completed_at,omitempty"`
}

func toBuildResponse(b db.Build) buildResponse {
	return buildResponse{
		ID: b.ID.String(), ApplicationID: b.ApplicationID.String(),
		GitURL: b.GitURL, GitRef: b.GitRef, CommitSHA: b.CommitSHA,
		Status: string(b.Status), Image: b.Image, ErrorMessage: b.ErrorMessage,
		CreatedAt: b.CreatedAt, CompletedAt: b.CompletedAt,
	}
}

// imageRepositoryFor is the push target this platform assigns a given
// application's builds, without a tag — image-builder appends
// ":<commit_sha>" itself once the clone resolves it
// (phase-7-deployment-platform.md Open Decision 3). The org-then-
// application path segments keep the shared local registry's namespace
// collision-free across tenants without needing per-tenant registry auth.
func (s *Server) imageRepositoryFor(orgID, appID uuid.UUID) string {
	return fmt.Sprintf("%s/%s/%s", s.registryAddr, orgID, appID)
}

// handleGitDeploy is the git_url branch of POST
// /v1/applications/:appId/deployments (phase-7-deployment-platform.md Task
// 4), split out of handleDeploy rather than nested inside it since almost
// nothing after the shared permission/quota preamble is common between the
// two modes.
//
// No deployment exists yet when this returns — only a build request — so
// unlike the image path's 201-with-a-deployment, this answers 202 Accepted
// with the build to poll. The deployments row appears later, created by
// handleBuildCompleted if and when the build succeeds.
//
// Gated by the same application.deploy permission as the image path:
// rbac-multitenancy.md §2 doesn't distinguish how a deploy was triggered,
// only that one was.
func (s *Server) handleGitDeploy(w http.ResponseWriter, r *http.Request, appID, userID uuid.UUID, req deployRequest) {
	ctx := r.Context()
	var (
		orgID uuid.UUID
		build db.Build
	)
	err := s.pool.WithTx(ctx, userID, uuid.Nil, func(ctx context.Context, conn db.Conn) error {
		var err error
		orgID, err = db.NewApplicationRepository(conn).OrgID(ctx, appID)
		if err != nil {
			return mapDBError(err, "no application with this id in an organization you belong to", "")
		}
		if err := db.SetCurrentOrg(ctx, conn, orgID); err != nil {
			return err
		}
		if err := requirePermission(ctx, conn, userID, orgID, auth.PermApplicationDeploy); err != nil {
			return err
		}

		app, err := db.NewApplicationRepository(conn).Get(ctx, appID)
		if err != nil {
			return mapDBError(err, "no application with this id in an organization you belong to", "")
		}

		// Layer 1 of the three-layer quota scheme, run here at build-*request*
		// time rather than at build-completion time: rejecting early avoids
		// spending minutes of build capacity on a deploy that could never be
		// placed, and nothing about an org's quota changes across a build
		// that would make a re-check afterwards meaningful
		// (phase-7-deployment-platform.md Open Decision 5). Layers 2
		// (scheduler) and 3 (Postgres trigger) still apply unchanged at
		// actual placement time, whichever path produced the deployment.
		if err := checkDeploymentQuota(ctx, conn, orgID); err != nil {
			return err
		}
		if err := checkResourceQuota(ctx, conn, orgID, app, app.ReplicasDesired); err != nil {
			return err
		}

		build, err = db.NewBuildRepository(conn).Create(ctx, orgID, appID, req.GitURL, req.GitRef, userID)
		if err != nil {
			return err
		}
		return recordAudit(ctx, conn, orgID, userID, "build.create", "build", build.ID, map[string]any{
			"application_id": appID.String(), "git_url": build.GitURL, "git_ref": build.GitRef,
		})
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}

	// Same outside-the-transaction publish posture as publishPlacement, and
	// the same treatment of a publish failure: nothing will ever pick this
	// build up, so it's terminal and gets written back rather than left
	// looking pending forever.
	if err := s.publishBuildRequested(ctx, build, appID, orgID); err != nil {
		s.logger.Error("publishing build.requested", "build_id", build.ID, "error", err)
		if updateErr := s.pool.WithTx(ctx, userID, orgID, func(ctx context.Context, conn db.Conn) error {
			msg := fmt.Sprintf("failed to queue build: %v", err)
			return db.NewBuildRepository(conn).UpdateResult(ctx, build.ID, db.BuildStatusFailed, nil, nil, &msg)
		}); updateErr != nil {
			s.logger.Error("recording build result", "build_id", build.ID, "error", updateErr)
		}
		s.writeError(w, r, &apiError{http.StatusBadGateway, "deploy_failed", fmt.Sprintf("failed to publish build request: %v", err)})
		return
	}

	writeJSON(w, http.StatusAccepted, toBuildResponse(build))
}

func (s *Server) publishBuildRequested(ctx context.Context, build db.Build, appID, orgID uuid.UUID) error {
	if s.bus == nil {
		return errors.New("event bus not connected")
	}
	data, err := json.Marshal(buildRequestedMessage{
		BuildID:         build.ID.String(),
		OrgID:           orgID.String(),
		ApplicationID:   appID.String(),
		GitURL:          build.GitURL,
		GitRef:          build.GitRef,
		ImageRepository: s.imageRepositoryFor(orgID, appID),
	})
	if err != nil {
		return err
	}
	return s.bus.PublishDurable(ctx, eventbus.BuildRequestedSubject, data)
}

// handleGetBuilds is GET /v1/applications/:appId/builds?limit=&cursor=
// (phase-7-deployment-platform.md Task 4). Read-only, so it takes the same
// membership-only posture as handleGetDeployments (rbac-multitenancy.md §2:
// every role, viewer included, may see what's deployed), and paginates
// identically to handleGetAuditLogs — BuildRepository.ListByApplication
// already returns an opaque cursor in that shape.
func (s *Server) handleGetBuilds(w http.ResponseWriter, r *http.Request) {
	userID, _ := auth.UserIDFromContext(r.Context())
	appID, err := uuid.Parse(r.PathValue("appId"))
	if err != nil {
		s.writeError(w, r, errBadRequest("appId must be a valid UUID"))
		return
	}

	limit := 50
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 && n <= 200 {
			limit = n
		}
	}
	cursor := r.URL.Query().Get("cursor")

	var (
		builds     []db.Build
		nextCursor string
	)
	err = s.pool.WithTx(r.Context(), userID, uuid.Nil, func(ctx context.Context, conn db.Conn) error {
		orgID, err := db.NewApplicationRepository(conn).OrgID(ctx, appID)
		if err != nil {
			return mapDBError(err, "no application with this id in an organization you belong to", "")
		}
		if err := db.SetCurrentOrg(ctx, conn, orgID); err != nil {
			return err
		}
		if _, err := requireMembership(ctx, conn, userID, orgID); err != nil {
			return err
		}
		builds, nextCursor, err = db.NewBuildRepository(conn).ListByApplication(ctx, appID, limit, cursor)
		return err
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}

	data := make([]buildResponse, len(builds))
	for i, b := range builds {
		data[i] = toBuildResponse(b)
	}
	var respCursor *string
	if nextCursor != "" {
		respCursor = &nextCursor
	}
	writeJSON(w, http.StatusOK, struct {
		Data       []buildResponse `json:"data"`
		NextCursor *string         `json:"next_cursor"`
	}{Data: data, NextCursor: respCursor})
}

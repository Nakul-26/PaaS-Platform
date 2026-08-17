package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"platform/internal/auth"
	"platform/internal/db"
	"platform/services/apiserver/internal/workerclient"
)

type deploymentResponse struct {
	ID                string     `json:"id"`
	ApplicationID     string     `json:"application_id"`
	Image             string     `json:"image"`
	Revision          int        `json:"revision"`
	Status            string     `json:"status"`
	Strategy          string     `json:"strategy"`
	WorkerContainerID *string    `json:"worker_container_id,omitempty"`
	CreatedAt         time.Time  `json:"created_at"`
	CompletedAt       *time.Time `json:"completed_at,omitempty"`
}

func toDeploymentResponse(d db.Deployment) deploymentResponse {
	return deploymentResponse{
		ID: d.ID.String(), ApplicationID: d.ApplicationID.String(), Image: d.Image,
		Revision: d.Revision, Status: string(d.Status), Strategy: d.Strategy,
		WorkerContainerID: d.WorkerContainerID, CreatedAt: d.CreatedAt, CompletedAt: d.CompletedAt,
	}
}

type deployRequest struct {
	Image string `json:"image,omitempty"`
}

// handleDeploy is POST /v1/applications/:appId/deployments. Phase 1 has
// exactly one worker, called directly over HTTP (phase-1-mvp.md Task 5) —
// no scheduler yet, so "deploy" means "record a deployment row, then drive
// the one worker to run it," recreate-only, single replica.
func (s *Server) handleDeploy(w http.ResponseWriter, r *http.Request) {
	userID, _ := auth.UserIDFromContext(r.Context())
	appID, err := uuid.Parse(r.PathValue("appId"))
	if err != nil {
		s.writeError(w, r, errBadRequest("appId must be a valid UUID"))
		return
	}

	var req deployRequest
	if r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			s.writeError(w, r, errBadRequest("request body must be valid JSON with an optional image"))
			return
		}
	}

	ctx := r.Context()
	var (
		orgID      uuid.UUID
		deployment db.Deployment
		app        db.Application
	)
	err = s.pool.WithTx(ctx, userID, uuid.Nil, func(ctx context.Context, conn db.Conn) error {
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

		apps := db.NewApplicationRepository(conn)
		app, err = apps.Get(ctx, appID)
		if err != nil {
			return mapDBError(err, "no application with this id in an organization you belong to", "")
		}

		image := app.Image
		if req.Image != "" && req.Image != app.Image {
			image = req.Image
			if err := apps.UpdateImage(ctx, appID, image); err != nil {
				return err
			}
			app.Image = image
		}

		deployment, err = db.NewDeploymentRepository(conn).Create(ctx, orgID, appID, image, userID)
		return mapDBError(err, "", "a deployment for this revision already exists")
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}

	// Driving the worker is a network call outside any DB transaction
	// (ADR-0012 — never held across a cross-service call). Its outcome is
	// then recorded back onto the deployment row for the audit trail
	// (database-schema.md's "what was deployed when").
	ports := make([]workerclient.PortBinding, len(app.Ports))
	for i, p := range app.Ports {
		ports[i] = workerclient.PortBinding{ContainerPort: p.ContainerPort, HostPort: p.HostPort, Protocol: p.Protocol}
	}
	containerName := fmt.Sprintf("app-%s-rev%d", appID.String()[:8], deployment.Revision)

	status, workerErr := s.worker.CreateContainer(ctx, workerclient.CreateContainerRequest{
		Image: deployment.Image,
		Name:  containerName,
		Ports: ports,
	})

	finalStatus := mapWorkerStatus(status.Status)
	var containerID *string
	if workerErr == nil {
		containerID = &status.ID
	} else {
		s.logger.Error("driving worker for deployment", "deployment_id", deployment.ID, "error", workerErr)
		finalStatus = db.DeploymentStatusFailed
	}

	updateErr := s.pool.WithTx(ctx, userID, orgID, func(ctx context.Context, conn db.Conn) error {
		return db.NewDeploymentRepository(conn).UpdateResult(ctx, deployment.ID, finalStatus, containerID)
	})
	if updateErr != nil {
		s.logger.Error("recording deployment result", "deployment_id", deployment.ID, "error", updateErr)
	}

	deployment.Status = finalStatus
	deployment.WorkerContainerID = containerID
	if workerErr != nil {
		s.writeError(w, r, &apiError{http.StatusBadGateway, "deploy_failed", fmt.Sprintf("worker rejected the deployment: %v", workerErr)})
		return
	}
	writeJSON(w, http.StatusCreated, toDeploymentResponse(deployment))
}

func mapWorkerStatus(workerStatus string) db.DeploymentStatus {
	switch workerStatus {
	case "running":
		return db.DeploymentStatusRunning
	case "exited":
		return db.DeploymentStatusFailed
	default:
		return db.DeploymentStatusScheduling
	}
}

// handleGetDeployments is GET /v1/applications/:appId/deployments,
// cursor-paginated per api-conventions.md §4.
func (s *Server) handleGetDeployments(w http.ResponseWriter, r *http.Request) {
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
	afterRevision, err := decodeRevisionCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		s.writeError(w, r, errBadRequest("cursor is invalid"))
		return
	}

	var deployments []db.Deployment
	err = s.pool.WithTx(r.Context(), userID, uuid.Nil, func(ctx context.Context, conn db.Conn) error {
		orgID, err := db.NewApplicationRepository(conn).OrgID(ctx, appID)
		if err != nil {
			return mapDBError(err, "no application with this id in an organization you belong to", "")
		}
		if err := db.SetCurrentOrg(ctx, conn, orgID); err != nil {
			return err
		}
		// Any org member may view deployments (rbac-multitenancy.md §2:
		// logs/metrics viewing is available to every role, including
		// viewer) — requirePermission is used here only to confirm
		// membership, not to gate on a specific permission.
		if _, err := requireMembership(ctx, conn, userID, orgID); err != nil {
			return err
		}
		deployments, err = db.NewDeploymentRepository(conn).ListByApplication(ctx, appID, limit, afterRevision)
		return err
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}

	data := make([]deploymentResponse, len(deployments))
	for i, d := range deployments {
		data[i] = toDeploymentResponse(d)
	}
	var nextCursor *string
	if len(deployments) == limit {
		c := encodeRevisionCursor(deployments[len(deployments)-1].Revision)
		nextCursor = &c
	}
	writeJSON(w, http.StatusOK, struct {
		Data       []deploymentResponse `json:"data"`
		NextCursor *string              `json:"next_cursor"`
	}{Data: data, NextCursor: nextCursor})
}

func encodeRevisionCursor(revision int) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(revision)))
}

func decodeRevisionCursor(cursor string) (*int, error) {
	if cursor == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, err
	}
	revision, err := strconv.Atoi(string(raw))
	if err != nil {
		return nil, err
	}
	return &revision, nil
}

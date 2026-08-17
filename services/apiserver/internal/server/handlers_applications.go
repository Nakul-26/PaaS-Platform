package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"

	"platform/internal/auth"
	"platform/internal/db"
	"platform/services/apiserver/internal/workerclient"
)

type portSpecDTO struct {
	ContainerPort int    `json:"container_port"`
	HostPort      int    `json:"host_port,omitempty"`
	Protocol      string `json:"protocol,omitempty"`
}

type applicationResponse struct {
	ID              string        `json:"id"`
	OrgID           string        `json:"org_id"`
	ProjectID       string        `json:"project_id"`
	Name            string        `json:"name"`
	Image           string        `json:"image"`
	ReplicasDesired int           `json:"replicas_desired"`
	Ports           []portSpecDTO `json:"ports"`
	CreatedAt       time.Time     `json:"created_at"`
}

func toApplicationResponse(a db.Application) applicationResponse {
	ports := make([]portSpecDTO, len(a.Ports))
	for i, p := range a.Ports {
		ports[i] = portSpecDTO{ContainerPort: p.ContainerPort, HostPort: p.HostPort, Protocol: p.Protocol}
	}
	return applicationResponse{
		ID: a.ID.String(), OrgID: a.OrgID.String(), ProjectID: a.ProjectID.String(),
		Name: a.Name, Image: a.Image, ReplicasDesired: a.ReplicasDesired, Ports: ports, CreatedAt: a.CreatedAt,
	}
}

type createApplicationRequest struct {
	Name  string        `json:"name"`
	Image string        `json:"image"`
	Ports []portSpecDTO `json:"ports,omitempty"`
}

// handleCreateApplication is POST /v1/projects/:projectId/applications
// (api-conventions.md §2) — a "deep by-ID" route with no orgId in the URL,
// so org_id must be resolved from the project first (database-schema.md
// §3's two-branch RLS policy is exactly what makes that resolution safe).
func (s *Server) handleCreateApplication(w http.ResponseWriter, r *http.Request) {
	userID, _ := auth.UserIDFromContext(r.Context())
	projectID, err := uuid.Parse(r.PathValue("projectId"))
	if err != nil {
		s.writeError(w, r, errBadRequest("projectId must be a valid UUID"))
		return
	}

	var req createApplicationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, r, errBadRequest("request body must be valid JSON with name and image"))
		return
	}
	if req.Name == "" || req.Image == "" {
		s.writeError(w, r, errUnprocessable("name and image are required"))
		return
	}
	ports := make([]db.PortSpec, len(req.Ports))
	for i, p := range req.Ports {
		ports[i] = db.PortSpec{ContainerPort: p.ContainerPort, HostPort: p.HostPort, Protocol: p.Protocol}
	}

	var app db.Application
	err = s.pool.WithTx(r.Context(), userID, uuid.Nil, func(ctx context.Context, conn db.Conn) error {
		orgID, err := db.NewProjectRepository(conn).OrgID(ctx, projectID)
		if err != nil {
			return mapDBError(err, "no project with this id in an organization you belong to", "")
		}
		if err := db.SetCurrentOrg(ctx, conn, orgID); err != nil {
			return err
		}
		if err := requirePermission(ctx, conn, userID, orgID, auth.PermApplicationCreate); err != nil {
			return err
		}
		app, err = db.NewApplicationRepository(conn).Create(ctx, orgID, projectID, req.Name, req.Image, ports)
		return mapDBError(err, "", "an application with this name already exists")
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, toApplicationResponse(app))
}

// handleDeleteApplication is DELETE /v1/applications/:appId. Per the
// exit-criteria script (phase-1-mvp.md), deleting an application must
// actually stop and remove whatever the worker is running for it — so the
// worker call happens before the DB delete commits, and a worker failure
// (other than "already gone") aborts the deletion rather than leaving an
// orphaned container.
func (s *Server) handleDeleteApplication(w http.ResponseWriter, r *http.Request) {
	userID, _ := auth.UserIDFromContext(r.Context())
	appID, err := uuid.Parse(r.PathValue("appId"))
	if err != nil {
		s.writeError(w, r, errBadRequest("appId must be a valid UUID"))
		return
	}
	ctx := r.Context()

	var (
		orgID       uuid.UUID
		containerID *string
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
		if err := requirePermission(ctx, conn, userID, orgID, auth.PermApplicationDelete); err != nil {
			return err
		}
		latest, err := db.NewDeploymentRepository(conn).Latest(ctx, appID)
		if err != nil && !errors.Is(err, db.ErrNotFound) {
			return err
		}
		if err == nil {
			containerID = latest.WorkerContainerID
		}
		return nil
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}

	if containerID != nil && *containerID != "" {
		if _, err := s.worker.StopContainer(ctx, *containerID, 10); err != nil && !errors.Is(err, workerclient.ErrNotFound) {
			s.logger.Error("stopping container before application delete", "container_id", *containerID, "error", err)
			s.writeError(w, r, &apiError{http.StatusBadGateway, "runtime_error", "could not stop the running deployment; application was not deleted"})
			return
		}
		if err := s.worker.RemoveContainer(ctx, *containerID); err != nil && !errors.Is(err, workerclient.ErrNotFound) {
			s.logger.Error("removing container before application delete", "container_id", *containerID, "error", err)
			s.writeError(w, r, &apiError{http.StatusBadGateway, "runtime_error", "could not remove the running deployment; application was not deleted"})
			return
		}
	}

	err = s.pool.WithTx(ctx, userID, orgID, func(ctx context.Context, conn db.Conn) error {
		if err := requirePermission(ctx, conn, userID, orgID, auth.PermApplicationDelete); err != nil {
			return err
		}
		return mapDBError(db.NewApplicationRepository(conn).Delete(ctx, appID), "no application with this id in an organization you belong to", "")
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

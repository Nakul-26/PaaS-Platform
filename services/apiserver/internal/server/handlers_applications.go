package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"

	"platform/internal/auth"
	"platform/internal/db"
	"platform/internal/eventbus"
)

// unassignMessage mirrors docs/nats-contract.md's node.<id>.unassign schema —
// the same shape controller-manager's and nodeagent's own (unexported)
// copies use (phase-3-controllers.md Task 6: no shared Go type across
// service boundaries, ADR-0012 — the wire contract is the doc, not a
// package).
type unassignMessage struct {
	AssignmentID string `json:"assignment_id"`
	ContainerID  string `json:"container_id"`
}

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

type scaleApplicationRequest struct {
	ReplicasDesired *int `json:"replicas_desired"`
}

// handleScaleApplication is PATCH /v1/applications/:appId
// (api-conventions.md §4: partial updates use PATCH), gated behind the same
// PermApplicationDeploy permission handleDeploy uses — scaling is a form of
// desired-state change, same trust level as deploying a new image. It only
// writes applications.replicas_desired; it does not place or remove any
// container itself (phase-3-controllers.md Task 5) — the Deployment
// controller (Task 4) picks up the new desired state on its next reconcile
// tick, exactly like it would react to a crash.
func (s *Server) handleScaleApplication(w http.ResponseWriter, r *http.Request) {
	userID, _ := auth.UserIDFromContext(r.Context())
	appID, err := uuid.Parse(r.PathValue("appId"))
	if err != nil {
		s.writeError(w, r, errBadRequest("appId must be a valid UUID"))
		return
	}

	var req scaleApplicationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, r, errBadRequest("request body must be valid JSON with replicas_desired"))
		return
	}
	if req.ReplicasDesired == nil || *req.ReplicasDesired < 0 {
		s.writeError(w, r, errUnprocessable("replicas_desired is required and must be zero or greater"))
		return
	}

	var app db.Application
	err = s.pool.WithTx(r.Context(), userID, uuid.Nil, func(ctx context.Context, conn db.Conn) error {
		orgID, err := db.NewApplicationRepository(conn).OrgID(ctx, appID)
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
		if err := apps.UpdateReplicas(ctx, appID, *req.ReplicasDesired); err != nil {
			return mapDBError(err, "no application with this id in an organization you belong to", "")
		}
		app, err = apps.Get(ctx, appID)
		return mapDBError(err, "no application with this id in an organization you belong to", "")
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toApplicationResponse(app))
}

// handleDeleteApplication is DELETE /v1/applications/:appId. Per the
// exit-criteria script (phase-1-mvp.md), deleting an application must
// actually stop and remove whatever is running for it. As of Task 6
// (phase-3-controllers.md), that no longer means a single hardcoded
// WORKER_ADDR call — each replica may be on a different node, so this
// publishes a node.<id>.unassign (Task 2) per container, addressed to that
// container's actual node_id, before the DB delete commits; a publish
// failure aborts the deletion rather than leaving an orphaned container.
func (s *Server) handleDeleteApplication(w http.ResponseWriter, r *http.Request) {
	userID, _ := auth.UserIDFromContext(r.Context())
	appID, err := uuid.Parse(r.PathValue("appId"))
	if err != nil {
		s.writeError(w, r, errBadRequest("appId must be a valid UUID"))
		return
	}
	ctx := r.Context()

	var (
		orgID      uuid.UUID
		containers []db.Container
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
			// latest.WorkerContainerID is a Phase 1 relic Task 6's
			// event-driven deploy path no longer populates — the containers
			// table (written by the scheduler once it places the
			// deployment) is now the source of truth, and may hold more
			// than one row now that applications can scale (Task 5).
			containers, err = db.NewContainerRepository(conn).ListByDeployment(ctx, latest.ID)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}

	// PublishDurable is JetStream-backed (docs/nats-contract.md: a lost
	// unassign leaves a container running that the control plane thinks is
	// gone), so a successful publish here is as strong a guarantee as the
	// old synchronous worker call was — treated as terminal on failure, same
	// as handleDeploy treats a failed placement.requested publish.
	for _, cn := range containers {
		if cn.ContainerRuntimeID == nil {
			continue
		}
		data, err := json.Marshal(unassignMessage{AssignmentID: cn.ID.String(), ContainerID: *cn.ContainerRuntimeID})
		if err != nil {
			s.logger.Error("marshaling unassign message", "container_id", cn.ID, "error", err)
			s.writeError(w, r, fmt.Errorf("marshaling unassign request: %w", err))
			return
		}
		var publishErr error
		if s.bus == nil {
			publishErr = errors.New("event bus not connected")
		} else {
			publishErr = s.bus.PublishDurable(ctx, eventbus.NodeUnassignSubject(cn.NodeID.String()), data)
		}
		if publishErr != nil {
			s.logger.Error("publishing unassign", "container_id", cn.ID, "node_id", cn.NodeID, "error", publishErr)
			s.writeError(w, r, &apiError{http.StatusBadGateway, "runtime_error", "could not stop the running deployment; application was not deleted"})
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

package server

import (
	"context"
	"encoding/base64"
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

// containerSummaryDTO is one row of a deployment's replica summary — see
// deploymentResponse.Containers.
type containerSummaryDTO struct {
	NodeID string `json:"node_id"`
	Status string `json:"status"`
}

// serviceDTO surfaces an application's load-balancer routing identity
// (phase-4-service-discovery-lb.md Task 6): not a resolvable hostname yet
// (Phase 5's job — custom domains/DNS), but the exact value Task 4's load
// balancer's X-Platform-Service routing header expects, so a caller with
// this value and the load balancer's own address can actually reach a
// healthy instance.
type serviceDTO struct {
	DNSName string `json:"dns_name"`
}

type deploymentResponse struct {
	ID            string `json:"id"`
	ApplicationID string `json:"application_id"`
	Image         string `json:"image"`
	Revision      int    `json:"revision"`
	Status        string `json:"status"`
	Strategy      string `json:"strategy"`
	// WorkerContainerID is a Phase 1 relic — handleDeploy no longer calls
	// the worker synchronously (Task 6: event-driven placement), so this is
	// always nil going forward.
	WorkerContainerID *string `json:"worker_container_id,omitempty"`
	// ReplicasDesired/ReplicasRunning/Containers (populated via a join
	// through containers — see attachReplicaState) are Task 7's replacement
	// for Phase 2's single NodeID/ContainerStatus pair, which assumed
	// exactly one container per deployment — no longer true once an
	// application can scale (Task 5) or a crashed replica gets replaced
	// (Task 4) alongside still-healthy ones.
	ReplicasDesired int                   `json:"replicas_desired"`
	ReplicasRunning int                   `json:"replicas_running"`
	Containers      []containerSummaryDTO `json:"containers,omitempty"`
	// Service is nil until Task 3's controller has lazily created one for
	// this application (on its first-ever healthy instance) — a freshly
	// deployed application with no running replicas yet has none. Same
	// application-level value repeated on every row, same precedent as
	// ReplicasDesired above (attachReplicaState).
	Service     *serviceDTO `json:"service,omitempty"`
	CreatedAt   time.Time   `json:"created_at"`
	CompletedAt *time.Time  `json:"completed_at,omitempty"`
}

func toDeploymentResponse(d db.Deployment) deploymentResponse {
	return deploymentResponse{
		ID: d.ID.String(), ApplicationID: d.ApplicationID.String(), Image: d.Image,
		Revision: d.Revision, Status: string(d.Status), Strategy: d.Strategy,
		WorkerContainerID: d.WorkerContainerID, CreatedAt: d.CreatedAt, CompletedAt: d.CompletedAt,
	}
}

// attachReplicaState fills in resp's replica summary — desiredReplicas
// (applications.replicas_desired) against every containers row currently
// recorded for deploymentID, regardless of how many there are (Task 7,
// phase-3-controllers.md: "show a replica summary ... instead of assuming
// one container per deployment").
func attachReplicaState(ctx context.Context, containers db.ContainerRepository, deploymentID uuid.UUID, desiredReplicas int, resp *deploymentResponse) error {
	rows, err := containers.ListByDeployment(ctx, deploymentID)
	if err != nil {
		return err
	}
	resp.ReplicasDesired = desiredReplicas
	summaries := make([]containerSummaryDTO, len(rows))
	for i, cn := range rows {
		summaries[i] = containerSummaryDTO{NodeID: cn.NodeID.String(), Status: string(cn.Status)}
		if cn.Status == db.ContainerStatusRunning {
			resp.ReplicasRunning++
		}
	}
	resp.Containers = summaries
	return nil
}

// deployRequest is POST /v1/applications/:appId/deployments' body. Image
// and GitURL are the two mutually exclusive input modes
// (phase-7-deployment-platform.md Task 4): an already-pushed image, or a
// Git repo the platform builds into one itself. Neither being set is not
// an error — that's the pre-existing "redeploy the application's current
// image" path (see handleDeploy), which predates the git mode and stays
// exactly as it was.
type deployRequest struct {
	Image  string `json:"image,omitempty"`
	GitURL string `json:"git_url,omitempty"`
	GitRef string `json:"git_ref,omitempty"`
}

// createDeploymentTx writes the desired state half of a deploy: bump the
// application's image if this deploy changes it, create the deployments
// row, and audit it. Extracted (phase-7-deployment-platform.md Task 4) so
// the two paths that produce a deployment — the HTTP image path
// (handleDeploy) and the build.completed consumer's succeeded branch
// (handleBuildCompleted) — run one sequence rather than two hand-copied
// ones. Runs inside the caller's transaction, alongside whatever quota
// check that caller already did, so the check and the write stay atomic
// (the TOCTOU gap Layer 1 exists to close). app is updated in place when
// image differs from it, so the caller's later publishPlacement sees the
// image actually deployed.
func createDeploymentTx(ctx context.Context, conn db.Conn, orgID, appID, actorID uuid.UUID, app *db.Application, image string) (db.Deployment, error) {
	if image != "" && image != app.Image {
		if err := db.NewApplicationRepository(conn).UpdateImage(ctx, appID, image); err != nil {
			return db.Deployment{}, err
		}
		app.Image = image
	}

	deployment, err := db.NewDeploymentRepository(conn).Create(ctx, orgID, appID, app.Image, actorID)
	if err != nil {
		return db.Deployment{}, mapDBError(err, "", "a deployment for this revision already exists")
	}
	if err := recordAudit(ctx, conn, orgID, actorID, "deployment.create", "deployment", deployment.ID, map[string]any{
		"application_id": appID.String(), "image": deployment.Image, "revision": deployment.Revision,
	}); err != nil {
		return db.Deployment{}, err
	}
	return deployment, nil
}

// publishPlacement publishes placement.requested for a just-created
// deployment — the other half of createDeploymentTx, and the other half
// shared by both deploy paths.
//
// Publishing is a network call outside any DB transaction (ADR-0012 —
// never held across a cross-service call). A publish failure is a real,
// terminal failure of this deploy attempt (nothing will ever place it), so
// it's the one case that gets written back via UpdateResult; a successful
// publish leaves the row exactly as Create left it ('pending') — the
// scheduler and worker don't report back onto deployments at all, only
// onto containers (docs/nats-contract.md), so attachReplicaState's join
// through containers is the actual source of live placement status from
// here on, not this row.
func (s *Server) publishPlacement(ctx context.Context, orgID, actorID uuid.UUID, app db.Application, deployment db.Deployment) error {
	ports := make([]portBinding, len(app.Ports))
	for i, p := range app.Ports {
		ports[i] = portBinding{ContainerPort: p.ContainerPort, HostPort: p.HostPort, Protocol: p.Protocol}
	}
	msgData, err := json.Marshal(placementRequestedMessage{
		DeploymentID:  deployment.ID.String(),
		ApplicationID: app.ID.String(),
		Image:         deployment.Image,
		Ports:         ports,
	})
	if err != nil {
		s.logger.Error("marshaling placement.requested", "deployment_id", deployment.ID, "error", err)
		return fmt.Errorf("marshaling placement request: %w", err)
	}

	var publishErr error
	if s.bus == nil {
		publishErr = errors.New("event bus not connected")
	} else {
		publishErr = s.bus.PublishDurable(ctx, eventbus.PlacementRequestedSubject, msgData)
	}
	if publishErr == nil {
		return nil
	}

	s.logger.Error("publishing placement.requested", "deployment_id", deployment.ID, "error", publishErr)
	if updateErr := s.pool.WithTx(ctx, actorID, orgID, func(ctx context.Context, conn db.Conn) error {
		return db.NewDeploymentRepository(conn).UpdateResult(ctx, deployment.ID, db.DeploymentStatusFailed, nil)
	}); updateErr != nil {
		s.logger.Error("recording deployment result", "deployment_id", deployment.ID, "error", updateErr)
	}
	return publishErr
}

// handleDeploy is POST /v1/applications/:appId/deployments. As of Task 6
// (phase-2-multi-node.md), it no longer drives a worker directly: it writes
// the deployments row (desired state, recreate-only, single replica) and
// publishes placement.requested, matching ARCHITECTURE.md §2.1 ("the API
// server does not talk to workers directly"). Actual placement is the
// scheduler's job (Task 5); poll GET .../deployments to observe it land, via
// attachReplicaState's join through containers.
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
			s.writeError(w, r, errBadRequest("request body must be valid JSON with an optional image or git_url"))
			return
		}
	}

	// The two input modes are mutually exclusive (Task 4): asking the
	// platform to build an image *and* naming one to deploy has no coherent
	// meaning. Setting neither is not rejected — it's the pre-existing
	// redeploy-current-image path this route has always had.
	if req.Image != "" && req.GitURL != "" {
		s.writeError(w, r, errBadRequest("image and git_url are mutually exclusive — pass one or neither"))
		return
	}
	if req.GitURL != "" {
		s.handleGitDeploy(w, r, appID, userID, req)
		return
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

		// Layer 1 of the three-layer quota scheme
		// (phase-6-multi-tenant-saas.md Task 6) — see quota.go's own doc
		// comment. A deploy alone never changes replicas_desired, so
		// checkResourceQuota's meaningful check here is containers (a
		// first-ever deploy is what actually brings an application's
		// containers into existence); its CPU/memory arm is a defensive
		// no-op re-check today (see that function's own doc comment).
		if err := checkDeploymentQuota(ctx, conn, orgID); err != nil {
			return err
		}
		if err := checkResourceQuota(ctx, conn, orgID, app, app.ReplicasDesired); err != nil {
			return err
		}

		deployment, err = createDeploymentTx(ctx, conn, orgID, appID, userID, &app, req.Image)
		return err
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}

	if err := s.publishPlacement(ctx, orgID, userID, app, deployment); err != nil {
		s.writeError(w, r, &apiError{http.StatusBadGateway, "deploy_failed", fmt.Sprintf("failed to publish placement request: %v", err)})
		return
	}

	// No containers exist for this deployment yet (placement is async —
	// the scheduler hasn't acted on the publish above yet), so the replica
	// summary starts at 0/desired with no containers listed.
	resp := toDeploymentResponse(deployment)
	resp.ReplicasDesired = app.ReplicasDesired
	writeJSON(w, http.StatusCreated, resp)
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
	var data []deploymentResponse
	err = s.pool.WithTx(r.Context(), userID, uuid.Nil, func(ctx context.Context, conn db.Conn) error {
		apps := db.NewApplicationRepository(conn)
		orgID, err := apps.OrgID(ctx, appID)
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
		// replicas_desired lives on the application, not any one deployment
		// revision — every row in this list is compared against the
		// application's *current* desired count (Task 7), same as the
		// Deployment controller (Task 4) only ever reconciles the latest
		// deployment against it.
		app, err := apps.Get(ctx, appID)
		if err != nil {
			return mapDBError(err, "no application with this id in an organization you belong to", "")
		}
		deployments, err = db.NewDeploymentRepository(conn).ListByApplication(ctx, appID, limit, afterRevision)
		if err != nil {
			return err
		}

		// services carries no RLS (database-schema.md — cluster runtime
		// state, not tenant-scoped, same as containers/nodes below), so this
		// works over the same RLS-scoped connection the rest of this
		// handler already uses, no bypass needed. ErrNotFound just means
		// Task 3's controller hasn't lazily created one yet — not this
		// request's error.
		var service *serviceDTO
		svc, err := db.NewServiceRepository(conn).GetByApplication(ctx, appID)
		switch {
		case err == nil:
			service = &serviceDTO{DNSName: svc.DNSName}
		case errors.Is(err, db.ErrNotFound):
			// No service yet; service stays nil.
		default:
			return err
		}

		containers := db.NewContainerRepository(conn)
		data = make([]deploymentResponse, len(deployments))
		for i, d := range deployments {
			data[i] = toDeploymentResponse(d)
			if err := attachReplicaState(ctx, containers, d.ID, app.ReplicasDesired, &data[i]); err != nil {
				return err
			}
			data[i].Service = service
		}
		return nil
	})
	if err != nil {
		s.writeError(w, r, err)
		return
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

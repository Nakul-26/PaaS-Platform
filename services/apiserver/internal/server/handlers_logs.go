package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/google/uuid"

	"platform/internal/auth"
	"platform/internal/db"
	"platform/services/apiserver/internal/workerclient"
)

// handleGetLogs is GET /v1/applications/:appId/logs?follow=true|false
// (phase-1-mvp.md Task 7) — reads directly from the worker agent's
// StreamLogs, proxied through here; not fanned out via NATS, that's Phase
// 4+ once there are multiple consumers of log data.
//
// s.worker is still the one hardcoded worker address from Phase 1
// (WORKER_ADDR) — this only resolves correctly while there's a single
// worker node, which is still true today (phase-2-multi-node.md Task 8, not
// yet done, is what brings up multiple). Routing this to whichever node
// actually holds the container is deferred, not this task's job.
func (s *Server) handleGetLogs(w http.ResponseWriter, r *http.Request) {
	userID, _ := auth.UserIDFromContext(r.Context())
	appID, err := uuid.Parse(r.PathValue("appId"))
	if err != nil {
		s.writeError(w, r, errBadRequest("appId must be a valid UUID"))
		return
	}
	follow := r.URL.Query().Get("follow") == "true"

	var containerID string
	err = s.pool.WithTx(r.Context(), userID, uuid.Nil, func(ctx context.Context, conn db.Conn) error {
		orgID, err := db.NewApplicationRepository(conn).OrgID(ctx, appID)
		if err != nil {
			return mapDBError(err, "no application with this id in an organization you belong to", "")
		}
		if err := db.SetCurrentOrg(ctx, conn, orgID); err != nil {
			return err
		}
		// Any org member may view logs (rbac-multitenancy.md §2: logs/metrics
		// viewing is available to every role, including viewer).
		if _, err := requireMembership(ctx, conn, userID, orgID); err != nil {
			return err
		}

		deployment, err := db.NewDeploymentRepository(conn).Latest(ctx, appID)
		if err != nil {
			return mapDBError(err, "this application has never been deployed", "")
		}

		// deployment.WorkerContainerID is a Phase 1 relic that Task 6's
		// event-driven deploy path no longer populates — the containers
		// table (written by the scheduler once it places the deployment) is
		// now the source of truth for which container backs it.
		containers, err := db.NewContainerRepository(conn).ListByDeployment(ctx, deployment.ID)
		if err != nil {
			return err
		}
		if len(containers) == 0 || containers[0].ContainerRuntimeID == nil {
			return errNotFound("this application's latest deployment has no running container to read logs from")
		}
		containerID = *containers[0].ContainerRuntimeID
		return nil
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}

	// Streaming from the worker is a network call outside any DB transaction
	// (ADR-0012), same as handleDeploy.
	logs, err := s.worker.StreamLogs(r.Context(), containerID, follow)
	if err != nil {
		if errors.Is(err, workerclient.ErrNotFound) {
			s.writeError(w, r, errNotFound("the deployed container no longer exists"))
			return
		}
		s.writeError(w, r, &apiError{http.StatusBadGateway, "runtime_error", fmt.Sprintf("streaming logs: %v", err)})
		return
	}
	defer func() { _ = logs.Close() }()

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)

	flusher, canFlush := w.(http.Flusher)
	buf := make([]byte, 4096)
	for {
		n, readErr := logs.Read(buf)
		if n > 0 {
			if _, writeErr := w.Write(buf[:n]); writeErr != nil {
				return
			}
			if canFlush {
				flusher.Flush()
			}
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				s.logger.Error("streaming logs", "application_id", appID, "error", readErr)
			}
			return
		}
	}
}

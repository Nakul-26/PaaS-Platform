package server

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"

	"platform/internal/auth"
	"platform/internal/db"
)

// Task 3 (phase-6-multi-tenant-saas.md): the first routes with orgId
// directly in the URL besides handleCreateProject — org_id needs no
// resolution step, so it's passed straight to pool.WithTx exactly like that
// handler does. Every requirePermission/requireMembership error here is run
// through mapDBError: a non-member's db.ErrNotFound must become this
// package's 404, never the unwrapped sentinel (which s.writeError would
// otherwise fall through to reporting as a 500 — a latent gap in
// handleCreateProject's identical org-in-URL shape, fixed alongside this).

type organizationResponse struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Slug      string    `json:"slug"`
	CreatedAt time.Time `json:"created_at"`
}

func toOrganizationResponse(o db.Organization) organizationResponse {
	return organizationResponse{ID: o.ID.String(), Name: o.Name, Slug: o.Slug, CreatedAt: o.CreatedAt}
}

// handleGetOrganization is GET /v1/orgs/:orgId. Any member may view their
// org's settings (rbac-multitenancy.md §2 has no separate
// organization.view row, same precedent as domains/deployments listing) —
// requireMembership only confirms membership, not organization.manage.
func (s *Server) handleGetOrganization(w http.ResponseWriter, r *http.Request) {
	userID, _ := auth.UserIDFromContext(r.Context())
	orgID, err := uuid.Parse(r.PathValue("orgId"))
	if err != nil {
		s.writeError(w, r, errBadRequest("orgId must be a valid UUID"))
		return
	}

	var org db.Organization
	err = s.pool.WithTx(r.Context(), userID, orgID, func(ctx context.Context, conn db.Conn) error {
		if _, err := requireMembership(ctx, conn, userID, orgID); err != nil {
			return mapDBError(err, "no organization with this id that you belong to", "")
		}
		var err error
		org, err = db.NewOrganizationRepository(conn).Get(ctx, orgID)
		return mapDBError(err, "no organization with this id that you belong to", "")
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toOrganizationResponse(org))
}

type updateOrganizationRequest struct {
	Name string `json:"name"`
}

// handleUpdateOrganization is PATCH /v1/orgs/:orgId {name}, gated by
// organization.manage (owner/admin per the matrix).
func (s *Server) handleUpdateOrganization(w http.ResponseWriter, r *http.Request) {
	userID, _ := auth.UserIDFromContext(r.Context())
	orgID, err := uuid.Parse(r.PathValue("orgId"))
	if err != nil {
		s.writeError(w, r, errBadRequest("orgId must be a valid UUID"))
		return
	}

	var req updateOrganizationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, r, errBadRequest("request body must be valid JSON with name"))
		return
	}
	if req.Name == "" {
		s.writeError(w, r, errUnprocessable("name is required"))
		return
	}

	var org db.Organization
	err = s.pool.WithTx(r.Context(), userID, orgID, func(ctx context.Context, conn db.Conn) error {
		if err := requirePermission(ctx, conn, userID, orgID, auth.PermOrganizationManage); err != nil {
			return mapDBError(err, "no organization with this id that you belong to", "")
		}
		var err error
		org, err = db.NewOrganizationRepository(conn).Update(ctx, orgID, req.Name)
		if err != nil {
			return mapDBError(err, "no organization with this id that you belong to", "")
		}
		return recordAudit(ctx, conn, orgID, userID, "organization.update", "organization", orgID, map[string]any{"name": org.Name})
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toOrganizationResponse(org))
}

// handleDeleteOrganization is DELETE /v1/orgs/:orgId, gated by
// organization.delete (owner only per the matrix — the most destructive
// route in the API surface so far). See OrganizationRepository.Delete's own
// doc comment for the known gap this leaves: DB rows cascade cleanly, but
// nothing stops the org's actually-running containers on their nodes first.
//
// Deliberately does not call recordAudit (Task 7,
// phase-6-multi-tenant-saas.md): audit_logs.org_id references organizations
// ON DELETE CASCADE (0011_audit_logs.sql), so a row recorded here would be
// erased by this very statement, in this very transaction — writing one
// would silently produce nothing durable, which is worse than not writing
// it at all. A real "org was deleted" audit trail needs audit_logs
// decoupled from that FK (e.g. a nullable org_id with ON DELETE SET NULL,
// or no FK at all) — out of this task's scope, same category as the
// container-cleanup gap already flagged above.
func (s *Server) handleDeleteOrganization(w http.ResponseWriter, r *http.Request) {
	userID, _ := auth.UserIDFromContext(r.Context())
	orgID, err := uuid.Parse(r.PathValue("orgId"))
	if err != nil {
		s.writeError(w, r, errBadRequest("orgId must be a valid UUID"))
		return
	}

	err = s.pool.WithTx(r.Context(), userID, orgID, func(ctx context.Context, conn db.Conn) error {
		if err := requirePermission(ctx, conn, userID, orgID, auth.PermOrganizationDelete); err != nil {
			return mapDBError(err, "no organization with this id that you belong to", "")
		}
		return mapDBError(db.NewOrganizationRepository(conn).Delete(ctx, orgID), "no organization with this id that you belong to", "")
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

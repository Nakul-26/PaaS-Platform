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
)

// Task 4 (phase-6-multi-tenant-saas.md): member management. Every route has
// orgId directly in the URL — same no-resolution-step shape as the org
// routes (handlers_organizations.go), so every requirePermission/
// requireMembership error here is likewise run through mapDBError.

type membershipResponse struct {
	ID        string    `json:"id"`
	OrgID     string    `json:"org_id"`
	UserID    string    `json:"user_id"`
	Email     string    `json:"email,omitempty"`
	Role      string    `json:"role"`
	CreatedAt time.Time `json:"created_at"`
}

func toMembershipResponse(m db.Membership, email string) membershipResponse {
	return membershipResponse{
		ID: m.ID.String(), OrgID: m.OrgID.String(), UserID: m.UserID.String(),
		Email: email, Role: string(m.Role), CreatedAt: m.CreatedAt,
	}
}

// parseMembershipRole rejects any string that isn't one of the four fixed
// roles (rbac-multitenancy.md §1) — invite/change-role requests carry a
// caller-supplied role string that must be validated the same way any
// other enum-shaped input is, before it ever reaches a query.
func parseMembershipRole(s string) (db.MembershipRole, bool) {
	switch role := db.MembershipRole(s); role {
	case db.MembershipRoleOwner, db.MembershipRoleAdmin, db.MembershipRoleDeveloper, db.MembershipRoleViewer:
		return role, true
	default:
		return "", false
	}
}

// handleListMembers is GET /v1/orgs/:orgId/members — requireMembership
// only (no dedicated members.view row in the matrix), any role may call it.
// See MembershipRepository.ListByOrg's own doc comment for what a caller
// actually gets back: the RLS policy itself, not this handler, is what
// gives an owner/admin the full roster while a developer/viewer sees only
// their own row.
func (s *Server) handleListMembers(w http.ResponseWriter, r *http.Request) {
	userID, _ := auth.UserIDFromContext(r.Context())
	orgID, err := uuid.Parse(r.PathValue("orgId"))
	if err != nil {
		s.writeError(w, r, errBadRequest("orgId must be a valid UUID"))
		return
	}

	var members []db.MembershipWithUser
	err = s.pool.WithTx(r.Context(), userID, orgID, func(ctx context.Context, conn db.Conn) error {
		if _, err := requireMembership(ctx, conn, userID, orgID); err != nil {
			return mapDBError(err, "no organization with this id that you belong to", "")
		}
		var err error
		members, err = db.NewMembershipRepository(conn).ListByOrg(ctx, orgID)
		return err
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	data := make([]membershipResponse, len(members))
	for i, m := range members {
		data[i] = toMembershipResponse(m.Membership, m.Email)
	}
	writeJSON(w, http.StatusOK, struct {
		Data []membershipResponse `json:"data"`
	}{Data: data})
}

type inviteMemberRequest struct {
	Email string `json:"email"`
	Role  string `json:"role"`
}

// handleInviteMember is POST /v1/orgs/:orgId/members {email, role}, gated
// by members.invite (owner/admin per the matrix). Open decision 3
// (phase-6-multi-tenant-saas.md): only works for an already-registered
// user, looked up by email — there is no email-sending/invite-token
// infrastructure anywhere in this codebase. An unregistered email gets a
// clear 404 telling the caller that person needs to sign up first, never a
// silent no-op.
func (s *Server) handleInviteMember(w http.ResponseWriter, r *http.Request) {
	userID, _ := auth.UserIDFromContext(r.Context())
	orgID, err := uuid.Parse(r.PathValue("orgId"))
	if err != nil {
		s.writeError(w, r, errBadRequest("orgId must be a valid UUID"))
		return
	}

	var req inviteMemberRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, r, errBadRequest("request body must be valid JSON with email and role"))
		return
	}
	if req.Email == "" {
		s.writeError(w, r, errUnprocessable("email is required"))
		return
	}
	role, ok := parseMembershipRole(req.Role)
	if !ok {
		s.writeError(w, r, errUnprocessable("role must be one of owner, admin, developer, viewer"))
		return
	}

	var (
		membership db.Membership
		email      string
	)
	err = s.pool.WithTx(r.Context(), userID, orgID, func(ctx context.Context, conn db.Conn) error {
		if err := requirePermission(ctx, conn, userID, orgID, auth.PermMembersInvite); err != nil {
			return mapDBError(err, "no organization with this id that you belong to", "")
		}

		target, err := db.NewUserRepository(conn).GetByEmail(ctx, req.Email)
		if err != nil {
			if errors.Is(err, db.ErrNotFound) {
				return errNotFound("no account exists with this email — they must sign up first")
			}
			return err
		}
		email = target.Email

		membership, err = db.NewMembershipRepository(conn).Create(ctx, orgID, target.ID, role)
		if err != nil {
			return mapDBError(err, "", "this user is already a member of this organization")
		}
		return recordAudit(ctx, conn, orgID, userID, "member.invite", "membership", target.ID, map[string]any{"email": email, "role": string(role)})
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, toMembershipResponse(membership, email))
}

type changeMemberRoleRequest struct {
	Role string `json:"role"`
}

// handleChangeMemberRole is PATCH /v1/orgs/:orgId/members/:userId {role},
// gated by members.change_role (owner/admin per the matrix), plus the
// last-owner guard: the matrix says an owner *can* demote another owner,
// but never down to zero owners org-wide (phase-6-multi-tenant-saas.md
// Task 4 — an application-layer check, not a matrix permission).
func (s *Server) handleChangeMemberRole(w http.ResponseWriter, r *http.Request) {
	userID, _ := auth.UserIDFromContext(r.Context())
	orgID, err := uuid.Parse(r.PathValue("orgId"))
	if err != nil {
		s.writeError(w, r, errBadRequest("orgId must be a valid UUID"))
		return
	}
	targetUserID, err := uuid.Parse(r.PathValue("userId"))
	if err != nil {
		s.writeError(w, r, errBadRequest("userId must be a valid UUID"))
		return
	}

	var req changeMemberRoleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, r, errBadRequest("request body must be valid JSON with role"))
		return
	}
	newRole, ok := parseMembershipRole(req.Role)
	if !ok {
		s.writeError(w, r, errUnprocessable("role must be one of owner, admin, developer, viewer"))
		return
	}

	var membership db.Membership
	err = s.pool.WithTx(r.Context(), userID, orgID, func(ctx context.Context, conn db.Conn) error {
		if err := requirePermission(ctx, conn, userID, orgID, auth.PermMembersChangeRole); err != nil {
			return mapDBError(err, "no organization with this id that you belong to", "")
		}

		memberships := db.NewMembershipRepository(conn)
		targetRole, err := memberships.RoleForUser(ctx, targetUserID, orgID)
		if err != nil {
			return mapDBError(err, "no member with this id in this organization", "")
		}
		if targetRole == db.MembershipRoleOwner && newRole != db.MembershipRoleOwner {
			owners, err := memberships.CountOwners(ctx, orgID)
			if err != nil {
				return err
			}
			if owners <= 1 {
				return errLastOwner("cannot demote the organization's only remaining owner")
			}
		}

		membership, err = memberships.UpdateRole(ctx, orgID, targetUserID, newRole)
		if err != nil {
			return mapDBError(err, "no member with this id in this organization", "")
		}
		return recordAudit(ctx, conn, orgID, userID, "member.change_role", "membership", targetUserID, map[string]any{"role_from": string(targetRole), "role_to": string(newRole)})
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toMembershipResponse(membership, ""))
}

// handleRemoveMember is DELETE /v1/orgs/:orgId/members/:userId, gated by
// members.remove (owner/admin per the matrix), plus the same last-owner
// guard as handleChangeMemberRole.
func (s *Server) handleRemoveMember(w http.ResponseWriter, r *http.Request) {
	userID, _ := auth.UserIDFromContext(r.Context())
	orgID, err := uuid.Parse(r.PathValue("orgId"))
	if err != nil {
		s.writeError(w, r, errBadRequest("orgId must be a valid UUID"))
		return
	}
	targetUserID, err := uuid.Parse(r.PathValue("userId"))
	if err != nil {
		s.writeError(w, r, errBadRequest("userId must be a valid UUID"))
		return
	}

	err = s.pool.WithTx(r.Context(), userID, orgID, func(ctx context.Context, conn db.Conn) error {
		if err := requirePermission(ctx, conn, userID, orgID, auth.PermMembersRemove); err != nil {
			return mapDBError(err, "no organization with this id that you belong to", "")
		}

		memberships := db.NewMembershipRepository(conn)
		targetRole, err := memberships.RoleForUser(ctx, targetUserID, orgID)
		if err != nil {
			return mapDBError(err, "no member with this id in this organization", "")
		}
		if targetRole == db.MembershipRoleOwner {
			owners, err := memberships.CountOwners(ctx, orgID)
			if err != nil {
				return err
			}
			if owners <= 1 {
				return errLastOwner("cannot remove the organization's only remaining owner")
			}
		}

		if err := memberships.Delete(ctx, orgID, targetUserID); err != nil {
			return mapDBError(err, "no member with this id in this organization", "")
		}
		return recordAudit(ctx, conn, orgID, userID, "member.remove", "membership", targetUserID, map[string]any{"role": string(targetRole)})
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

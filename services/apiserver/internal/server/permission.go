package server

import (
	"context"
	"regexp"
	"strings"

	"github.com/google/uuid"

	"platform/internal/auth"
	"platform/internal/db"
)

// requirePermission is the single shared authorization check
// (rbac-multitenancy.md §3.1): resolve the caller's role in orgID and
// assert perm. conn must already have app.current_user_id set (it always
// does — see db.Pool.WithTx). A caller with no membership in orgID gets the
// same error as a caller who fails the permission check on their role
// having the wrong shape confirmed 404 — the distinction between "not a
// member" and "wrong role" is handled by the caller mapping the returned
// error (db.ErrNotFound vs. this function's own forbidden sentinel).
func requirePermission(ctx context.Context, conn db.Conn, userID, orgID uuid.UUID, perm auth.Permission) error {
	role, err := db.NewMembershipRepository(conn).RoleForUser(ctx, userID, orgID)
	if err != nil {
		return err
	}
	if !auth.HasPermission(auth.Role(role), perm) {
		return errForbidden("your role does not permit this action")
	}
	return nil
}

// requireMembership confirms userID belongs to orgID without checking any
// specific permission — for read-only routes every role can access
// (rbac-multitenancy.md §2: logs/metrics viewing is available to viewer
// too, unlike every mutating action).
func requireMembership(ctx context.Context, conn db.Conn, userID, orgID uuid.UUID) (auth.Role, error) {
	role, err := db.NewMembershipRepository(conn).RoleForUser(ctx, userID, orgID)
	if err != nil {
		return "", err
	}
	return auth.Role(role), nil
}

var slugSanitizer = regexp.MustCompile(`[^a-z0-9]+`)

// slugify derives a URL-safe slug from a display name. Not a general-
// purpose transliterator — good enough for Phase 1's ASCII names; a
// dedicated slug package is not warranted until non-ASCII names are a real
// requirement (no speculative generality).
func slugify(name string) string {
	s := slugSanitizer.ReplaceAllString(strings.ToLower(strings.TrimSpace(name)), "-")
	return strings.Trim(s, "-")
}

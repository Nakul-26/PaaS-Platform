package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// MembershipRole mirrors docs/rbac-multitenancy.md §1's four roles. Kept as
// its own type here (rather than importing internal/auth.Role) so this
// package stays independent of the auth package — the apiserver, which
// depends on both, converts between them at its boundary.
type MembershipRole string

const (
	MembershipRoleOwner     MembershipRole = "owner"
	MembershipRoleAdmin     MembershipRole = "admin"
	MembershipRoleDeveloper MembershipRole = "developer"
	MembershipRoleViewer    MembershipRole = "viewer"
)

type Membership struct {
	ID        uuid.UUID
	OrgID     uuid.UUID
	UserID    uuid.UUID
	Role      MembershipRole
	CreatedAt time.Time
}

// MembershipWithUser pairs a Membership with the member's email — the shape
// GET /v1/orgs/:orgId/members actually needs (phase-6-multi-tenant-saas.md
// Task 4): a bare user_id isn't human-identifiable in a member-listing
// UI/CLI table.
type MembershipWithUser struct {
	Membership
	Email string
}

// MembershipRepository is the port apiserver depends on (ADR-0011). Every
// method requires app.current_user_id to already be set on conn's
// transaction (db.SetCurrentUser) — the memberships RLS policy's "own
// membership" branch keys off exactly that. ListByOrg/UpdateRole/Delete
// additionally rely on that same policy's other branch (is_org_admin,
// 0001_organizations_users_memberships.sql) to see/affect rows beyond the
// caller's own — they're only ever called after requirePermission has
// already confirmed the caller is an owner/admin (members.invite/remove/
// change_role, rbac-multitenancy.md §2), which is exactly the set
// is_org_admin admits.
type MembershipRepository interface {
	Create(ctx context.Context, orgID, userID uuid.UUID, role MembershipRole) (Membership, error)
	// RoleForUser returns the caller's role in orgID, or ErrNotFound if
	// they aren't a member — the same "don't leak existence" signal used
	// throughout (rbac-multitenancy.md §5).
	RoleForUser(ctx context.Context, userID, orgID uuid.UUID) (MembershipRole, error)
	// ListForUser returns every org userID belongs to — used at
	// signup/login to tell the caller which org(s) they can act in (Phase
	// 1: always exactly one, the default org created at signup).
	ListForUser(ctx context.Context, userID uuid.UUID) ([]Membership, error)
	// ListByOrg returns every member of orgID. Run over the caller's
	// ordinary RLS-scoped session (not an admin-bypass connection): per the
	// memberships_isolation policy an owner/admin caller sees the full
	// roster, while a developer/viewer caller sees only their own row — a
	// deliberate consequence of that already-shipped policy (its own doc
	// comment: "needed for member-management UI, Phase 6"), not a bug this
	// task works around. handleListMembers only gates on requireMembership
	// (any role may call the route, per the matrix's lack of a dedicated
	// members.view row) — RLS itself is what narrows what a non-admin
	// caller actually sees back.
	ListByOrg(ctx context.Context, orgID uuid.UUID) ([]MembershipWithUser, error)
	// UpdateRole changes an existing member's role. Returns ErrNotFound if
	// userID isn't a member of orgID.
	UpdateRole(ctx context.Context, orgID, userID uuid.UUID, role MembershipRole) (Membership, error)
	// Delete removes userID's membership in orgID. Returns ErrNotFound if
	// they weren't a member.
	Delete(ctx context.Context, orgID, userID uuid.UUID) error
	// CountOwners returns how many owner-role members orgID currently has —
	// the last-owner guard (phase-6-multi-tenant-saas.md Task 4) checks
	// this before allowing a remove/demote to proceed, so an org can never
	// end up with zero owners.
	CountOwners(ctx context.Context, orgID uuid.UUID) (int, error)
}

type membershipRepository struct{ conn Conn }

func NewMembershipRepository(conn Conn) MembershipRepository {
	return &membershipRepository{conn: conn}
}

func (r *membershipRepository) Create(ctx context.Context, orgID, userID uuid.UUID, role MembershipRole) (Membership, error) {
	var m Membership
	err := r.conn.QueryRow(ctx,
		`INSERT INTO memberships (org_id, user_id, role) VALUES ($1, $2, $3)
		 RETURNING id, org_id, user_id, role, created_at`,
		orgID, userID, role,
	).Scan(&m.ID, &m.OrgID, &m.UserID, &m.Role, &m.CreatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return Membership{}, ErrConflict
		}
		return Membership{}, fmt.Errorf("creating membership: %w", err)
	}
	return m, nil
}

func (r *membershipRepository) RoleForUser(ctx context.Context, userID, orgID uuid.UUID) (MembershipRole, error) {
	var role MembershipRole
	err := r.conn.QueryRow(ctx,
		`SELECT role FROM memberships WHERE user_id = $1 AND org_id = $2`,
		userID, orgID,
	).Scan(&role)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrNotFound
		}
		return "", fmt.Errorf("fetching membership role: %w", err)
	}
	return role, nil
}

func (r *membershipRepository) ListForUser(ctx context.Context, userID uuid.UUID) ([]Membership, error) {
	rows, err := r.conn.Query(ctx,
		`SELECT id, org_id, user_id, role, created_at FROM memberships WHERE user_id = $1`,
		userID,
	)
	if err != nil {
		return nil, fmt.Errorf("listing memberships: %w", err)
	}
	defer rows.Close()

	var memberships []Membership
	for rows.Next() {
		var m Membership
		if err := rows.Scan(&m.ID, &m.OrgID, &m.UserID, &m.Role, &m.CreatedAt); err != nil {
			return nil, fmt.Errorf("scanning membership: %w", err)
		}
		memberships = append(memberships, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating memberships: %w", err)
	}
	return memberships, nil
}

func (r *membershipRepository) ListByOrg(ctx context.Context, orgID uuid.UUID) ([]MembershipWithUser, error) {
	rows, err := r.conn.Query(ctx,
		`SELECT m.id, m.org_id, m.user_id, m.role, m.created_at, u.email
		 FROM memberships m JOIN users u ON u.id = m.user_id
		 WHERE m.org_id = $1 ORDER BY m.created_at`,
		orgID,
	)
	if err != nil {
		return nil, fmt.Errorf("listing org members: %w", err)
	}
	defer rows.Close()

	var members []MembershipWithUser
	for rows.Next() {
		var m MembershipWithUser
		if err := rows.Scan(&m.ID, &m.OrgID, &m.UserID, &m.Role, &m.CreatedAt, &m.Email); err != nil {
			return nil, fmt.Errorf("scanning org member: %w", err)
		}
		members = append(members, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating org members: %w", err)
	}
	return members, nil
}

func (r *membershipRepository) UpdateRole(ctx context.Context, orgID, userID uuid.UUID, role MembershipRole) (Membership, error) {
	var m Membership
	err := r.conn.QueryRow(ctx,
		`UPDATE memberships SET role = $3 WHERE org_id = $1 AND user_id = $2
		 RETURNING id, org_id, user_id, role, created_at`,
		orgID, userID, role,
	).Scan(&m.ID, &m.OrgID, &m.UserID, &m.Role, &m.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Membership{}, ErrNotFound
		}
		return Membership{}, fmt.Errorf("updating membership role: %w", err)
	}
	return m, nil
}

func (r *membershipRepository) Delete(ctx context.Context, orgID, userID uuid.UUID) error {
	tag, err := r.conn.Exec(ctx, `DELETE FROM memberships WHERE org_id = $1 AND user_id = $2`, orgID, userID)
	if err != nil {
		return fmt.Errorf("deleting membership: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *membershipRepository) CountOwners(ctx context.Context, orgID uuid.UUID) (int, error) {
	var count int
	err := r.conn.QueryRow(ctx,
		`SELECT count(*) FROM memberships WHERE org_id = $1 AND role = 'owner'`,
		orgID,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("counting org owners: %w", err)
	}
	return count, nil
}

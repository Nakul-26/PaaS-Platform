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

// MembershipRepository is the port apiserver depends on (ADR-0011). Every
// method requires app.current_user_id to already be set on conn's
// transaction (db.SetCurrentUser) — the memberships RLS policy's "own
// membership" branch keys off exactly that.
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

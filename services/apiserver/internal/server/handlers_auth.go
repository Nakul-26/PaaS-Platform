package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"platform/internal/auth"
	"platform/internal/db"
)

type userResponse struct {
	ID    string `json:"id"`
	Email string `json:"email"`
}

type orgMembershipResponse struct {
	ID   string `json:"id"`
	Slug string `json:"slug"`
	Role string `json:"role"`
}

type authResponse struct {
	User                 userResponse          `json:"user"`
	Org                  orgMembershipResponse `json:"org"`
	AccessToken          string                `json:"access_token"`
	AccessTokenExpiresAt time.Time             `json:"access_token_expires_at"`
	RefreshToken         string                `json:"refresh_token"`
}

type signupRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

// handleSignup implements the Phase 1 default-org bootstrap
// (phase-1-mvp.md Task 5, ADR-0004): every new user gets exactly one
// organization, created in the same transaction as the user and their
// owner membership, so the three rows are never observably out of sync.
func (s *Server) handleSignup(w http.ResponseWriter, r *http.Request) {
	var req signupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, r, errBadRequest("request body must be valid JSON with email and password"))
		return
	}
	if req.Email == "" || req.Password == "" {
		s.writeError(w, r, errUnprocessable("email and password are required"))
		return
	}

	passwordHash, err := auth.HashPassword(req.Password)
	if err != nil {
		s.writeError(w, r, err)
		return
	}

	ctx := r.Context()
	orgName, orgSlug := defaultOrgFor(req.Email)

	var (
		user db.User
		org  db.Organization
	)
	err = s.pool.WithTx(ctx, uuid.Nil, uuid.Nil, func(ctx context.Context, conn db.Conn) error {
		var err error
		user, err = db.NewUserRepository(conn).Create(ctx, req.Email, passwordHash)
		if err != nil {
			return mapDBError(err, "", "an account with this email already exists")
		}

		org, err = db.NewOrganizationRepository(conn).Create(ctx, orgName, orgSlug)
		if err != nil {
			return err
		}

		// The memberships RLS policy's "own membership" branch needs
		// app.current_user_id set to the row being inserted — see
		// db.SetCurrentUser's doc comment.
		if err := db.SetCurrentUser(ctx, conn, user.ID); err != nil {
			return err
		}
		_, err = db.NewMembershipRepository(conn).Create(ctx, org.ID, user.ID, db.MembershipRoleOwner)
		return err
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}

	resp, err := s.issueAuthResponse(ctx, user, org.ID, org.Slug, string(db.MembershipRoleOwner))
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, resp)
}

type loginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req loginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, r, errBadRequest("request body must be valid JSON with email and password"))
		return
	}

	ctx := r.Context()
	user, err := db.NewUserRepository(s.pool.Conn()).GetByEmail(ctx, req.Email)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			s.writeError(w, r, errUnauthorized("invalid email or password"))
			return
		}
		s.writeError(w, r, err)
		return
	}

	ok, err := auth.VerifyPassword(user.PasswordHash, req.Password)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	if !ok {
		s.writeError(w, r, errUnauthorized("invalid email or password"))
		return
	}

	var memberships []db.Membership
	err = s.pool.WithTx(ctx, user.ID, uuid.Nil, func(ctx context.Context, conn db.Conn) error {
		var err error
		memberships, err = db.NewMembershipRepository(conn).ListForUser(ctx, user.ID)
		return err
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	if len(memberships) == 0 {
		s.writeError(w, r, errors.New("user has no organization membership"))
		return
	}
	primary := memberships[0]

	orgSlug, err := s.orgSlug(ctx, primary.OrgID)
	if err != nil {
		s.writeError(w, r, err)
		return
	}

	resp, err := s.issueAuthResponse(ctx, user, primary.OrgID, orgSlug, string(primary.Role))
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

type refreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

// handleRefresh implements rotating refresh tokens (ADR-0008): the
// presented token is revoked and a new one issued in the same call, so a
// second use of the same (now-old) token is detectable as reuse.
func (s *Server) handleRefresh(w http.ResponseWriter, r *http.Request) {
	var req refreshRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.RefreshToken == "" {
		s.writeError(w, r, errBadRequest("request body must be valid JSON with refresh_token"))
		return
	}

	ctx := r.Context()
	tokens := db.NewRefreshTokenRepository(s.pool.Conn())
	stored, err := tokens.GetByHash(ctx, auth.HashRefreshToken(req.RefreshToken))
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			s.writeError(w, r, errUnauthorized("refresh token is invalid"))
			return
		}
		s.writeError(w, r, err)
		return
	}
	if stored.RevokedAt != nil || time.Now().After(stored.ExpiresAt) {
		s.writeError(w, r, errUnauthorized("refresh token is invalid"))
		return
	}

	user, err := db.NewUserRepository(s.pool.Conn()).GetByID(ctx, stored.UserID)
	if err != nil {
		s.writeError(w, r, err)
		return
	}

	accessToken, expiresAt, err := s.issuer.IssueAccessToken(user.ID)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	newPlain, newHash, err := auth.GenerateRefreshToken()
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	newRecord, err := tokens.Create(ctx, user.ID, newHash, time.Now().Add(auth.RefreshTokenTTL))
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	if err := tokens.Rotate(ctx, stored.ID, newRecord.ID); err != nil {
		s.writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, struct {
		AccessToken          string    `json:"access_token"`
		AccessTokenExpiresAt time.Time `json:"access_token_expires_at"`
		RefreshToken         string    `json:"refresh_token"`
	}{accessToken, expiresAt, newPlain})
}

func (s *Server) issueAuthResponse(ctx context.Context, user db.User, orgID uuid.UUID, orgSlug, role string) (authResponse, error) {
	accessToken, expiresAt, err := s.issuer.IssueAccessToken(user.ID)
	if err != nil {
		return authResponse{}, err
	}
	plainRefresh, refreshHash, err := auth.GenerateRefreshToken()
	if err != nil {
		return authResponse{}, err
	}
	if _, err := db.NewRefreshTokenRepository(s.pool.Conn()).Create(ctx, user.ID, refreshHash, time.Now().Add(auth.RefreshTokenTTL)); err != nil {
		return authResponse{}, err
	}

	return authResponse{
		User:                 userResponse{ID: user.ID.String(), Email: user.Email},
		Org:                  orgMembershipResponse{ID: orgID.String(), Slug: orgSlug, Role: role},
		AccessToken:          accessToken,
		AccessTokenExpiresAt: expiresAt,
		RefreshToken:         plainRefresh,
	}, nil
}

func (s *Server) orgSlug(ctx context.Context, orgID uuid.UUID) (string, error) {
	var slug string
	err := s.pool.Conn().QueryRow(ctx, `SELECT slug FROM organizations WHERE id = $1`, orgID).Scan(&slug)
	return slug, err
}

// defaultOrgFor derives a default org name/slug from a signup email
// (ADR-0004: Phase 1 doesn't build org-creation UI, every user gets one
// default org). The slug gets a short random suffix since organizations.slug
// is globally unique and two people signing up with the same email local
// part must not collide.
func defaultOrgFor(email string) (name, slug string) {
	local, _, found := strings.Cut(email, "@")
	if !found || local == "" {
		local = "personal"
	}
	suffix := uuid.New().String()[:8]
	return local + "'s Organization", slugify(local) + "-" + suffix
}

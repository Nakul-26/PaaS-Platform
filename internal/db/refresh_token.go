package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type RefreshToken struct {
	ID           uuid.UUID
	UserID       uuid.UUID
	TokenHash    string
	CreatedAt    time.Time
	ExpiresAt    time.Time
	RevokedAt    *time.Time
	ReplacedByID *uuid.UUID
}

// RefreshTokenRepository is the port apiserver depends on (ADR-0011).
// refresh_tokens carries no RLS (see the migration's comment) so its
// methods work against any Conn.
type RefreshTokenRepository interface {
	Create(ctx context.Context, userID uuid.UUID, tokenHash string, expiresAt time.Time) (RefreshToken, error)
	// GetByHash returns ErrNotFound if no token matches — the caller
	// treats an unknown hash the same as any other invalid-refresh-token
	// case, never distinguishing "wrong" from "never existed".
	GetByHash(ctx context.Context, tokenHash string) (RefreshToken, error)
	// Rotate marks id as replaced by newID (ADR-0008: makes reuse of a
	// rotated-out token detectable).
	Rotate(ctx context.Context, id, newID uuid.UUID) error
	Revoke(ctx context.Context, id uuid.UUID) error
}

type refreshTokenRepository struct{ conn Conn }

func NewRefreshTokenRepository(conn Conn) RefreshTokenRepository {
	return &refreshTokenRepository{conn: conn}
}

// #nosec G101 -- a SQL column list, not a credential; gosec's heuristic
// flags the substring "token_hash".
const refreshTokenColumns = `id, user_id, token_hash, created_at, expires_at, revoked_at, replaced_by_id`

func scanRefreshToken(row pgx.Row) (RefreshToken, error) {
	var t RefreshToken
	err := row.Scan(&t.ID, &t.UserID, &t.TokenHash, &t.CreatedAt, &t.ExpiresAt, &t.RevokedAt, &t.ReplacedByID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RefreshToken{}, ErrNotFound
		}
		return RefreshToken{}, err
	}
	return t, nil
}

func (r *refreshTokenRepository) Create(ctx context.Context, userID uuid.UUID, tokenHash string, expiresAt time.Time) (RefreshToken, error) {
	t, err := scanRefreshToken(r.conn.QueryRow(ctx,
		`INSERT INTO refresh_tokens (user_id, token_hash, expires_at) VALUES ($1, $2, $3) RETURNING `+refreshTokenColumns,
		userID, tokenHash, expiresAt,
	))
	if err != nil {
		if isUniqueViolation(err) {
			return RefreshToken{}, ErrConflict
		}
		return RefreshToken{}, fmt.Errorf("creating refresh token: %w", err)
	}
	return t, nil
}

func (r *refreshTokenRepository) GetByHash(ctx context.Context, tokenHash string) (RefreshToken, error) {
	t, err := scanRefreshToken(r.conn.QueryRow(ctx,
		`SELECT `+refreshTokenColumns+` FROM refresh_tokens WHERE token_hash = $1`,
		tokenHash,
	))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return RefreshToken{}, ErrNotFound
		}
		return RefreshToken{}, fmt.Errorf("fetching refresh token: %w", err)
	}
	return t, nil
}

func (r *refreshTokenRepository) Rotate(ctx context.Context, id, newID uuid.UUID) error {
	tag, err := r.conn.Exec(ctx,
		`UPDATE refresh_tokens SET revoked_at = now(), replaced_by_id = $2 WHERE id = $1`,
		id, newID,
	)
	if err != nil {
		return fmt.Errorf("rotating refresh token: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *refreshTokenRepository) Revoke(ctx context.Context, id uuid.UUID) error {
	tag, err := r.conn.Exec(ctx, `UPDATE refresh_tokens SET revoked_at = now() WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("revoking refresh token: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

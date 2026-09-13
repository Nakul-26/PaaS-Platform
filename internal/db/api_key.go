package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// APIKey mirrors the api_keys table (0002_api_keys.sql). KeyHash is
// deliberately not a field here — the plaintext key is handed back exactly
// once, at creation (auth.GenerateAPIKey), and nothing past that point ever
// needs the hash again outside this repository's own queries.
type APIKey struct {
	ID        uuid.UUID
	OrgID     uuid.UUID
	Name      string
	Scopes    []string
	CreatedBy uuid.UUID
	CreatedAt time.Time
	RevokedAt *time.Time
}

// APIKeyRepository is the port apiserver depends on (ADR-0011). RLS-bound
// (api_keys_isolation, 0002_api_keys.sql) exactly like DomainRepository —
// Create/ListByOrg/OrgID/Revoke all run over the request's ordinary
// RLS-scoped connection, after the caller's org/permission has already been
// confirmed. LookupActiveByHash is the one exception: it's the auth path
// itself (auth.Authenticate calling this before any identity is known), so
// it goes through the lookup_active_api_key SECURITY DEFINER function
// (0012_api_key_lookup_function.sql) that intentionally bypasses RLS for
// exactly that one lookup — see that migration's own comment.
type APIKeyRepository interface {
	Create(ctx context.Context, orgID uuid.UUID, name string, scopes []string, createdBy uuid.UUID, keyHash string) (APIKey, error)
	// ListByOrg returns every key ever created for orgID, revoked or not —
	// a revoked key stays visible (with RevokedAt set) so the list doubles
	// as a lightweight audit trail, rather than silently disappearing.
	ListByOrg(ctx context.Context, orgID uuid.UUID) ([]APIKey, error)
	// OrgID resolves id's owning org, RLS-scoped only by app.current_user_id
	// — see ProjectRepository.OrgID for why: the deep-by-ID
	// DELETE /v1/api-keys/:keyId route needs org_id before it can even call
	// requirePermission.
	OrgID(ctx context.Context, id uuid.UUID) (uuid.UUID, error)
	// Revoke sets revoked_at on an active key. Returns ErrNotFound for an
	// unknown id or a key that's already revoked — re-revoking isn't a
	// meaningfully different outcome from "no such active key" here.
	Revoke(ctx context.Context, id uuid.UUID) error
	// LookupActiveByHash finds a non-revoked key by its hash — the
	// Authenticate path's own lookup, run before any app.current_user_id/
	// current_org_id is known (that's what this call determines). Returns
	// ErrNotFound for an unknown or already-revoked hash, deliberately not
	// distinguished (rbac-multitenancy.md §5's "don't leak existence"
	// applies to a guessed/leaked key exactly like any other resource id).
	LookupActiveByHash(ctx context.Context, hash string) (APIKey, error)
}

type apiKeyRepository struct{ conn Conn }

func NewAPIKeyRepository(conn Conn) APIKeyRepository {
	return &apiKeyRepository{conn: conn}
}

func (r *apiKeyRepository) Create(ctx context.Context, orgID uuid.UUID, name string, scopes []string, createdBy uuid.UUID, keyHash string) (APIKey, error) {
	// A nil scopes (no scopes given at creation) would otherwise bind as SQL
	// NULL, not "use the column's default" — api_keys.scopes is NOT NULL
	// DEFAULT '{}', so an explicit nil parameter fails that constraint
	// instead of falling through to the default. Coerce to an empty slice
	// so "no scopes" round-trips as '{}', not an error.
	if scopes == nil {
		scopes = []string{}
	}

	var k APIKey
	err := r.conn.QueryRow(ctx,
		`INSERT INTO api_keys (org_id, name, key_hash, scopes, created_by)
		 VALUES ($1, $2, $3, $4, $5)
		 RETURNING id, org_id, name, scopes, created_by, created_at, revoked_at`,
		orgID, name, keyHash, scopes, createdBy,
	).Scan(&k.ID, &k.OrgID, &k.Name, &k.Scopes, &k.CreatedBy, &k.CreatedAt, &k.RevokedAt)
	if err != nil {
		return APIKey{}, fmt.Errorf("creating api key: %w", err)
	}
	return k, nil
}

func (r *apiKeyRepository) ListByOrg(ctx context.Context, orgID uuid.UUID) ([]APIKey, error) {
	rows, err := r.conn.Query(ctx,
		`SELECT id, org_id, name, scopes, created_by, created_at, revoked_at
		 FROM api_keys WHERE org_id = $1 ORDER BY created_at`,
		orgID,
	)
	if err != nil {
		return nil, fmt.Errorf("listing api keys: %w", err)
	}
	defer rows.Close()

	var keys []APIKey
	for rows.Next() {
		var k APIKey
		if err := rows.Scan(&k.ID, &k.OrgID, &k.Name, &k.Scopes, &k.CreatedBy, &k.CreatedAt, &k.RevokedAt); err != nil {
			return nil, fmt.Errorf("scanning api key: %w", err)
		}
		keys = append(keys, k)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating api keys: %w", err)
	}
	return keys, nil
}

func (r *apiKeyRepository) OrgID(ctx context.Context, id uuid.UUID) (uuid.UUID, error) {
	var orgID uuid.UUID
	err := r.conn.QueryRow(ctx, `SELECT org_id FROM api_keys WHERE id = $1`, id).Scan(&orgID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return uuid.Nil, ErrNotFound
		}
		return uuid.Nil, fmt.Errorf("resolving api key org: %w", err)
	}
	return orgID, nil
}

func (r *apiKeyRepository) Revoke(ctx context.Context, id uuid.UUID) error {
	tag, err := r.conn.Exec(ctx, `UPDATE api_keys SET revoked_at = now() WHERE id = $1 AND revoked_at IS NULL`, id)
	if err != nil {
		return fmt.Errorf("revoking api key: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *apiKeyRepository) LookupActiveByHash(ctx context.Context, hash string) (APIKey, error) {
	var k APIKey
	err := r.conn.QueryRow(ctx,
		`SELECT id, org_id, name, scopes, created_by, created_at FROM lookup_active_api_key($1)`,
		hash,
	).Scan(&k.ID, &k.OrgID, &k.Name, &k.Scopes, &k.CreatedBy, &k.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return APIKey{}, ErrNotFound
		}
		return APIKey{}, fmt.Errorf("looking up api key: %w", err)
	}
	return k, nil
}

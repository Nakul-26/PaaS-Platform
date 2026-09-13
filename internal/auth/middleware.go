package auth

import (
	"context"
	"net/http"
	"strings"

	"github.com/google/uuid"
)

type contextKey int

const (
	userIDContextKey contextKey = iota
	apiKeyOrgIDContextKey
)

// WithUserID returns a context carrying the authenticated caller's user id.
func WithUserID(ctx context.Context, userID uuid.UUID) context.Context {
	return context.WithValue(ctx, userIDContextKey, userID)
}

// UserIDFromContext returns the authenticated caller's user id, as set by
// the Authenticate middleware.
func UserIDFromContext(ctx context.Context) (uuid.UUID, bool) {
	userID, ok := ctx.Value(userIDContextKey).(uuid.UUID)
	return userID, ok
}

// WithAPIKeyOrgID marks ctx as authenticated via an API key scoped to
// orgID — set only on the pk_live_ auth path below, never for an ordinary
// JWT-authenticated request. requirePermission/requireMembership
// (services/apiserver) use APIKeyOrgIDFromContext to confirm a request
// never acts outside this one org, even though the key's creator (the user
// id WithUserID carries here) may belong to several
// (phase-6-multi-tenant-saas.md Task 5: "an API key is scoped to one org
// already, unlike a user's JWT which can belong to several").
func WithAPIKeyOrgID(ctx context.Context, orgID uuid.UUID) context.Context {
	return context.WithValue(ctx, apiKeyOrgIDContextKey, orgID)
}

// APIKeyOrgIDFromContext returns the org an API-key-authenticated request
// is scoped to, or (uuid.Nil, false) for an ordinary JWT-authenticated one.
func APIKeyOrgIDFromContext(ctx context.Context) (uuid.UUID, bool) {
	orgID, ok := ctx.Value(apiKeyOrgIDContextKey).(uuid.UUID)
	return orgID, ok
}

// APIKeyLookup is the port Authenticate uses to verify a pk_live_-prefixed
// bearer credential (ADR-0011) — kept independent of internal/db exactly
// like TokenIssuer keeps JWT concerns self-contained here; the apiserver
// wires internal/db's real APIKeyRepository.LookupActiveByHash into this at
// startup.
type APIKeyLookup interface {
	LookupActiveByHash(ctx context.Context, hash string) (userID, orgID uuid.UUID, err error)
}

// Authenticate verifies the Authorization header and, on success, injects
// the caller's identity into the request context before calling next. This
// only establishes *who* is calling (ADR-0008) — authorization (*what* they
// can do) is a separate, per-route check against Postgres membership data
// (docs/rbac-multitenancy.md §3), not performed here.
//
// Two credential shapes are accepted, distinguished by prefix
// (api-conventions.md §3): an ordinary JWT access token, verified via
// issuer, or a pk_live_-prefixed API key, looked up via keys
// (phase-6-multi-tenant-saas.md Task 5). keys may be nil — every caller
// that only ever needs JWT auth (existing tests in particular) keeps
// working unchanged; a pk_live_ credential presented against a nil keys is
// rejected the same as any other invalid token, not a panic.
func Authenticate(issuer *TokenIssuer, keys APIKeyLookup, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := r.Header.Get("Authorization")
		const prefix = "Bearer "
		if !strings.HasPrefix(header, prefix) {
			writeAuthError(w)
			return
		}
		token := strings.TrimPrefix(header, prefix)

		if strings.HasPrefix(token, apiKeyPrefix) {
			if keys == nil {
				writeAuthError(w)
				return
			}
			userID, orgID, err := keys.LookupActiveByHash(r.Context(), HashAPIKey(token))
			if err != nil {
				writeAuthError(w)
				return
			}
			ctx := WithAPIKeyOrgID(WithUserID(r.Context(), userID), orgID)
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		userID, err := issuer.VerifyAccessToken(token)
		if err != nil {
			writeAuthError(w)
			return
		}
		next.ServeHTTP(w, r.WithContext(WithUserID(r.Context(), userID)))
	})
}

func writeAuthError(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	_, _ = w.Write([]byte(`{"error":{"code":"unauthorized","message":"a valid bearer access token is required"}}`))
}

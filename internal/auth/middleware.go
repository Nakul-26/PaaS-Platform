package auth

import (
	"context"
	"net/http"
	"strings"

	"github.com/google/uuid"
)

type contextKey int

const userIDContextKey contextKey = iota

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

// Authenticate verifies the Authorization: Bearer <jwt> header and, on
// success, injects the caller's user id into the request context before
// calling next. This only establishes *who* is calling (ADR-0008) —
// authorization (*what* they can do) is a separate, per-route check against
// Postgres membership data (docs/rbac-multitenancy.md §3), not performed
// here.
func Authenticate(issuer *TokenIssuer, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := r.Header.Get("Authorization")
		const prefix = "Bearer "
		if !strings.HasPrefix(header, prefix) {
			writeAuthError(w)
			return
		}
		token := strings.TrimPrefix(header, prefix)
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

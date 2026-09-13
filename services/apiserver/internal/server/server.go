// Package server implements the platform's public REST API
// (docs/api-conventions.md, Phase 1 Task 5): auth, projects, applications,
// deployments. It depends on internal/auth (ADR-0008) and internal/db
// (ADR-0011) for its ports, and on the worker agent's HTTP contract
// (docs/worker-agent-contract.md) via workerclient for the one operational
// side effect Phase 1 has — actually running a deployed image.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/google/uuid"

	"platform/internal/auth"
	"platform/internal/db"
	"platform/internal/eventbus"
	"platform/services/apiserver/internal/workerclient"
)

// Server wires the API server's dependencies (ADR-0011 ports) into HTTP
// handlers. bus may be nil (phase-2-multi-node.md Task 6: apiserver keeps
// serving every other route even if NATS was unreachable at startup) — only
// handleDeploy depends on it, and checks for nil itself.
type Server struct {
	pool   *db.Pool
	issuer *auth.TokenIssuer
	worker *workerclient.Client
	bus    eventbus.EventBus
	logger *slog.Logger
	// registryAddr is the container registry git-built images are pushed to
	// (phase-7-deployment-platform.md Task 2/4) — used only to compose the
	// image repository named in a build.requested message, never dialed from
	// here. See WithImageRegistry.
	registryAddr string
}

// defaultImageRegistryAddr matches infrastructure/docker-compose.yml's
// local registry:2 service, so a Server built without WithImageRegistry
// (every test constructing one directly) still composes a usable image
// repository rather than an empty-prefixed one.
const defaultImageRegistryAddr = "localhost:5000"

func New(pool *db.Pool, issuer *auth.TokenIssuer, worker *workerclient.Client, bus eventbus.EventBus, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{pool: pool, issuer: issuer, worker: worker, bus: bus, logger: logger, registryAddr: defaultImageRegistryAddr}
}

// WithImageRegistry overrides the registry address git-built images are
// pushed to. Kept as an option rather than a sixth positional parameter on
// New: config ownership stays in main.go (which reads IMAGE_REGISTRY_ADDR),
// while the many call sites that don't care about it stay untouched.
func (s *Server) WithImageRegistry(addr string) *Server {
	if addr != "" {
		s.registryAddr = addr
	}
	return s
}

// Routes returns the API server's HTTP handler, ready to pass to
// http.Server. Route shapes follow docs/api-conventions.md §2 exactly.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /v1/auth/signup", s.handleSignup)
	mux.HandleFunc("POST /v1/auth/login", s.handleLogin)
	mux.HandleFunc("POST /v1/auth/refresh", s.handleRefresh)

	mux.Handle("GET /v1/orgs/{orgId}", s.authenticated(s.handleGetOrganization))
	mux.Handle("PATCH /v1/orgs/{orgId}", s.authenticated(s.handleUpdateOrganization))
	mux.Handle("DELETE /v1/orgs/{orgId}", s.authenticated(s.handleDeleteOrganization))
	mux.Handle("GET /v1/orgs/{orgId}/members", s.authenticated(s.handleListMembers))
	mux.Handle("POST /v1/orgs/{orgId}/members", s.authenticated(s.handleInviteMember))
	mux.Handle("PATCH /v1/orgs/{orgId}/members/{userId}", s.authenticated(s.handleChangeMemberRole))
	mux.Handle("DELETE /v1/orgs/{orgId}/members/{userId}", s.authenticated(s.handleRemoveMember))
	mux.Handle("POST /v1/orgs/{orgId}/api-keys", s.authenticated(s.handleCreateAPIKey))
	mux.Handle("GET /v1/orgs/{orgId}/api-keys", s.authenticated(s.handleListAPIKeys))
	mux.Handle("DELETE /v1/api-keys/{keyId}", s.authenticated(s.handleRevokeAPIKey))
	mux.Handle("GET /v1/orgs/{orgId}/quota", s.authenticated(s.handleGetQuota))
	mux.Handle("GET /v1/orgs/{orgId}/audit-logs", s.authenticated(s.handleGetAuditLogs))
	mux.Handle("POST /v1/orgs/{orgId}/projects", s.authenticated(s.handleCreateProject))
	mux.Handle("POST /v1/projects/{projectId}/applications", s.authenticated(s.handleCreateApplication))
	mux.Handle("POST /v1/applications/{appId}/deployments", s.authenticated(s.handleDeploy))
	mux.Handle("GET /v1/applications/{appId}/deployments", s.authenticated(s.handleGetDeployments))
	mux.Handle("GET /v1/applications/{appId}/builds", s.authenticated(s.handleGetBuilds))
	mux.Handle("GET /v1/applications/{appId}/logs", s.authenticated(s.handleGetLogs))
	mux.Handle("PATCH /v1/applications/{appId}", s.authenticated(s.handleScaleApplication))
	mux.Handle("DELETE /v1/applications/{appId}", s.authenticated(s.handleDeleteApplication))
	mux.Handle("GET /v1/nodes", s.authenticated(s.handleGetNodes))
	mux.Handle("POST /v1/projects/{projectId}/domains", s.authenticated(s.handleCreateDomain))
	mux.Handle("GET /v1/projects/{projectId}/domains", s.authenticated(s.handleListDomains))
	mux.Handle("DELETE /v1/domains/{domainId}", s.authenticated(s.handleDeleteDomain))

	return mux
}

func (s *Server) authenticated(h http.HandlerFunc) http.Handler {
	return auth.Authenticate(s.issuer, apiKeyLookupAdapter{pool: s.pool}, h)
}

// apiKeyLookupAdapter adapts db.APIKeyRepository to auth.APIKeyLookup
// (ADR-0011: internal/auth stays independent of internal/db, so it can't
// import db.APIKeyRepository directly — the apiserver is the one place
// that depends on both, so it owns the adapter). Runs over the pool's bare,
// non-transactional connection: LookupActiveByHash goes through a SECURITY
// DEFINER function that intentionally bypasses RLS itself (see
// APIKeyRepository's own doc comment), so no app.current_user_id/
// current_org_id needs to be set first — there's no identity yet to set
// them from, that's exactly what this call determines.
type apiKeyLookupAdapter struct{ pool *db.Pool }

func (a apiKeyLookupAdapter) LookupActiveByHash(ctx context.Context, hash string) (userID, orgID uuid.UUID, err error) {
	key, err := db.NewAPIKeyRepository(a.pool.Conn()).LookupActiveByHash(ctx, hash)
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	return key.CreatedBy, key.OrgID, nil
}

// --- shared error envelope (api-conventions.md §4) ---

type apiError struct {
	status  int
	code    string
	message string
}

func (e *apiError) Error() string { return e.message }

func errNotFound(message string) error  { return &apiError{http.StatusNotFound, "not_found", message} }
func errForbidden(message string) error { return &apiError{http.StatusForbidden, "forbidden", message} }
func errConflict(message string) error  { return &apiError{http.StatusConflict, "conflict", message} }
func errBadRequest(message string) error {
	return &apiError{http.StatusBadRequest, "invalid_body", message}
}
func errUnprocessable(message string) error {
	return &apiError{http.StatusUnprocessableEntity, "missing_field", message}
}
func errUnauthorized(message string) error {
	return &apiError{http.StatusUnauthorized, "unauthorized", message}
}

// errLastOwner backs the last-owner guard (phase-6-multi-tenant-saas.md
// Task 4) — 422 with its own code, same "status carries the primary
// signal, code disambiguates within it" convention api-conventions.md §4
// uses for quota_exceeded.
func errLastOwner(message string) error {
	return &apiError{http.StatusUnprocessableEntity, "last_owner", message}
}

// errQuotaExceeded backs Layer 1 of the three-layer quota scheme
// (ARCHITECTURE.md §2.9, phase-6-multi-tenant-saas.md Task 6) — 422 with
// its own code, same "status carries the primary signal, code
// disambiguates" convention as errLastOwner, and the literal worked example
// api-conventions.md §4 already uses.
func errQuotaExceeded(message string) error {
	return &apiError{http.StatusUnprocessableEntity, "quota_exceeded", message}
}

// mapDBError translates a db package sentinel error into the apiError a
// given call site means by it — db.ErrNotFound in particular is
// deliberately ambiguous (resource missing vs. RLS-filtered-for-a-
// non-member, rbac-multitenancy.md §5) and every call site means "404" by
// it, never "403".
func mapDBError(err error, notFoundMessage, conflictMessage string) error {
	switch {
	case errors.Is(err, db.ErrNotFound):
		return errNotFound(notFoundMessage)
	case errors.Is(err, db.ErrConflict):
		return errConflict(conflictMessage)
	default:
		return err
	}
}

type errorResponse struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

func (s *Server) writeError(w http.ResponseWriter, r *http.Request, err error) {
	if apiErr, ok := err.(*apiError); ok {
		writeJSON(w, apiErr.status, errorResponse{Error: errorBody{Code: apiErr.code, Message: apiErr.message}})
		return
	}
	s.logger.Error("unhandled error", "method", r.Method, "path", r.URL.Path, "error", err)
	writeJSON(w, http.StatusInternalServerError, errorResponse{
		Error: errorBody{Code: "internal_error", Message: "an unexpected error occurred"},
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

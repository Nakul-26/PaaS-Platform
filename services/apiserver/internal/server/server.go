// Package server implements the platform's public REST API
// (docs/api-conventions.md, Phase 1 Task 5): auth, projects, applications,
// deployments. It depends on internal/auth (ADR-0008) and internal/db
// (ADR-0011) for its ports, and on the worker agent's HTTP contract
// (docs/worker-agent-contract.md) via workerclient for the one operational
// side effect Phase 1 has — actually running a deployed image.
package server

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

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
}

func New(pool *db.Pool, issuer *auth.TokenIssuer, worker *workerclient.Client, bus eventbus.EventBus, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{pool: pool, issuer: issuer, worker: worker, bus: bus, logger: logger}
}

// Routes returns the API server's HTTP handler, ready to pass to
// http.Server. Route shapes follow docs/api-conventions.md §2 exactly.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /v1/auth/signup", s.handleSignup)
	mux.HandleFunc("POST /v1/auth/login", s.handleLogin)
	mux.HandleFunc("POST /v1/auth/refresh", s.handleRefresh)

	mux.Handle("POST /v1/orgs/{orgId}/projects", s.authenticated(s.handleCreateProject))
	mux.Handle("POST /v1/projects/{projectId}/applications", s.authenticated(s.handleCreateApplication))
	mux.Handle("POST /v1/applications/{appId}/deployments", s.authenticated(s.handleDeploy))
	mux.Handle("GET /v1/applications/{appId}/deployments", s.authenticated(s.handleGetDeployments))
	mux.Handle("GET /v1/applications/{appId}/logs", s.authenticated(s.handleGetLogs))
	mux.Handle("PATCH /v1/applications/{appId}", s.authenticated(s.handleScaleApplication))
	mux.Handle("DELETE /v1/applications/{appId}", s.authenticated(s.handleDeleteApplication))
	mux.Handle("GET /v1/nodes", s.authenticated(s.handleGetNodes))

	return mux
}

func (s *Server) authenticated(h http.HandlerFunc) http.Handler {
	return auth.Authenticate(s.issuer, h)
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

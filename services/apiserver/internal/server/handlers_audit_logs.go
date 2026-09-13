package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"platform/internal/auth"
	"platform/internal/db"
)

// auditLogResponse mirrors db.AuditLogEntry (Task 7). ActorUserID is
// omitted (nil) for a system-initiated action — none exists yet, but the
// column stays nullable per database-schema.md.
type auditLogResponse struct {
	ID          string          `json:"id"`
	OrgID       string          `json:"org_id"`
	ActorUserID *string         `json:"actor_user_id,omitempty"`
	Action      string          `json:"action"`
	TargetType  string          `json:"target_type"`
	TargetID    string          `json:"target_id"`
	Metadata    json.RawMessage `json:"metadata"`
	CreatedAt   time.Time       `json:"created_at"`
}

func toAuditLogResponse(e db.AuditLogEntry) auditLogResponse {
	resp := auditLogResponse{
		ID: e.ID.String(), OrgID: e.OrgID.String(), Action: e.Action,
		TargetType: e.TargetType, TargetID: e.TargetID.String(),
		Metadata: e.Metadata, CreatedAt: e.CreatedAt,
	}
	if e.ActorUserID != nil {
		id := e.ActorUserID.String()
		resp.ActorUserID = &id
	}
	return resp
}

// handleGetAuditLogs is GET /v1/orgs/:orgId/audit-logs?limit=&cursor=
// (Task 7), gated by audit_logs.view (owner/admin per the matrix).
// Cursor-paginated exactly like handleGetDeployments — same
// (data, next_cursor) response shape and the same limit clamp — since
// AuditLogRepository.ListByOrg already returns an opaque cursor in that
// shape (api-conventions.md §4's own worked example is this exact route).
func (s *Server) handleGetAuditLogs(w http.ResponseWriter, r *http.Request) {
	userID, _ := auth.UserIDFromContext(r.Context())
	orgID, err := uuid.Parse(r.PathValue("orgId"))
	if err != nil {
		s.writeError(w, r, errBadRequest("orgId must be a valid UUID"))
		return
	}

	limit := 50
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 && n <= 200 {
			limit = n
		}
	}
	cursor := r.URL.Query().Get("cursor")

	var (
		entries    []db.AuditLogEntry
		nextCursor string
	)
	err = s.pool.WithTx(r.Context(), userID, orgID, func(ctx context.Context, conn db.Conn) error {
		if err := requirePermission(ctx, conn, userID, orgID, auth.PermAuditLogsView); err != nil {
			return mapDBError(err, "no organization with this id that you belong to", "")
		}
		var err error
		entries, nextCursor, err = db.NewAuditLogRepository(conn).ListByOrg(ctx, orgID, cursor, limit)
		return err
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	data := make([]auditLogResponse, len(entries))
	for i, e := range entries {
		data[i] = toAuditLogResponse(e)
	}
	var respCursor *string
	if nextCursor != "" {
		respCursor = &nextCursor
	}
	writeJSON(w, http.StatusOK, struct {
		Data       []auditLogResponse `json:"data"`
		NextCursor *string            `json:"next_cursor"`
	}{Data: data, NextCursor: respCursor})
}

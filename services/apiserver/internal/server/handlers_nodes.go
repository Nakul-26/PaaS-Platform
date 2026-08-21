package server

import (
	"context"
	"net/http"
	"time"

	"github.com/google/uuid"

	"platform/internal/auth"
	"platform/internal/db"
)

type nodeResponse struct {
	ID              string    `json:"id"`
	Hostname        string    `json:"hostname"`
	Status          string    `json:"status"`
	LastHeartbeatAt time.Time `json:"last_heartbeat_at"`
}

// handleGetNodes is GET /v1/nodes (phase-2-multi-node.md Task 6). nodes
// carries no RLS (it's infrastructure, not tenant-scoped —
// database-schema.md's `nodes` entry), so visibility is gated by
// requirePlatformAdmin instead of an org membership check.
func (s *Server) handleGetNodes(w http.ResponseWriter, r *http.Request) {
	userID, _ := auth.UserIDFromContext(r.Context())

	var nodes []db.Node
	err := s.pool.WithTx(r.Context(), userID, uuid.Nil, func(ctx context.Context, conn db.Conn) error {
		if err := requirePlatformAdmin(ctx, conn, userID); err != nil {
			return err
		}
		var err error
		nodes, err = db.NewNodeRepository(conn).List(ctx)
		return err
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}

	data := make([]nodeResponse, len(nodes))
	for i, n := range nodes {
		data[i] = nodeResponse{ID: n.ID.String(), Hostname: n.Hostname, Status: string(n.Status), LastHeartbeatAt: n.LastHeartbeatAt}
	}
	writeJSON(w, http.StatusOK, struct {
		Data []nodeResponse `json:"data"`
	}{Data: data})
}

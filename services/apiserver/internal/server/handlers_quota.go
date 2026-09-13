package server

import (
	"context"
	"net/http"

	"github.com/google/uuid"

	"platform/internal/auth"
	"platform/internal/db"
)

// quotaResponse pairs an org's ceilings with its live usage against each
// one — the shape `platform get quota` needs to show both sides of the
// same number (Task 6).
type quotaResponse struct {
	OrgID                string `json:"org_id"`
	MaxCPUMillicores     int    `json:"max_cpu_millicores"`
	MaxMemoryMB          int    `json:"max_memory_mb"`
	MaxContainers        int    `json:"max_containers"`
	MaxProjects          int    `json:"max_projects"`
	MaxDeploymentsPerDay int    `json:"max_deployments_per_day"`
	UsedCPUMillicores    int    `json:"used_cpu_millicores"`
	UsedMemoryMB         int    `json:"used_memory_mb"`
	UsedContainers       int    `json:"used_containers"`
	UsedProjects         int    `json:"used_projects"`
	UsedDeploymentsToday int    `json:"used_deployments_today"`
}

func toQuotaResponse(q db.ResourceQuota, u db.ResourceUsage) quotaResponse {
	return quotaResponse{
		OrgID:                q.OrgID.String(),
		MaxCPUMillicores:     q.MaxCPUMillicores,
		MaxMemoryMB:          q.MaxMemoryMB,
		MaxContainers:        q.MaxContainers,
		MaxProjects:          q.MaxProjects,
		MaxDeploymentsPerDay: q.MaxDeploymentsPerDay,
		UsedCPUMillicores:    u.CPUMillicores,
		UsedMemoryMB:         u.MemoryMB,
		UsedContainers:       u.Containers,
		UsedProjects:         u.Projects,
		UsedDeploymentsToday: u.DeploymentsToday,
	}
}

// handleGetQuota is GET /v1/orgs/:orgId/quota (Task 6) — membership-only,
// same precedent as GET /v1/orgs/:orgId and GET .../members: the matrix has
// no dedicated quota.view row, so any role may check current usage against
// the org's ceilings. CLI-only surface (`platform get quota`) — there is no
// corresponding write route; quota limits are platform-operator-set, not
// self-service (open decision in the phase doc).
func (s *Server) handleGetQuota(w http.ResponseWriter, r *http.Request) {
	userID, _ := auth.UserIDFromContext(r.Context())
	orgID, err := uuid.Parse(r.PathValue("orgId"))
	if err != nil {
		s.writeError(w, r, errBadRequest("orgId must be a valid UUID"))
		return
	}

	var (
		quota db.ResourceQuota
		usage db.ResourceUsage
	)
	err = s.pool.WithTx(r.Context(), userID, orgID, func(ctx context.Context, conn db.Conn) error {
		if _, err := requireMembership(ctx, conn, userID, orgID); err != nil {
			return mapDBError(err, "no organization with this id that you belong to", "")
		}
		quotas := db.NewQuotaRepository(conn)
		var err error
		quota, err = quotas.Get(ctx, orgID)
		if err != nil {
			return mapDBError(err, "no organization with this id that you belong to", "")
		}
		usage, err = quotas.CurrentUsage(ctx, orgID)
		return err
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toQuotaResponse(quota, usage))
}

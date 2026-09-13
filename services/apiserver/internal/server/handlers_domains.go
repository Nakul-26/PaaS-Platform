package server

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"

	"platform/internal/auth"
	"platform/internal/db"
)

// Task 2 (phase-5-networking-ingress.md): wires the already-specified
// domains.manage permission (rbac-multitenancy.md §2) to real routes for
// the first time. domains is tenant-authored config exactly like
// applications/projects (RLS-bound, database-schema.md §3), so every
// handler here runs over the request's ordinary RLS-scoped connection —
// never DomainRoutingRepository's admin one, which exists solely for the
// load balancer's own cross-tenant resync (Task 4).

type domainResponse struct {
	ID            string    `json:"id"`
	OrgID         string    `json:"org_id"`
	ProjectID     string    `json:"project_id"`
	ApplicationID string    `json:"application_id"`
	Hostname      string    `json:"hostname"`
	TLSStatus     string    `json:"tls_status"`
	CreatedAt     time.Time `json:"created_at"`
}

func toDomainResponse(d db.Domain) domainResponse {
	return domainResponse{
		ID: d.ID.String(), OrgID: d.OrgID.String(), ProjectID: d.ProjectID.String(),
		ApplicationID: d.ApplicationID.String(), Hostname: d.Hostname, TLSStatus: string(d.TLSStatus), CreatedAt: d.CreatedAt,
	}
}

type createDomainRequest struct {
	Hostname      string `json:"hostname"`
	ApplicationID string `json:"application_id"`
}

// handleCreateDomain is POST /v1/projects/:projectId/domains — a deep
// by-ID route (api-conventions.md §2), same shape as
// handleCreateApplication: projectId is in the URL, so org_id is resolved
// from the project first. application_id is caller-supplied (a project can
// have more than one application), so it additionally confirms the
// referenced application actually belongs to this project — the domains
// table's own FK only guarantees the application exists at all, not that
// it's this project's, and a mismatch here would otherwise silently create
// a domain whose application_id points outside project_id.
func (s *Server) handleCreateDomain(w http.ResponseWriter, r *http.Request) {
	userID, _ := auth.UserIDFromContext(r.Context())
	projectID, err := uuid.Parse(r.PathValue("projectId"))
	if err != nil {
		s.writeError(w, r, errBadRequest("projectId must be a valid UUID"))
		return
	}

	var req createDomainRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, r, errBadRequest("request body must be valid JSON with hostname and application_id"))
		return
	}
	if req.Hostname == "" {
		s.writeError(w, r, errUnprocessable("hostname is required"))
		return
	}
	applicationID, err := uuid.Parse(req.ApplicationID)
	if err != nil {
		s.writeError(w, r, errUnprocessable("application_id must be a valid UUID"))
		return
	}

	var domain db.Domain
	err = s.pool.WithTx(r.Context(), userID, uuid.Nil, func(ctx context.Context, conn db.Conn) error {
		orgID, err := db.NewProjectRepository(conn).OrgID(ctx, projectID)
		if err != nil {
			return mapDBError(err, "no project with this id in an organization you belong to", "")
		}
		if err := db.SetCurrentOrg(ctx, conn, orgID); err != nil {
			return err
		}
		if err := requirePermission(ctx, conn, userID, orgID, auth.PermDomainsManage); err != nil {
			return err
		}

		app, err := db.NewApplicationRepository(conn).Get(ctx, applicationID)
		if err != nil {
			return mapDBError(err, "no application with this id in this project", "")
		}
		if app.ProjectID != projectID {
			return errNotFound("no application with this id in this project")
		}

		domains := db.NewDomainRepository(conn)
		domain, err = domains.Create(ctx, orgID, projectID, applicationID, req.Hostname)
		if err != nil {
			return mapDBError(err, "", "a domain with this hostname already exists")
		}
		// tls_status set to active synchronously here, not left at Create's
		// 'pending' default (open decision 3,
		// phase-5-networking-ingress.md): Task 5's self-signed adapter
		// generates certs deterministically and on-demand in the load
		// balancer itself, so there's no real provisioning latency for a
		// 'pending' state to actually model yet.
		if err := domains.UpdateTLSStatus(ctx, domain.ID, db.DomainTLSStatusActive); err != nil {
			return err
		}
		domain.TLSStatus = db.DomainTLSStatusActive
		return recordAudit(ctx, conn, orgID, userID, "domain.create", "domain", domain.ID, map[string]any{"hostname": domain.Hostname, "application_id": applicationID.String()})
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, toDomainResponse(domain))
}

// handleListDomains is GET /v1/projects/:projectId/domains. Any org member
// may view domains (rbac-multitenancy.md §2 has no separate read
// permission for domains, same as deployments/logs) — requireMembership
// only confirms membership, not domains.manage specifically.
func (s *Server) handleListDomains(w http.ResponseWriter, r *http.Request) {
	userID, _ := auth.UserIDFromContext(r.Context())
	projectID, err := uuid.Parse(r.PathValue("projectId"))
	if err != nil {
		s.writeError(w, r, errBadRequest("projectId must be a valid UUID"))
		return
	}

	var domains []db.Domain
	err = s.pool.WithTx(r.Context(), userID, uuid.Nil, func(ctx context.Context, conn db.Conn) error {
		orgID, err := db.NewProjectRepository(conn).OrgID(ctx, projectID)
		if err != nil {
			return mapDBError(err, "no project with this id in an organization you belong to", "")
		}
		if err := db.SetCurrentOrg(ctx, conn, orgID); err != nil {
			return err
		}
		if _, err := requireMembership(ctx, conn, userID, orgID); err != nil {
			return err
		}
		domains, err = db.NewDomainRepository(conn).ListByProject(ctx, projectID)
		return err
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	data := make([]domainResponse, len(domains))
	for i, d := range domains {
		data[i] = toDomainResponse(d)
	}
	writeJSON(w, http.StatusOK, struct {
		Data []domainResponse `json:"data"`
	}{Data: data})
}

// handleDeleteDomain is DELETE /v1/domains/:domainId — a deep by-ID route
// with no projectId in the URL, so org_id is resolved from the domain
// itself first, same pattern as handleDeleteApplication.
func (s *Server) handleDeleteDomain(w http.ResponseWriter, r *http.Request) {
	userID, _ := auth.UserIDFromContext(r.Context())
	domainID, err := uuid.Parse(r.PathValue("domainId"))
	if err != nil {
		s.writeError(w, r, errBadRequest("domainId must be a valid UUID"))
		return
	}

	err = s.pool.WithTx(r.Context(), userID, uuid.Nil, func(ctx context.Context, conn db.Conn) error {
		domains := db.NewDomainRepository(conn)
		orgID, err := domains.OrgID(ctx, domainID)
		if err != nil {
			return mapDBError(err, "no domain with this id in an organization you belong to", "")
		}
		if err := db.SetCurrentOrg(ctx, conn, orgID); err != nil {
			return err
		}
		if err := requirePermission(ctx, conn, userID, orgID, auth.PermDomainsManage); err != nil {
			return err
		}
		if err := domains.Delete(ctx, domainID); err != nil {
			return mapDBError(err, "no domain with this id in an organization you belong to", "")
		}
		return recordAudit(ctx, conn, orgID, userID, "domain.delete", "domain", domainID, nil)
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

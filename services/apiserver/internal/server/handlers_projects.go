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

type projectResponse struct {
	ID        string    `json:"id"`
	OrgID     string    `json:"org_id"`
	Name      string    `json:"name"`
	Slug      string    `json:"slug"`
	CreatedAt time.Time `json:"created_at"`
}

func toProjectResponse(p db.Project) projectResponse {
	return projectResponse{ID: p.ID.String(), OrgID: p.OrgID.String(), Name: p.Name, Slug: p.Slug, CreatedAt: p.CreatedAt}
}

type createProjectRequest struct {
	Name string `json:"name"`
	Slug string `json:"slug,omitempty"`
}

// handleCreateProject is POST /v1/orgs/:orgId/projects (api-conventions.md
// §2) — orgId is already known from the URL, so this route never needs the
// deep-by-ID resolution path (see permission.go / database-schema.md §3).
func (s *Server) handleCreateProject(w http.ResponseWriter, r *http.Request) {
	userID, _ := auth.UserIDFromContext(r.Context())
	orgID, err := uuid.Parse(r.PathValue("orgId"))
	if err != nil {
		s.writeError(w, r, errBadRequest("orgId must be a valid UUID"))
		return
	}

	var req createProjectRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, r, errBadRequest("request body must be valid JSON with name"))
		return
	}
	if req.Name == "" {
		s.writeError(w, r, errUnprocessable("name is required"))
		return
	}
	slug := req.Slug
	if slug == "" {
		slug = slugify(req.Name)
	}

	var project db.Project
	err = s.pool.WithTx(r.Context(), userID, orgID, func(ctx context.Context, conn db.Conn) error {
		if err := requirePermission(ctx, conn, userID, orgID, auth.PermProjectCreate); err != nil {
			return err
		}
		var err error
		project, err = db.NewProjectRepository(conn).Create(ctx, orgID, req.Name, slug)
		return mapDBError(err, "", "a project with this slug already exists in this organization")
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, toProjectResponse(project))
}

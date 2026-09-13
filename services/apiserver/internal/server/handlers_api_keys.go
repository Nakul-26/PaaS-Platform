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

// Task 5 (phase-6-multi-tenant-saas.md): API keys. POST/GET have orgId
// directly in the URL, same no-resolution-step shape as the org/member
// routes; DELETE is deep-by-ID (no orgId in the URL), so org_id is resolved
// from the key itself first, same pattern as handleDeleteDomain. Both
// api_keys.create and api_keys.revoke additionally flow through
// requirePermission's API-key-org-match check (permission.go) — relevant
// here too, not just on other resources: a key can only manage keys in its
// own org, even if its creator belongs to others.

type apiKeyResponse struct {
	ID        string     `json:"id"`
	OrgID     string     `json:"org_id"`
	Name      string     `json:"name"`
	Scopes    []string   `json:"scopes"`
	CreatedBy string     `json:"created_by"`
	CreatedAt time.Time  `json:"created_at"`
	RevokedAt *time.Time `json:"revoked_at,omitempty"`
}

func toAPIKeyResponse(k db.APIKey) apiKeyResponse {
	return apiKeyResponse{
		ID: k.ID.String(), OrgID: k.OrgID.String(), Name: k.Name,
		Scopes: k.Scopes, CreatedBy: k.CreatedBy.String(),
		CreatedAt: k.CreatedAt, RevokedAt: k.RevokedAt,
	}
}

// createAPIKeyResponse embeds apiKeyResponse plus the one-time plaintext
// key — never present on any other response in this file, since nothing
// past this single response ever has access to it again (open decision 5:
// scopes are stored and returned but not enforced beyond the key's own
// org/creator-role, see the deferred list in the phase doc).
type createAPIKeyResponse struct {
	apiKeyResponse
	Key string `json:"key"`
}

type createAPIKeyRequest struct {
	Name   string   `json:"name"`
	Scopes []string `json:"scopes,omitempty"`
}

// handleCreateAPIKey is POST /v1/orgs/:orgId/api-keys {name, scopes},
// gated by api_keys.create (owner/admin per the matrix).
func (s *Server) handleCreateAPIKey(w http.ResponseWriter, r *http.Request) {
	userID, _ := auth.UserIDFromContext(r.Context())
	orgID, err := uuid.Parse(r.PathValue("orgId"))
	if err != nil {
		s.writeError(w, r, errBadRequest("orgId must be a valid UUID"))
		return
	}

	var req createAPIKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, r, errBadRequest("request body must be valid JSON with name"))
		return
	}
	if req.Name == "" {
		s.writeError(w, r, errUnprocessable("name is required"))
		return
	}

	plaintext, hash, err := auth.GenerateAPIKey()
	if err != nil {
		s.writeError(w, r, err)
		return
	}

	var key db.APIKey
	err = s.pool.WithTx(r.Context(), userID, orgID, func(ctx context.Context, conn db.Conn) error {
		if err := requirePermission(ctx, conn, userID, orgID, auth.PermAPIKeysCreate); err != nil {
			return mapDBError(err, "no organization with this id that you belong to", "")
		}
		var err error
		key, err = db.NewAPIKeyRepository(conn).Create(ctx, orgID, req.Name, req.Scopes, userID, hash)
		if err != nil {
			return err
		}
		// metadata never includes the plaintext key or its hash — same
		// "never present past this one response" discipline as
		// createAPIKeyResponse itself.
		return recordAudit(ctx, conn, orgID, userID, "api_key.create", "api_key", key.ID, map[string]any{"name": key.Name})
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, createAPIKeyResponse{apiKeyResponse: toAPIKeyResponse(key), Key: plaintext})
}

// handleListAPIKeys is GET /v1/orgs/:orgId/api-keys, gated by
// api_keys.create too — the matrix has no separate list permission, and
// since only owner/admin can create a key per the matrix, only they get to
// see the list (phase-6-multi-tenant-saas.md Task 5). The plaintext key is
// never present here — apiKeyResponse has no Key field, only
// createAPIKeyResponse does.
func (s *Server) handleListAPIKeys(w http.ResponseWriter, r *http.Request) {
	userID, _ := auth.UserIDFromContext(r.Context())
	orgID, err := uuid.Parse(r.PathValue("orgId"))
	if err != nil {
		s.writeError(w, r, errBadRequest("orgId must be a valid UUID"))
		return
	}

	var keys []db.APIKey
	err = s.pool.WithTx(r.Context(), userID, orgID, func(ctx context.Context, conn db.Conn) error {
		if err := requirePermission(ctx, conn, userID, orgID, auth.PermAPIKeysCreate); err != nil {
			return mapDBError(err, "no organization with this id that you belong to", "")
		}
		var err error
		keys, err = db.NewAPIKeyRepository(conn).ListByOrg(ctx, orgID)
		return err
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	data := make([]apiKeyResponse, len(keys))
	for i, k := range keys {
		data[i] = toAPIKeyResponse(k)
	}
	writeJSON(w, http.StatusOK, struct {
		Data []apiKeyResponse `json:"data"`
	}{Data: data})
}

// handleRevokeAPIKey is DELETE /v1/api-keys/:keyId — a deep by-ID route
// with no orgId in the URL, so org_id is resolved from the key itself
// first, same pattern as handleDeleteDomain. Gated by api_keys.revoke
// (owner/admin per the matrix).
func (s *Server) handleRevokeAPIKey(w http.ResponseWriter, r *http.Request) {
	userID, _ := auth.UserIDFromContext(r.Context())
	keyID, err := uuid.Parse(r.PathValue("keyId"))
	if err != nil {
		s.writeError(w, r, errBadRequest("keyId must be a valid UUID"))
		return
	}

	err = s.pool.WithTx(r.Context(), userID, uuid.Nil, func(ctx context.Context, conn db.Conn) error {
		keys := db.NewAPIKeyRepository(conn)
		orgID, err := keys.OrgID(ctx, keyID)
		if err != nil {
			return mapDBError(err, "no api key with this id in an organization you belong to", "")
		}
		if err := db.SetCurrentOrg(ctx, conn, orgID); err != nil {
			return err
		}
		if err := requirePermission(ctx, conn, userID, orgID, auth.PermAPIKeysRevoke); err != nil {
			return err
		}
		if err := keys.Revoke(ctx, keyID); err != nil {
			return mapDBError(err, "no api key with this id in an organization you belong to", "")
		}
		return recordAudit(ctx, conn, orgID, userID, "api_key.revoke", "api_key", keyID, nil)
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

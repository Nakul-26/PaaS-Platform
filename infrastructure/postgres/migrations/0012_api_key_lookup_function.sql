-- +goose Up
-- The API-key auth path (phase-6-multi-tenant-saas.md Task 5,
-- internal/auth.Authenticate) needs to look up an api_keys row by its hash
-- before any identity is known at all — that lookup *is* what establishes
-- app.current_user_id/current_org_id, so there's nothing yet for
-- api_keys_isolation's ordinary RLS policy (0002_api_keys.sql) to key off;
-- run over the plain platform_app connection it would always see zero
-- rows. Same problem 0001_organizations_users_memberships.sql's
-- is_org_admin solves for memberships' self-referential check, same fix: a
-- SECURITY DEFINER function scoped to exactly this one lookup, nothing
-- broader — it does not expose key_hash itself, and only ever matches a
-- caller-supplied hash against revoked_at IS NULL rows.
-- +goose StatementBegin
CREATE FUNCTION lookup_active_api_key(p_key_hash TEXT)
RETURNS TABLE(id UUID, org_id UUID, name TEXT, scopes TEXT[], created_by UUID, created_at TIMESTAMPTZ)
LANGUAGE sql SECURITY DEFINER STABLE AS $$
    SELECT id, org_id, name, scopes, created_by, created_at
    FROM api_keys
    WHERE key_hash = p_key_hash AND revoked_at IS NULL;
$$;
-- +goose StatementEnd

GRANT EXECUTE ON FUNCTION lookup_active_api_key(TEXT) TO platform_app;

-- +goose Down
DROP FUNCTION lookup_active_api_key(TEXT);

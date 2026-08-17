-- +goose Up
-- Rotating refresh tokens (ADR-0008): stored hashed, never plaintext.
-- Scoped to a user, not an org — like `users` itself, this table carries no
-- RLS policy (database-schema.md §2 "no RLS — a user's own row is always
-- visible to themself"); a session is a property of a user's login, not of
-- any one org they belong to.
CREATE TABLE refresh_tokens (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    token_hash TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ,
    -- Set when this token is rotated out for a successor, so a reused old
    -- token is detectable (ADR-0008) rather than just silently rejected.
    replaced_by_id UUID REFERENCES refresh_tokens (id)
);

CREATE INDEX refresh_tokens_user_id_idx ON refresh_tokens (user_id);

-- +goose Down
DROP TABLE refresh_tokens;

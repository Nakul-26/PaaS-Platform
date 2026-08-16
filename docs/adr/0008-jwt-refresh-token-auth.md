# ADR 0008: JWT access tokens + rotating refresh tokens for authentication

- Status: Accepted
- Date: 2026-08-16

## Context

The API server needs to authenticate dashboard, CLI, and (later) third-party API-key callers, and needs to scale horizontally without requiring every instance to share a live session store just to verify a request.

## Decision

- Short-lived JWT access tokens (target: 15 minutes), signed by the API server, carrying `user_id` and enough claims to avoid a DB round-trip for basic auth checks (org memberships are still re-checked server-side per request for authorization, not trusted purely from the token — see `rbac-multitenancy.md`).
- Long-lived, rotating refresh tokens, stored hashed in Postgres, used only to mint new access tokens. Rotation on every use (old refresh token invalidated, new one issued) so a leaked-and-reused old token is detectable.
- API keys (separate mechanism, `api_keys` table) for CLI/CI/automation use cases that shouldn't go through interactive login.

## Consequences

- Positive: access-token verification is stateless (signature check only), so it doesn't add a DB dependency to every authenticated request's hot path.
- Positive: refresh-token rotation gives real revocation capability without needing a session store lookup on every single request — only on the less-frequent refresh call.
- Negative: access tokens can't be instantly revoked mid-lifetime (standard JWT tradeoff) — mitigated by keeping the access-token TTL short (15 min) so a compromised token has a bounded window.
- Authorization (what an authenticated user is *allowed* to do) is never trusted from the token alone — every request re-checks membership/role/quota against Postgres. The token proves *who*, not *what they can do right now*. This matters because role changes and removals must take effect immediately, not after a 15-minute token expiry.

## Alternatives considered

- **Server-side sessions only**: simpler revocation story, but couples every request to a shared session store lookup, working against horizontal API-server scaling.
- **Long-lived JWTs with no refresh**: rejected — no practical revocation path short of a blocklist, which reintroduces the shared-state problem this design avoids.

## Related

`rbac-multitenancy.md`, `database-schema.md` (`api_keys` table).

# API Conventions

Governs the API server's public REST surface (ADR-0007). Both the dashboard and CLI are plain clients of this contract — no special-cased internal endpoints.

## 1. Base URL & versioning

```text
https://api.<platform-domain>/v1/...
```

- Version lives in the URL path (`/v1`), not a header — simpler for `curl`-based debugging and CLI implementation.
- Breaking changes require a new version prefix (`/v2`); additive changes (new optional fields, new endpoints) don't bump the version.

## 2. Resource naming

Standard REST resource nouns, plural, nested under their parent where ownership is fixed:

```text
POST   /v1/auth/signup
POST   /v1/auth/login
POST   /v1/auth/refresh

GET    /v1/orgs/:orgId/projects
POST   /v1/orgs/:orgId/projects
GET    /v1/projects/:projectId/applications
POST   /v1/projects/:projectId/applications
GET    /v1/applications/:appId
PATCH  /v1/applications/:appId                # scale: { replicas_desired } (phase-3-controllers.md Task 5)
POST   /v1/applications/:appId/deployments
GET    /v1/applications/:appId/deployments
GET    /v1/deployments/:deploymentId
POST   /v1/deployments/:deploymentId/rollback
GET    /v1/applications/:appId/logs           # see §6 streaming
GET    /v1/orgs/:orgId/nodes
GET    /v1/orgs/:orgId/audit-logs
```

Rule: once a resource has a global unique ID (everything here uses UUIDs), deep endpoints address it directly by ID (`/v1/applications/:appId`) rather than requiring the full parent chain in the URL — avoids `/v1/orgs/:orgId/projects/:projectId/applications/:appId/deployments/:deploymentId` chains. The org/project scoping is still enforced server-side via the resource's stored `org_id`/RLS, not trusted from the URL.

## 3. Auth

- `POST /v1/auth/signup` and `POST /v1/auth/login` (ADR-0008) return a short-lived access token, a rotating refresh token, and the caller's default org (ADR-0004 — Phase 1 has no org-creation flow, so signup always bootstraps exactly one). `POST /v1/auth/refresh` trades a still-valid refresh token for a new access/refresh pair, revoking the one presented (rotation, not reuse).
- `Authorization: Bearer <jwt>` for interactive (dashboard/CLI login) sessions.
- `Authorization: Bearer <api_key>` for non-interactive callers — API server distinguishes by key format/prefix (e.g. `pk_live_...`) and looks up `api_keys.key_hash` accordingly (ADR-0008).
- Every authenticated request resolves the caller's org membership + role server-side before touching any resource — never trust an `org_id` or role claim from client input.

## 4. Request/response conventions

- All bodies are JSON. `Content-Type: application/json` required on writes.
- Timestamps: RFC 3339 UTC (`2026-08-16T09:00:00Z`).
- IDs: UUIDv4 strings.
- Partial updates use `PATCH`, not `PUT` — `PUT` is not used anywhere in this API (avoids the "must resend the whole resource" footgun).

### Standard error envelope

```json
{
  "error": {
    "code": "quota_exceeded",
    "message": "Organization has reached its container limit (20/20).",
    "details": { "limit": 20, "current": 20 }
  }
}
```

- `code`: stable, machine-readable, snake_case — safe for the CLI/dashboard to branch on.
- `message`: human-readable, safe to show directly in UI.
- HTTP status still carries the primary signal (`400`, `401`, `403`, `404`, `409`, `422`, `429`, `500`); `code` disambiguates within a status.

### Pagination

Cursor-based, not offset-based — offset pagination degrades badly once tables like `audit_logs` and `resource_usage_samples` grow, and cursor pagination is barely more complex to implement upfront.

```text
GET /v1/orgs/:orgId/audit-logs?limit=50&cursor=eyJpZCI6...

{
  "data": [ ... ],
  "next_cursor": "eyJpZCI6..." | null
}
```

### Idempotency

Deployment-creating and scaling requests accept an `Idempotency-Key` header. The API server stores the key (short TTL, e.g. 24h) against the resulting resource ID and returns the original response on a retried request with the same key — protects against double-deploys from CLI/network retries.

## 5. Rate limiting

- Enforced per API key / per authenticated user, not just per IP (IP alone doesn't isolate tenants sharing a NAT/office network).
- Standard headers on every response: `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`.
- `429` responses use the standard error envelope with `code: "rate_limited"`.

## 6. Streaming / live data

REST for everything else, but two things are inherently long-lived:

- **Log tailing**: `GET /v1/applications/:appId/logs?follow=true` upgrades to Server-Sent Events (SSE). Chosen over WebSocket here specifically because it's one-directional (server→client) and SSE's auto-reconnect behavior is simpler than reimplementing it over raw WebSocket.
- **Live metrics on the dashboard** (CPU/memory graphs, replica status): also SSE, same rationale.
- WebSocket is reserved for genuinely bidirectional needs (future: interactive `exec` into a container) — not used where SSE suffices.

## 7. What the CLI and dashboard are allowed to assume

- Every endpoint in this document enforces RBAC + tenant isolation server-side (`rbac-multitenancy.md`) — clients must not implement authorization logic themselves; a client-side role check is a UX nicety, never a security boundary.
- Clients must handle `403` (authenticated but not permitted) distinctly from `401` (not authenticated) — a `403` on an action means "ask an org admin," not "log in again."

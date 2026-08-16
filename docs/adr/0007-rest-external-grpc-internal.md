# ADR 0007: REST for the external API surface; gRPC reserved for internal streaming (deferred past MVP)

- Status: Accepted
- Date: 2026-08-16

## Context

Two different kinds of clients talk to this system: humans/human-driven tools (dashboard, CLI) calling a fairly conventional CRUD-shaped API, and, later, our own services talking to each other with streaming needs (log tail, exec-into-container) where typed contracts matter more than broad client compatibility.

## Decision

- The API server's public surface is REST over HTTPS with JSON bodies, versioned under `/v1`. See `api-conventions.md` for the full contract.
- Internal service-to-service communication starts on NATS (ADR-0005) for pub/sub and simple assignment messages. gRPC + protobuf is introduced starting Phase 2+ specifically for cases that need typed request/response or streaming between two known services (not for anything human/dashboard-facing).
- The dashboard and CLI are constrained to be plain clients of the same public REST API — no special internal-only endpoints for the dashboard. This keeps the API server the single, honest authorization boundary (every caller goes through the same auth/RBAC middleware).

## Consequences

- Positive: REST keeps the human-facing surface simple to document, debug with `curl`, and consume from both TypeScript and Go without codegen being mandatory on day one.
- Positive: constraining the dashboard to the public API prevents an "admin backdoor" surface from silently accumulating weaker auth checks than the main API.
- Positive: gRPC is introduced only when its benefits (streaming, compile-time contracts) are actually needed, not speculatively.
- Negative: two protocols in the system long-term (REST + gRPC) instead of one — judged acceptable since they serve genuinely different call shapes (see the communication-pattern rule of thumb in `ARCHITECTURE.md` §3).

## Alternatives considered

- **GraphQL** for the external API: rejected — the API surface here is conventionally CRUD/resource-shaped, and GraphQL's schema-stitching flexibility isn't needed at this scope.
- **gRPC-Web for the dashboard too**: rejected for the MVP — adds tooling complexity (proxying, codegen) the dashboard doesn't need yet; revisit only if REST genuinely becomes a bottleneck for a dashboard use case (e.g. very high-frequency live updates, which SSE/WebSocket already cover — see `api-conventions.md`).

## Related

`api-conventions.md`, `ARCHITECTURE.md` §3, §8.

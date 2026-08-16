# Coding Standards

Applies across all services in the monorepo (`ARCHITECTURE.md` §5). These are enforced expectations for anything merged, not aspirational guidelines.

## 1. Go (all infrastructure services)

- **Error handling**: always wrap with context (`fmt.Errorf("scheduling replica for app %s: %w", appID, err)`), never discard an error silently. No `panic` in request-handling or reconciliation-loop paths — return errors and let the caller decide; `panic` is reserved for genuine programmer-error invariant violations at startup (e.g. missing required config).
- **Context propagation**: every function that does I/O (DB, HTTP, NATS, Docker) takes a `context.Context` as its first argument and respects cancellation/deadlines. No `context.Background()` calls buried inside business logic — it should only appear at the true top of a call chain (an HTTP handler, a reconcile-loop tick).
- **Structured logging**: `slog`, not `fmt.Println`/`log.Printf`. Every log line in a request or reconciliation path includes correlating fields (`org_id`, `request_id`, `deployment_id` as applicable) — logs must be greppable/filterable, not prose.
- **Package layout**: `internal/` packages are not importable outside this module by design (ADR-adjacent to `ARCHITECTURE.md` §5's repo-structure reasoning) — this is enforced by Go's own `internal/` convention, not just documentation.
- **Interfaces**: defined at the point of use (consumer side), not speculatively next to the implementation — this is standard Go idiom and matters specifically for `ContainerRuntime` (ADR-0006), where the interface shape should be driven by what the worker agent actually needs, not by what Docker's SDK happens to expose.
- **External dependencies behind an interface (ADR-0011)**: no business logic calls a vendor SDK (Docker client, NATS client, `pgx`, a JWT library) directly — it depends on an internal interface with one adapter implementing it. Only add a new interface for a dependency that has a documented reason to expect an alternative (tracked in `modularity-and-extensibility.md` §2) — this is not a license to wrap everything speculatively.
- **No cross-service internal imports (ADR-0012)**: a package under `services/<name>/` must never import another service's package tree. It may depend on domain-free `internal/` code and on other services' published network contracts (REST/gRPC/NATS) only. If review finds a direct import across service boundaries, that's a blocking finding, not a style nit — it silently undoes the deployment independence the whole `services/` split exists for.
- **Concurrency**: no unbounded goroutine spawning per request/event — use worker pools or bounded channels where volume is unbounded by nature (e.g. log line fan-out). Every goroutine's exit condition must be traceable (tie to a `context.Context` or a clearly closed channel) — no goroutine leaks.
- **Testing**: table-driven tests as the default shape for pure logic (scheduler scoring, RBAC permission checks). See `testing-strategy.md` for what tier of test is expected where.
- **Linting**: `golangci-lint` with (at minimum) `govet`, `staticcheck`, `errcheck`, `gosec` enabled — `gosec` specifically because this project handles secrets and multi-tenant data.

## 2. TypeScript / Next.js (dashboard)

- **Strict mode**: `strict: true` in `tsconfig.json`, no exceptions. No `any` without an inline comment justifying it (and preferring `unknown` + a narrowing check instead, wherever feasible).
- **Runtime validation at the API boundary**: responses from the Go API server are validated at runtime (e.g. `zod`) before being trusted as typed data — a TypeScript type alone doesn't protect against the API server and dashboard drifting out of sync during development. This is where the OpenAPI-generated-types decision from `ARCHITECTURE.md` §11 open question 2 plugs in: generated types describe the *shape*, runtime validation catches *drift*.
- **Server vs. client components**: default to server components; a component becomes a client component only when it needs interactivity/state/live data (SSE log tail, metrics graphs) — not by default.
- **No direct calls to Postgres/NATS/workers from the dashboard** — it is a REST client of the API server only (ADR-0007). This isn't just a convenience convention; it's the enforcement of "the API server is the single authorization boundary."
- **Styling**: Tailwind utility classes; component-level abstraction only once a pattern repeats 3+ times (mirrors the top-level project rule against premature abstraction).

## 3. Cross-cutting

- **No commented-out code** left in merged PRs — delete it; git history is the record.
- **No dead feature flags / backwards-compat shims** for code that hasn't shipped externally yet — this is pre-1.0 infrastructure; change the thing directly instead of branching around it.
- **Every non-obvious "why"** (a workaround, a subtle invariant, a deliberately-simplified tradeoff) gets a one-line comment or a link to the relevant ADR — prefer linking to an ADR over re-explaining the reasoning inline when one exists.
- **Migrations**: every schema change ships with its RLS policy in the same migration (ADR-0010) — a migration adding a tenant-scoped table without a policy is treated as incomplete, not "policy to follow later."

## 4. Definition of "ready to merge"

- Builds and lints clean.
- Has the tier of test appropriate to what changed (`testing-strategy.md`).
- If it touches an RBAC-protected route or tenant-scoped table: includes the cross-tenant/permission-matrix tests required by `rbac-multitenancy.md` §5.
- If it makes or changes an architectural decision: has a corresponding new or superseding ADR in `docs/adr/`.
- If it introduces a new external dependency or a new cross-service interaction: the interface/contract is registered in `docs/modularity-and-extensibility.md` §2 or §3, not just implemented ad hoc.

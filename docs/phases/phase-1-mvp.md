# Phase 1 — Single-Node Container Platform (MVP): Detailed Task Plan

Parent scope: `ARCHITECTURE.md` §7 (MVP scope) and §10 (Phase 1 entry). This doc breaks that scope into implementable, orderable tasks with acceptance criteria, so implementation can start from a checklist rather than re-deriving the plan mid-coding.

Exit criteria for the whole phase (restated from `ARCHITECTURE.md` §7):

```text
platform login
platform create project demo
platform deploy demo --image nginx:latest
platform get deployments          # shows running
curl <worker-host>:<mapped-port>  # serves nginx
platform logs demo
platform delete demo              # container stops and is removed
```

## Task sequence

Tasks are ordered by dependency — each assumes the ones above it exist. Parallelizable pairs are noted.

### 1. Repo & module scaffolding
- Initialize git repo, `go.mod` at repo root (or per-service modules — decide single-module-monorepo vs. multi-module during this task; default to single module for Phase 1 simplicity).
- Create the directory layout from `ARCHITECTURE.md` §5.
- `golangci-lint` config per `coding-standards.md` §1.
- Acceptance: `go build ./...` succeeds on an empty scaffold; lint runs clean.

### 2. Postgres + migrations
- Stand up `docker-compose.yml` with `postgres` service.
- `goose` migration tooling wired per `database-schema.md` §4.
- Write migrations `0001`–`0004` (organizations/users/memberships, api_keys, projects, applications/env_vars) — the tables Phase 1 actually touches. Remaining tables from `database-schema.md` can land in later phases' migrations rather than all upfront, *except* that the tenant-scoping columns (ADR-0004) must be present on every table from its first creation, whenever it's created.
- RLS policies written in the same migrations per ADR-0010 (even though Phase 1 has only one org in practice — the policy should still be correct and tested).
- Acceptance: migrations apply cleanly to a fresh DB; a basic RLS cross-tenant test (two orgs, two rows, query as each) passes against real Postgres via `testcontainers-go`.

### 3. `ContainerRuntime` interface + Docker implementation
- Define the interface per ADR-0006 in `internal/runtime`.
- Implement against Docker Engine SDK: `PullImage`, `CreateContainer`, `StartContainer`, `StopContainer`, `RemoveContainer`, `ContainerStatus`, `StreamLogs`.
- Acceptance: an integration test (real Docker daemon via `testcontainers-go` or the host daemon) pulls `nginx:latest`, starts it, confirms `ContainerStatus` reports running, streams at least one log line, stops and removes it.

*(Can run in parallel with Task 2 — no dependency between them.)*

### 4. Worker agent (single instance, Phase 1 shape — no NATS yet)
- Separate process from the API server, exposing an internal HTTP endpoint for "start this container," "stop this container," "get status," "get logs" — not embedded in the API server. This is required, not just preferred: per ADR-0012 the API server may only reach the worker over a network contract, even at Phase 1 scale with a single worker, so Phase 2's NATS-based split is additive (a new transport for the same contract) rather than a retrofit that has to first extract a boundary that was never there.
- Uses the `ContainerRuntime` interface from Task 3.
- Acceptance: can be driven manually (e.g. via a test HTTP call) to start/stop/inspect a container.

### 5. API server: core CRUD
- Auth: signup/login issuing JWT + refresh token per ADR-0008 (password hashing, e.g. `bcrypt`/`argon2`).
- Default-org bootstrap on first user signup (Phase 1 doesn't build org-creation UI/flow yet — every new user gets a default org, per `ARCHITECTURE.md` §7).
- Data access goes through one repository interface per aggregate (`OrganizationRepository`, `ApplicationRepository`, `DeploymentRepository`, ...) per ADR-0011/`modularity-and-extensibility.md` §2 — handlers depend on the repository interface, never call `sqlc`-generated queries or raw SQL directly. This is what makes the required RLS cross-tenant tests (Task 2) easy to write against a fake in unit tests and real in integration tests.
- Routes: `create project`, `create application`, `deploy` (creates a `deployments` row, calls the worker agent from Task 4 over its HTTP contract — no real scheduler yet, Phase 1 has exactly one worker), `get deployments`, `delete application`.
- RBAC middleware wired per `rbac-multitenancy.md` §3, even though Phase 1 only exercises the `owner` role in practice (every user is `owner` of their default org) — the middleware itself should be real, not stubbed, so Phase 6 is "add more roles to an existing check," not "build the check."
- Follows `api-conventions.md` for all route shapes, error envelope, pagination.
- Acceptance: integration tests hit real routes against a real (test-container) Postgres + real worker agent, covering the full create→deploy→status→delete flow.

### 6. CLI
- `cobra`-based, thin REST client per ADR-0007 (no logic duplicated from the API server — if a check needs to happen, it happens server-side and the CLI just surfaces the response).
- Commands: `login`, `create project`, `deploy`, `get deployments`, `logs`, `delete`.
- Acceptance: the exact exit-criteria script at the top of this doc runs successfully end-to-end using the CLI binary against a `docker-compose`-launched stack.

### 7. Log retrieval (Phase 1 shape)
- `platform logs <app>` reads directly from the worker agent's `StreamLogs` (Task 3/4), proxied through the API server (not yet fanned out via NATS — that's Phase 4+ when there are multiple consumers of log data).
- Acceptance: `platform logs demo` shows nginx's access/error log lines for a running deployment.

### 8. End-to-end test automation
- Automate the exit-criteria script from `testing-strategy.md` §1 (E2E tier) as a real, runnable test, not just a manual checklist.
- Acceptance: this test is what actually certifies Phase 1 done — running it is the literal exit criteria check.

## Explicitly deferred out of Phase 1

Per `ARCHITECTURE.md` §7 — do not build any of this now, even if it looks small:
- Multi-node scheduling (Phase 2) — Task 5 calls the one worker directly.
- NATS eventing (Phase 2+) — Task 4's worker is called directly over HTTP for Phase 1; introducing NATS here would be solving Phase 2's problem early.
- Load balancer / service discovery (Phase 4/5).
- RBAC role variety and org-management UI (Phase 6) — the middleware exists (Task 5) but only `owner` is exercised.
- Rolling deployments / rollback (Phase 8) — Phase 1's `deploy` is recreate-only, single replica.

## Open implementation decisions to resolve at the start of coding

1. Single Go module vs. multi-module monorepo (Task 1) — recommend single module for Phase 1, revisit if build/dependency isolation becomes a real problem.
2. Worker agent as a separate process from day one vs. embedded in the API server for Phase 1 only — leaning separate process (see Task 4) so Phase 2's NATS-based worker protocol is additive.

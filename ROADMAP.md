# Roadmap & Phase Status

Living tracker — update this file as phases progress. Design decisions live in `ARCHITECTURE.md` and `docs/adr/`; this file only tracks status. Detailed exit criteria for each phase are in `ARCHITECTURE.md` §10; Phase 1 additionally has a full task breakdown in [`docs/phases/phase-1-mvp.md`](docs/phases/phase-1-mvp.md).

Status legend: ⬜ Not started · 🟨 In progress · ✅ Done

| Phase | Name | Status | Notes |
|---|---|:---:|---|
| 0 | Architecture | ✅ | `ARCHITECTURE.md` + full `docs/` set written 2026-08-16. |
| 1 | Single-Node Container Platform (MVP) | 🟨 | Started 2026-08-16. Tasks 1-6 done: repo/module scaffolding; Postgres migrations 0001-0006 with RLS (ADR-0010) + cross-tenant integration test; `ContainerRuntime` interface + Docker SDK adapter with lifecycle integration test; worker agent (separate process, HTTP contract in `docs/worker-agent-contract.md`) with lifecycle integration test driven purely over HTTP; API server core CRUD (JWT auth + rotating refresh tokens, default-org bootstrap, per-aggregate repositories, RBAC middleware, project/application/deployment routes driving the worker agent) with a full-flow integration test against real Postgres + a real worker process; `cobra`-based CLI (`apps/cli`, ADR-0007) — `signup`/`login`, `create project`, `deploy` (create-or-update + `--port` binding), `get deployments`, `logs`, `delete` — manually verified end-to-end against a real Postgres + worker + apiserver stack, including a real `nginx` container reachable via its mapped port. `logs` is wired to the not-yet-built `GET /v1/applications/:appId/logs` route (Task 7) and fails cleanly (404) until then. |
| 2 | Multi-Node Infrastructure | ⬜ | |
| 3 | Desired State + Controllers | ⬜ | |
| 4 | Service Discovery + Load Balancer | ⬜ | |
| 5 | Networking + Ingress | ⬜ | |
| 6 | Multi-Tenant SaaS Layer | ⬜ | Schema pre-built in Phase 1 per ADR-0004; this phase is enforcement + UI. |
| 7 | Deployment Platform (Git → build → deploy) | ⬜ | |
| 8 | Advanced Deployment (rolling, rollback) | ⬜ | |
| 9 | Observability | ⬜ | |
| 10 | Reliability (failure injection) | ⬜ | |
| 11 | Production Hardening | ⬜ | |

## Next action

Task 7: Log retrieval (`GET /v1/applications/:appId/logs` proxied through the API server to the worker's `StreamLogs`, per `docs/worker-agent-contract.md`) — per `docs/phases/phase-1-mvp.md`. This is what makes the CLI's already-built `platform logs` command actually work.

## How to update this file

- Flip a phase to 🟨 when its first task starts, ✅ only when its exit criteria (`ARCHITECTURE.md` §10) actually pass.
- Add a one-line note on anything that deviated from the written plan — if a phase's actual implementation diverges from `ARCHITECTURE.md` or its ADRs, update those documents in the same change, don't just note the drift here and move on.

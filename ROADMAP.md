# Roadmap & Phase Status

Living tracker — update this file as phases progress. Design decisions live in `ARCHITECTURE.md` and `docs/adr/`; this file only tracks status. Detailed exit criteria for each phase are in `ARCHITECTURE.md` §10; Phase 1 additionally has a full task breakdown in [`docs/phases/phase-1-mvp.md`](docs/phases/phase-1-mvp.md).

Status legend: ⬜ Not started · 🟨 In progress · ✅ Done

| Phase | Name | Status | Notes |
|---|---|:---:|---|
| 0 | Architecture | ✅ | `ARCHITECTURE.md` + full `docs/` set written 2026-08-16. |
| 1 | Single-Node Container Platform (MVP) | ✅ | Started 2026-08-16, exit criteria automated and passing 2026-08-17. Tasks 1-8 done: repo/module scaffolding; Postgres migrations 0001-0006 with RLS (ADR-0010) + cross-tenant integration test; `ContainerRuntime` interface + Docker SDK adapter with lifecycle integration test; worker agent (separate process, HTTP contract in `docs/worker-agent-contract.md`) with lifecycle integration test driven purely over HTTP; API server core CRUD (JWT auth + rotating refresh tokens, default-org bootstrap, per-aggregate repositories, RBAC middleware, project/application/deployment routes driving the worker agent) with a full-flow integration test against real Postgres + a real worker process; `cobra`-based CLI (`apps/cli`, ADR-0007) — `signup`/`login`, `create project`, `deploy` (create-or-update + `--port` binding), `get deployments`, `logs`, `delete` — manually verified end-to-end against a real Postgres + worker + apiserver stack, including a real `nginx` container reachable via its mapped port; log retrieval (`GET /v1/applications/:appId/logs`, proxied through the API server to the worker's `StreamLogs`) making `platform logs`/`--follow` fully functional, covered by the Task 5 integration test's extension into a real deploy-then-read-logs step plus a cross-tenant denial check; the phase's exit-criteria script itself automated as `e2e/phase1_test.go` (`go test -tags=e2e ./e2e/...`) — builds the real `platform`/`apiserver`/`worker` binaries, runs them as separate OS processes against a real testcontainer Postgres, and drives the CLI binary through signup→create project→deploy nginx→poll for running→curl the mapped port→read logs→delete→confirm the container is actually unreachable afterward, passing in ~40s with no leaked containers. Along the way, fixed a bug the new `--follow` path would have hit: both `workerclient` and the CLI's `apiclient` had a blanket 30s `http.Client` Timeout that covers the *entire* response body read, which would have silently killed any log stream (follow or not) after 30 seconds — replaced with a per-call `context.WithTimeout` on non-streaming calls only, leaving streaming calls bounded solely by the caller's own context. |
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

Phase 1 is done. Next: start Phase 2 (Multi-Node Infrastructure) per `ARCHITECTURE.md` §10 — begin with its task breakdown (no `docs/phases/phase-2-*.md` written yet, so that's the first step, mirroring how `docs/phases/phase-1-mvp.md` was written before Phase 1's coding started).

## How to update this file

- Flip a phase to 🟨 when its first task starts, ✅ only when its exit criteria (`ARCHITECTURE.md` §10) actually pass.
- Add a one-line note on anything that deviated from the written plan — if a phase's actual implementation diverges from `ARCHITECTURE.md` or its ADRs, update those documents in the same change, don't just note the drift here and move on.

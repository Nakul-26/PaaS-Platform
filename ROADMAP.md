# Roadmap & Phase Status

Living tracker — update this file as phases progress. Design decisions live in `ARCHITECTURE.md` and `docs/adr/`; this file only tracks status. Detailed exit criteria for each phase are in `ARCHITECTURE.md` §10; Phase 1 additionally has a full task breakdown in [`docs/phases/phase-1-mvp.md`](docs/phases/phase-1-mvp.md).

Status legend: ⬜ Not started · 🟨 In progress · ✅ Done

| Phase | Name | Status | Notes |
|---|---|:---:|---|
| 0 | Architecture | ✅ | `ARCHITECTURE.md` + full `docs/` set written 2026-08-16. |
| 1 | Single-Node Container Platform (MVP) | 🟨 | Started 2026-08-16. Task 1 (repo & module scaffolding) in progress: `go.mod`, repo layout, `.golangci.yml`, service/CLI stub binaries in place. |
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

Finish Task 1 (repo & module scaffolding) — verify `golangci-lint` passes — then move to Task 2 (Postgres + migrations) and Task 3 (`ContainerRuntime` interface), which can run in parallel, per `docs/phases/phase-1-mvp.md`.

## How to update this file

- Flip a phase to 🟨 when its first task starts, ✅ only when its exit criteria (`ARCHITECTURE.md` §10) actually pass.
- Add a one-line note on anything that deviated from the written plan — if a phase's actual implementation diverges from `ARCHITECTURE.md` or its ADRs, update those documents in the same change, don't just note the drift here and move on.

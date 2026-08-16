# Testing Strategy

Defines what "tested" means at each layer, referenced by `coding-standards.md`'s merge checklist and by Phase 10's failure-injection work in `ARCHITECTURE.md`.

## 1. The four tiers

### Unit tests
Scope: pure business logic with no I/O — scheduler scoring function, RBAC permission-matrix evaluation, quota-check arithmetic, reconciliation diff calculation.
Tooling: Go standard `testing` package, table-driven. No mocking frameworks needed at this tier if functions are designed to take plain data in and return plain data out (this is itself a design goal, not just a testing convenience).
Expectation: every pure function with a non-trivial branch (especially the reconciliation diff logic in ADR-0001/§2.3 and the scheduler's filter-then-score in `ARCHITECTURE.md` §2.2) has table-driven coverage of its edge cases, not just the happy path.

### Integration tests
Scope: a service talking to a real dependency — API server against a real (test-container) Postgres, worker agent against a real (test-container) Docker daemon, scheduler round-tripping an assignment through a real NATS instance.
Tooling: `testcontainers-go` to spin up real Postgres/NATS instances per test run rather than mocking the database — mirrors the project's own "don't mock the thing you're trying to verify actually works" principle, especially important for RLS policies (ADR-0010), which are meaningless to test against a mocked DB.
Expectation: every RBAC-protected route and every tenant-scoped table gets integration-level cross-tenant-denial and permission-matrix tests per `rbac-multitenancy.md` §5 — these are integration tests by nature (they require a real DB with real RLS policies active).

### End-to-end tests
Scope: full flows through the real system, as a user or CLI would experience them.
Example (Phase 1 MVP, mirrors `ARCHITECTURE.md` §7's definition-of-done script):
```text
CLI login → create project → deploy image → poll until running →
HTTP request to the deployed container succeeds → fetch logs → delete → confirm gone
```
Tooling: driven through the actual CLI binary and real HTTP calls against a `docker-compose`-launched stack, not through internal Go function calls — the point of this tier is verifying the real external contract.
Expectation: one E2E flow per phase's exit criteria (`ARCHITECTURE.md` §10) — this is literally how a phase's exit criteria gets automated instead of manually re-verified before every future change.

### Failure-injection tests (Phase 10, but designed for from earlier)
Scope: the specific scenarios in `ARCHITECTURE.md` §9's risk register — worker crash, node disappearance, container crash, Postgres unavailability, network partition, NATS unavailability, scheduler failure.
Tooling: `docker-compose` service kill/pause for process-level failures; `iptables`/network-namespace manipulation or a proxy like `toxiproxy` for network partition simulation.
Expectation: each scenario has a written note on **observed** behavior (not assumed) per `ARCHITECTURE.md` Phase 10 exit criteria — a failure-injection test that isn't run and recorded doesn't count as done.

## 2. What tier is required for what kind of change

| Change touches... | Minimum required tier |
|---|---|
| Pure logic (scoring, diffing, permission checks) | Unit |
| A DB query, migration, or RLS policy | Integration |
| An API route | Integration (+ E2E if it's part of a phase's core flow) |
| Cross-service behavior (scheduler → worker → container) | E2E |
| Anything in the risk register (`ARCHITECTURE.md` §9) | Failure-injection, once that phase is reached |

## 3. What we explicitly do not do

- No testing framework abstraction layers beyond what's needed — Go's standard `testing` package plus `testcontainers-go` is sufficient; not introducing a BDD framework (Ginkgo, etc.) without a concrete reason it's needed.
- No snapshot-testing the dashboard UI pixel-for-pixel as a primary strategy — favor testing behavior (does the RBAC-gated button actually call the right endpoint and handle a `403`) over visual diffing, which is high-maintenance relative to the signal it gives for this project.
- Not aiming for 100% coverage as a metric — aiming for "every risk-register scenario and every RBAC/tenant-isolation path is covered," which is a much more specific and meaningful bar.

## 4. CI expectations

- Unit + integration tests run on every PR.
- E2E flow(s) for the phase currently in progress run on every PR touching that phase's services.
- Failure-injection suite runs on a schedule (not every PR — it's slower and more disruptive) once Phase 10 introduces it, plus on-demand before any release-shaped milestone.

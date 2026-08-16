# ADR 0009: Local development simulates multiple nodes as logical processes on one shared Docker daemon

- Status: Accepted
- Date: 2026-08-16

## Context

Local development has exactly one real Docker daemon (the developer's). "3 worker nodes" running locally cannot mean 3 isolated hosts without either faking it or nesting Docker-in-Docker per simulated node. This needs to be a documented, deliberate decision — the alternative is discovering the limitation later via a confusing bug report (e.g. "why did node 2's container affect node 1's resource graph").

## Decision

Local dev runs 3 separate worker-agent *processes* (via `docker-compose`), each configured with an artificial capacity ceiling, all pointed at the same host Docker daemon. This fully exercises scheduling and reconciliation logic (placement decisions, load distribution, node-down simulation by killing a worker process) but does **not** provide real resource isolation between "nodes" — a container scheduled to "node 2" can still affect host-level resources shared with "node 1"'s containers.

Real multi-host resource isolation is validated separately, in Phase 2's actual multi-VPS deployment — not expected to be proven by local dev.

## Consequences

- Positive: local dev stays fast and simple (`docker-compose up`, no nested daemons).
- Positive: scheduler/controller logic gets full test coverage locally.
- Negative: a class of bugs (real resource contention/isolation issues between nodes) is invisible in local dev and must be caught in Phase 2's real multi-node testing or Phase 10's failure-injection pass — this gap is accepted, not hidden.

## Alternatives considered

- **Docker-in-Docker per simulated worker**: more realistic isolation, meaningfully more complex and slower locally. Deferred; revisit only if logical-node simulation proves insufficient for testing a specific scheduler behavior that genuinely requires isolation to observe.

## Related

`ARCHITECTURE.md` §6, §9 R1.

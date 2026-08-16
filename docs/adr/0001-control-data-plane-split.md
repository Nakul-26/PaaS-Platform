# ADR 0001: Split the system into a control plane and a data plane

- Status: Accepted
- Date: 2026-08-16

## Context

The system needs to both *decide* what should be running (accept API calls, store desired state, schedule work, enforce tenant rules) and *actually run it* (execute containers, route HTTP traffic to them). If these responsibilities aren't separated, every part of the system ends up depending on every other part being up, and a bug or outage in, say, the billing/quota code path can take down already-running customer traffic.

## Decision

Split the architecture into two planes with an explicit contract between them:

- **Control plane** (API server, scheduler, controller manager, Postgres): owns desired state, auth, tenant data, scheduling decisions.
- **Data plane** (worker agents, Docker, load balancer): owns actually running containers and routing live traffic.

The data plane must be able to keep serving already-scheduled traffic even if the control plane is completely offline. Concretely: the load balancer reads from the service registry (data plane's own view), never from the API server or Postgres on the request hot path.

## Consequences

- Positive: control-plane outages degrade gracefully (no new deploys/scaling, but existing apps keep serving) instead of causing full outages.
- Positive: the two planes can be scaled, deployed, and reasoned about independently.
- Negative: requires an explicit reconciliation/sync mechanism (service registry, NATS events) to keep the data plane's view eventually consistent with control-plane state — more moving parts than a single monolithic service.
- This split is the reason the service registry (ADR-adjacent, see `ARCHITECTURE.md` §2.6) exists as a separate concept from "whatever's in Postgres" — it's the data plane's own durable-but-independent view.

## Alternatives considered

- **Single monolithic service** doing auth, scheduling, and traffic routing in one process. Rejected: couples the availability of user-facing traffic to the availability of control-plane logic, which defeats a core goal of an orchestration platform (isolate failures).

## Related

Referenced throughout `ARCHITECTURE.md` §1.2–1.3.

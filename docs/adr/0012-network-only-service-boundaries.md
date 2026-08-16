# ADR 0012: Services communicate only across the network boundary, never via shared in-process code

- Status: Accepted
- Date: 2026-08-16

## Context

`ARCHITECTURE.md` §5 already ships each control/data-plane component as its own binary under `services/`. But separate binaries alone don't guarantee real independence — it's easy for one service's code to import another service's internal package "just to reuse a function," which quietly re-creates monolithic coupling inside a multi-binary repo. If that happens, the services can't actually be scaled independently, deployed to separate machines, or rewritten in a different language later without a coordinated rewrite of both sides — which defeats the explicit goal of this platform being easy to extend and long-lived.

## Decision

A service under `services/<name>/` may depend on exactly two things outside its own package tree:

1. **Domain-free shared code** in `internal/` — config loading, structured-logging setup, generated proto/API-client types, auth-token verification helpers. Nothing in `internal/` may contain business/domain logic specific to one service; if it does, that's a signal it should move into that service's own package tree instead.
2. **The published network contract of another service** — its REST API (`api-conventions.md`), its `proto/` gRPC definitions, or its NATS subjects and message schemas.

A service may never import another service's own package tree directly. Any data or behavior one service needs from another crosses the network — REST, gRPC, or NATS — the same boundary it would cross if the two services were already deployed on separate hosts, even while (for now) they're co-located in one `docker-compose` stack.

## Consequences

- Positive: any single service can be extracted to its own deployment — separate host, independent scaling policy, even a full rewrite in another language — as a pure operations/deployment change. The code was never coupled beyond its published contract, so there's nothing to untangle.
- Positive: this is what actually makes "supports a microservices architecture" true from Phase 1 onward, rather than a Phase-11 aspiration — the services already only talk over the network; splitting their deployment later is a manifest/config change, not a rewrite.
- Positive: forces every cross-service interaction to be a deliberate, versioned, documented contract instead of an ad hoc function call — ties directly into `api-conventions.md` and the `proto/` contracts introduced from Phase 2 onward.
- Negative: some small logic duplication is accepted where two services need similar-but-not-identical behavior and it isn't worth standing up a shared contract for it (e.g. both validating a resource-name's shape). This duplication is a deliberate tradeoff — preferred over the coupling a shared internal domain package would silently introduce.
- Negative: slightly more ceremony for genuinely internal, same-team, same-repo interactions than a direct function call would be — accepted as the cost of keeping the option to split real later.

## Alternatives considered

- **Shared internal domain packages, imported directly by multiple services "just for now"**: rejected — this is precisely the shortcut that turns a multi-binary repo back into a distributed monolith: all the deployment complexity of running several processes, none of the independence benefit that's supposed to justify it.
- **A true single-binary monolith with in-process module boundaries instead of separate services at all**: rejected — this project's stated goal is to understand and eventually operate a multi-node, multi-service system; a monolith wouldn't exercise the network-boundary discipline, service discovery, or independent-failure characteristics that are the actual point of the exercise (ADR-0001).

## Related

ADR-0001 (control/data plane split — the first instance of "these two things must not silently depend on each other's internals"), ADR-0011 (the analogous rule for external dependencies), `docs/modularity-and-extensibility.md`, `ARCHITECTURE.md` §5.

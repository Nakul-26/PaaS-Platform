# ADR 0011: Every external dependency sits behind an internal interface (ports & adapters)

- Status: Accepted
- Date: 2026-08-16

## Context

This platform is explicitly meant to be a long-term, evolving project: the container runtime is expected to change (ADR-0006 already anticipates containerd/Firecracker), the message broker could plausibly change (ADR-0005 chose NATS but that's a revisable call), the scheduling algorithm is expected to grow more sophisticated (`ARCHITECTURE.md` §2.2 already flags this), load-balancing strategy is meant to be pluggable (§2.5), and auth is likely to grow from local passwords to SSO/OAuth for enterprise tenants. If business logic anywhere calls a vendor SDK (Docker's client, NATS's client, `pgx` queries, a specific JWT library) directly, every one of those future changes becomes a cross-cutting rewrite touching every call site instead of a contained, additive change.

## Decision

Every external dependency is accessed exclusively through a small Go interface ("port") owned by the consuming code, with exactly one concrete adapter implementing it initially. This generalizes the pattern ADR-0006 already established for `ContainerRuntime` to every other swappable dependency in the system. The concrete list of interfaces, their current adapters, and their likely future alternatives is maintained as a living registry in [`docs/modularity-and-extensibility.md`](../modularity-and-extensibility.md) — not duplicated here, since that list will grow as new phases introduce new dependencies.

This is deliberately scoped, not applied everywhere reflexively: an interface is introduced only for a dependency the project already has a concrete, documented reason to expect will change or gain alternatives (tracked in the registry). A dependency with exactly one plausible implementation and no driver for change does not get a speculative interface — that would violate the project's own anti-over-engineering rule. The registry doc is the single place that records *why* each interface earned its place.

## Consequences

- Positive: swapping or adding an implementation (containerd alongside Docker, a new scheduling strategy, SSO alongside password auth) is additive — a new adapter file, not a rewrite of consumers.
- Positive: each interface is a natural seam for unit testing business logic against a fake, without needing the real dependency running (`testing-strategy.md` §1, unit tier).
- Positive: forces an explicit, minimal contract instead of a vendor SDK's full surface leaking into business logic — the interface documents what the system actually needs from the dependency, not everything the SDK happens to offer.
- Negative: upfront design cost per interface, and a poorly-shaped interface chosen early may need revising once a second adapter is actually built — accepted, since revising a small internal interface is far cheaper than the alternative this ADR exists to avoid.

## Alternatives considered

- **Direct SDK calls, refactor into an interface later if a swap actually becomes necessary**: rejected. This is exactly the failure mode the project's long-term-modularity requirement calls out — by the time a swap is needed, the SDK's types and assumptions are already threaded through every caller, and "extract an interface" becomes a large, risky, all-at-once change instead of something that was cheap from the start.

## Related

ADR-0006 (the first instance of this pattern), ADR-0012 (the analogous rule for service-to-service boundaries), `docs/modularity-and-extensibility.md` (the concrete, living registry), `coding-standards.md`.

# ADR 0006: Docker Engine as the first container runtime, behind a `ContainerRuntime` interface

- Status: Accepted
- Date: 2026-08-16

## Context

The worker agent needs to pull images and start/stop/restart/health-check containers. Docker Engine is the fastest path to a working data plane and has the best-documented Go SDK. But the project's stated goal is to understand the *abstraction*, not permanently couple to one runtime, and containerd/OCI are explicitly called out as a later evaluation.

## Decision

Define a `ContainerRuntime` interface in `internal/runtime` (methods roughly: `PullImage`, `CreateContainer`, `StartContainer`, `StopContainer`, `RemoveContainer`, `ContainerStatus`, `StreamLogs`) before writing any worker-agent logic against it. Implement it once, backed by the Docker Engine API via the official Go SDK, in Phase 1. All worker-agent code depends on the interface, never on the Docker SDK types directly outside the implementation package.

## Consequences

- Positive: a future containerd/OCI implementation is a new file implementing one interface, not a rewrite of scheduling, health-checking, or log-streaming logic.
- Positive: the interface boundary is a natural place to unit-test worker-agent logic against a fake runtime, without needing a real Docker daemon in every test.
- Negative: slight upfront design cost — the interface must be designed carefully enough (streaming logs, exec, resource limits) that a runtime swap later doesn't require reshaping it. Treated as acceptable, bounded cost.

## Alternatives considered

- **Couple directly to the Docker SDK everywhere**: faster initially, but contradicts an explicit project goal and makes the containerd evaluation a rewrite instead of an addition.
- **Start with containerd directly**: rejected for Phase 1 — Docker's tooling/documentation/local-dev ergonomics are better for a first working system, and the interface makes this a reversible choice.

## Related

`ARCHITECTURE.md` §2.4, §8.

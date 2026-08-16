# ADR 0002: Go as the primary language for all infrastructure services

- Status: Accepted
- Date: 2026-08-16

## Context

The API server, scheduler, controller manager, worker agent, and load balancer all need: cheap concurrency (heartbeats, reconciliation loops, proxying many connections), easy static-binary distribution to worker nodes, and a good story for talking to Docker and eventually gRPC/protobuf.

## Decision

Use Go for every control-plane and data-plane service. TypeScript/Next.js is used only for the dashboard (a client of the API, not an infrastructure component) — see ADR-0007 for the API boundary between them.

## Consequences

- Positive: goroutines map directly onto "one loop per reconciled resource type" and "one heartbeat per node" without needing a separate async runtime or thread pool abstraction.
- Positive: worker agents ship as a single static binary — no runtime dependency to install on a node beyond the binary itself and Docker.
- Positive: Kubernetes and Nomad are both written in Go; idioms, libraries (client-go patterns, controller-runtime concepts), and prior art transfer directly, which matters for a project whose explicit goal is to build understanding of how those systems work.
- Negative: team (or future solo-developer-you) needs to be comfortable in Go across every backend service — no polyglot flexibility to pick "the right tool" per service.

## Alternatives considered

- **Rust**: stronger compile-time guarantees, but slower iteration speed and a smaller ecosystem of ready-made libraries (Docker SDK, NATS client maturity) for this scope.
- **Java/JVM**: mature ecosystem, but heavier runtime footprint per worker-agent instance and slower startup — a bad fit for a binary meant to run lightly on every node.
- **Node.js** for infra services too: rejected — single-threaded event loop is a worse fit for CPU-adjacent work like scheduling/reconciliation than goroutines, and static-binary distribution isn't native.

## Related

`ARCHITECTURE.md` §8 (technology justification table).

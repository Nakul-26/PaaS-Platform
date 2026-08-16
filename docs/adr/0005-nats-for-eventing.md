# ADR 0005: NATS for internal eventing and work distribution

- Status: Accepted
- Date: 2026-08-16

## Context

Several relationships in the system have a "one producer, N independent unknown-in-advance consumers" shape: a deployment state change needs to reach the reconciliation controller, the audit log, and (eventually) webhook subscribers, without the API server knowing or caring who's listening. Similarly, scheduling assignments need to reach whichever worker owns a given node, and service-registry updates need to reach the load balancer quickly without polling Postgres on every request.

A message broker is not being introduced for its own sake — it's justified specifically by this pub/sub shape, which plain request/response REST calls don't solve without the producer maintaining a manually-managed list of consumers.

## Decision

Use NATS (core NATS for ephemeral/lossy-tolerant traffic like heartbeats; JetStream for anything requiring at-least-once delivery, like crash/state-change events) as the internal event bus and work-distribution mechanism.

## Consequences

- Positive: single small Go binary, minimal operational overhead compared to Kafka or RabbitMQ — appropriate for a small-team/solo project.
- Positive: core NATS's subject-based pub/sub maps directly onto "publish to `node.<id>.assign`" and "subscribe to `service.updated`" without extra infrastructure.
- Positive: JetStream is opt-in per-stream, so durability is paid for only where it's actually needed, not globally.
- Negative: introduces a new distributed dependency — every NATS-dependent read path must have a fallback (see ADR-0001's data-plane-independence principle and `ARCHITECTURE.md` §9 R7). NATS being down must degrade the system, not break it.
- Negative: no built-in web UI/tooling ecosystem as mature as Kafka's — acceptable tradeoff given the operational savings.

## Alternatives considered

- **RabbitMQ**: solid pub/sub and work-queue semantics, but heavier operationally (Erlang runtime, more config surface) for the scale this project targets.
- **Kafka**: over-provisioned for this project's throughput; operational cost (ZooKeeper/KRaft, partition management) far exceeds the benefit at this scale.
- **Redis Streams**: viable lightweight alternative; NATS chosen for cleaner subject-based routing semantics that map more directly onto per-node/per-service addressing than Redis's stream-key model.

## Related

`ARCHITECTURE.md` §3 (communication table), §9 R7.

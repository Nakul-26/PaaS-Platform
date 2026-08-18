// Package eventbus defines the EventBus interface (ADR-0005, ADR-0011) for
// internal pub/sub and work distribution, and the NATS adapter that
// implements it. Subjects and message schemas are documented in
// docs/nats-contract.md, not here — this package is the transport, not the
// domain.
package eventbus

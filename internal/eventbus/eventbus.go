package eventbus

import "context"

// Message is a received event, independent of whether it arrived over core
// NATS or JetStream.
type Message struct {
	Subject string
	Data    []byte
}

// Handler processes a core (lossy, fire-and-forget) message.
type Handler func(msg Message)

// DurableHandler processes a JetStream message. A nil return acknowledges
// the message; a non-nil return negatively acknowledges it, requesting
// redelivery.
type DurableHandler func(msg Message) error

// Subscription is a live subscription (core or durable) that can be torn
// down independently of the EventBus itself.
type Subscription interface {
	Unsubscribe() error
}

// StreamConfig describes a JetStream stream to ensure exists before
// publishing or subscribing durably against it.
type StreamConfig struct {
	Name     string
	Subjects []string
}

// EventBus is the port through which services publish and consume events
// (ADR-0005, ADR-0011), independent of the underlying broker. NATS is the
// first and only adapter; the interface is deliberately narrow — it
// exposes the two delivery shapes ADR-0005 actually calls for (lossy core
// pub/sub, at-least-once JetStream) rather than the full NATS client
// surface.
type EventBus interface {
	// Publish sends data on subject over core NATS: at-most-once, no
	// persistence. Appropriate for heartbeats and other traffic where a
	// dropped message is tolerable.
	Publish(ctx context.Context, subject string, data []byte) error

	// Subscribe registers handler for every message published to subject
	// over core NATS.
	Subscribe(subject string, handler Handler) (Subscription, error)

	// EnsureStream creates or updates a JetStream stream so that
	// PublishDurable/SubscribeDurable against its subjects have somewhere
	// to persist to. Idempotent.
	EnsureStream(ctx context.Context, cfg StreamConfig) error

	// PublishDurable sends data on subject via JetStream and waits for the
	// server's acknowledgement that it was persisted. subject must be
	// covered by a stream already created via EnsureStream. Appropriate
	// for events where loss is unacceptable (e.g. placement assignments).
	PublishDurable(ctx context.Context, subject string, data []byte) error

	// SubscribeDurable creates (or reattaches to) a durable JetStream
	// consumer named consumerName on streamName, filtered to subject, and
	// delivers messages to handler with at-least-once semantics —
	// handler's error/nil return controls redelivery (see DurableHandler).
	SubscribeDurable(ctx context.Context, streamName, consumerName, subject string, handler DurableHandler) (Subscription, error)

	// Close releases the underlying connection.
	Close() error
}

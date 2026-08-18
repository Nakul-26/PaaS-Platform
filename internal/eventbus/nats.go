package eventbus

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// natsBus is the (currently only) EventBus adapter (ADR-0011), backed by a
// single NATS connection shared between core pub/sub and JetStream.
type natsBus struct {
	conn *nats.Conn
	js   jetstream.JetStream
}

// Connect dials the NATS server at url and returns an EventBus backed by
// it. The caller owns the returned EventBus and must Close it.
func Connect(url string) (EventBus, error) {
	conn, err := nats.Connect(url)
	if err != nil {
		return nil, fmt.Errorf("connecting to nats at %s: %w", url, err)
	}

	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("initializing jetstream context: %w", err)
	}

	return &natsBus{conn: conn, js: js}, nil
}

func (b *natsBus) Publish(_ context.Context, subject string, data []byte) error {
	if err := b.conn.Publish(subject, data); err != nil {
		return fmt.Errorf("publishing to %s: %w", subject, err)
	}
	return nil
}

func (b *natsBus) Subscribe(subject string, handler Handler) (Subscription, error) {
	sub, err := b.conn.Subscribe(subject, func(msg *nats.Msg) {
		handler(Message{Subject: msg.Subject, Data: msg.Data})
	})
	if err != nil {
		return nil, fmt.Errorf("subscribing to %s: %w", subject, err)
	}
	return coreSubscription{sub}, nil
}

func (b *natsBus) EnsureStream(ctx context.Context, cfg StreamConfig) error {
	if _, err := b.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     cfg.Name,
		Subjects: cfg.Subjects,
	}); err != nil {
		return fmt.Errorf("ensuring stream %s: %w", cfg.Name, err)
	}
	return nil
}

func (b *natsBus) PublishDurable(ctx context.Context, subject string, data []byte) error {
	if _, err := b.js.Publish(ctx, subject, data); err != nil {
		return fmt.Errorf("publishing durably to %s: %w", subject, err)
	}
	return nil
}

func (b *natsBus) SubscribeDurable(ctx context.Context, streamName, consumerName, subject string, handler DurableHandler) (Subscription, error) {
	consumer, err := b.js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		Durable:       consumerName,
		FilterSubject: subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		return nil, fmt.Errorf("creating durable consumer %s on stream %s: %w", consumerName, streamName, err)
	}

	consumeCtx, err := consumer.Consume(func(msg jetstream.Msg) {
		if err := handler(Message{Subject: msg.Subject(), Data: msg.Data()}); err != nil {
			_ = msg.Nak()
			return
		}
		_ = msg.Ack()
	})
	if err != nil {
		return nil, fmt.Errorf("consuming from durable consumer %s: %w", consumerName, err)
	}
	return durableSubscription{consumeCtx}, nil
}

func (b *natsBus) Close() error {
	b.conn.Close()
	return nil
}

type coreSubscription struct {
	sub *nats.Subscription
}

func (s coreSubscription) Unsubscribe() error {
	return s.sub.Unsubscribe()
}

type durableSubscription struct {
	cc jetstream.ConsumeContext
}

func (s durableSubscription) Unsubscribe() error {
	s.cc.Stop()
	return nil
}

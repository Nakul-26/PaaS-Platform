//go:build integration

package eventbus

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/nats"
)

// startTestNATS starts a real NATS container (JetStream enabled by the
// module's default command) and returns a connected EventBus.
//
//	go test -tags=integration ./internal/eventbus/...
func startTestNATS(t *testing.T, ctx context.Context) EventBus {
	t.Helper()

	container, err := nats.Run(ctx, "nats:2.11.7")
	if err != nil {
		t.Fatalf("starting nats container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	connStr, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("nats connection string: %v", err)
	}

	bus, err := Connect(connStr)
	if err != nil {
		t.Fatalf("connecting eventbus: %v", err)
	}
	t.Cleanup(func() { _ = bus.Close() })

	return bus
}

// TestNATSBus_CoreRoundTrip covers Task 1's acceptance: a message published
// over core NATS is delivered to a subscriber.
func TestNATSBus_CoreRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	bus := startTestNATS(t, ctx)

	received := make(chan Message, 1)
	sub, err := bus.Subscribe("test.core.subject", func(msg Message) {
		received <- msg
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	if err := bus.Publish(ctx, "test.core.subject", []byte("hello")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case msg := <-received:
		if string(msg.Data) != "hello" {
			t.Fatalf("expected data %q, got %q", "hello", msg.Data)
		}
		if msg.Subject != "test.core.subject" {
			t.Fatalf("expected subject %q, got %q", "test.core.subject", msg.Subject)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for core message")
	}
}

// TestNATSBus_DurableRoundTrip covers the JetStream side of the wrapper:
// EnsureStream, PublishDurable and SubscribeDurable round-trip a message
// with at-least-once delivery.
func TestNATSBus_DurableRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	bus := startTestNATS(t, ctx)

	const streamName = "TEST_STREAM"
	if err := bus.EnsureStream(ctx, StreamConfig{
		Name:     streamName,
		Subjects: []string{"test.durable.>"},
	}); err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}

	received := make(chan Message, 1)
	sub, err := bus.SubscribeDurable(ctx, streamName, "test-consumer", "test.durable.subject", func(msg Message) error {
		received <- msg
		return nil
	})
	if err != nil {
		t.Fatalf("SubscribeDurable: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	if err := bus.PublishDurable(ctx, "test.durable.subject", []byte("world")); err != nil {
		t.Fatalf("PublishDurable: %v", err)
	}

	select {
	case msg := <-received:
		if string(msg.Data) != "world" {
			t.Fatalf("expected data %q, got %q", "world", msg.Data)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for durable message")
	}
}

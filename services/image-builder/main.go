// Command image-builder clones a Git repo, builds a Docker image from a
// Dockerfile at its root, and pushes it to a registry (ARCHITECTURE.md
// §2.10, phase-7-deployment-platform.md). It consumes build.requested and
// publishes build.completed over NATS (docs/nats-contract.md) — never
// called directly by apiserver (ADR-0012), and it never touches Postgres
// at all (its NATS consumer's own doc comment): every outcome it reports
// is written to the builds table by apiserver's own build.completed
// consumer, not by this process.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"platform/internal/config"
	"platform/internal/eventbus"
	"platform/services/image-builder/internal/imagebuilder"
)

func main() {
	logger := slog.Default()

	builder, err := imagebuilder.NewDockerBuilder()
	if err != nil {
		logger.Error("connecting to docker daemon", "error", err)
		os.Exit(1)
	}
	defer func() { _ = builder.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	bus := connectEventBus(ctx, logger)
	if bus == nil {
		logger.Error("image-builder: giving up connecting to nats, cannot function without it")
		os.Exit(1)
	}
	defer func() { _ = bus.Close() }()

	consumer := imagebuilder.NewConsumer(bus, builder, logger)
	sub, err := consumer.Subscribe(ctx)
	if err != nil {
		logger.Error("image-builder: subscribing to build.requested", "error", err)
		os.Exit(1)
	}
	defer func() { _ = sub.Unsubscribe() }()

	logger.Info("image-builder: ready")
	<-ctx.Done()
	logger.Info("image-builder: shutting down")
}

// connectEventBus dials NATS with a short retry loop, mirroring the
// scheduler's own connectEventBus (services/scheduler/main.go) —
// docker-compose brings nats and image-builder up concurrently with no
// exec healthcheck to gate on. Like the scheduler (and unlike the worker),
// NATS is not optional here: image-builder's entire job is NATS-driven, so
// a nil return is fatal to main.
func connectEventBus(ctx context.Context, logger *slog.Logger) eventbus.EventBus {
	url := config.String("IMAGE_BUILDER_NATS_URL", "nats://127.0.0.1:4222")
	deadline := time.Now().Add(30 * time.Second)
	for {
		bus, err := eventbus.Connect(url)
		if err == nil {
			return bus
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			logger.Error("image-builder: nats not reachable", "url", url, "error", err)
			return nil
		}
		logger.Warn("image-builder: nats not reachable yet, retrying", "url", url, "error", err)
		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
			return nil
		}
	}
}

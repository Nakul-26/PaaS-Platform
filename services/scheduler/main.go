// Command scheduler places workloads onto worker nodes (ARCHITECTURE.md
// §2.2). It talks to Postgres directly (internal/db) and to workers only
// over the published NATS contract (docs/nats-contract.md, ADR-0012) —
// never a worker's package tree. Single instance only for Phase 2
// (ARCHITECTURE.md §9 R3: no leader election yet).
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"platform/internal/config"
	"platform/internal/db"
	"platform/internal/eventbus"
	"platform/services/scheduler/internal/scheduler"
)

func main() {
	logger := slog.Default()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dbURL := config.String("APP_DATABASE_URL", "postgres://platform_app:platform_app@localhost:5432/platform?sslmode=disable")
	pool, err := db.Open(ctx, dbURL)
	if err != nil {
		logger.Error("connecting to postgres", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	bus := connectEventBus(ctx, logger)
	if bus == nil {
		logger.Error("scheduler: giving up connecting to nats, cannot function without it")
		os.Exit(1)
	}
	defer func() { _ = bus.Close() }()

	sched := scheduler.New(bus, db.NewNodeRepository(pool.Conn()), db.NewContainerRepository(pool.Conn()), scheduler.Config{
		HeartbeatTimeout:      config.Duration("SCHEDULER_HEARTBEAT_TIMEOUT", 15*time.Second),
		LivenessSweepInterval: config.Duration("SCHEDULER_LIVENESS_SWEEP_INTERVAL", 5*time.Second),
	}, logger)

	logger.Info("scheduler: starting")
	if err := sched.Run(ctx); err != nil {
		logger.Error("scheduler: run failed", "error", err)
		os.Exit(1)
	}
	logger.Info("scheduler: shutting down")
}

// connectEventBus dials NATS with a short retry loop, mirroring the
// worker's own connectEventBus (services/worker/main.go) — docker-compose
// brings nats and scheduler up concurrently with no exec healthcheck to
// gate on. Unlike the worker, NATS is not optional here: the scheduler's
// entire job is NATS-driven, so a nil return is fatal to main, not a
// degrade-gracefully case.
func connectEventBus(ctx context.Context, logger *slog.Logger) eventbus.EventBus {
	url := config.String("SCHEDULER_NATS_URL", "nats://127.0.0.1:4222")
	deadline := time.Now().Add(30 * time.Second)
	for {
		bus, err := eventbus.Connect(url)
		if err == nil {
			return bus
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			logger.Error("scheduler: nats not reachable", "url", url, "error", err)
			return nil
		}
		logger.Warn("scheduler: nats not reachable yet, retrying", "url", url, "error", err)
		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
			return nil
		}
	}
}

// Command controller-manager runs desired-state reconciliation loops
// (ARCHITECTURE.md §2.3, phase-3-controllers.md Task 4). Talks to Postgres
// directly (internal/db) and to workers/scheduler only over the published
// NATS contract (docs/nats-contract.md, ADR-0012). Single instance only for
// Phase 3 (ARCHITECTURE.md §9 R3's posture, same constraint Phase 2 placed
// on the scheduler) — no leader election yet.
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
	"platform/services/controller-manager/internal/controller"
)

func main() {
	logger := slog.Default()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Connects as the Postgres superuser (DATABASE_URL) rather than the
	// RLS-scoped platform_app role apiserver/scheduler use for
	// tenant-scoped tables: the Deployment controller has no per-request
	// user session to key app.current_org_id off, and its job is
	// inherently cross-tenant — every tick, every application in the
	// system (db.ReconcileRepository's doc comment, ADR-0010's documented
	// RLS-bypass exception).
	dbURL := config.String("DATABASE_URL", "postgres://platform:platform@localhost:5432/platform?sslmode=disable")
	pool, err := db.Open(ctx, dbURL)
	if err != nil {
		logger.Error("connecting to postgres", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	bus := connectEventBus(ctx, logger)
	if bus == nil {
		logger.Error("controller-manager: giving up connecting to nats, cannot function without it")
		os.Exit(1)
	}
	defer func() { _ = bus.Close() }()

	ctrl := controller.New(bus, db.NewReconcileRepository(pool.Conn()), db.NewContainerRepository(pool.Conn()), controller.Config{
		ReconcileInterval: config.Duration("CONTROLLER_RECONCILE_INTERVAL", 5*time.Second),
	}, logger)

	logger.Info("controller-manager: starting")
	if err := ctrl.Run(ctx); err != nil {
		logger.Error("controller-manager: run failed", "error", err)
		os.Exit(1)
	}
	logger.Info("controller-manager: shutting down")
}

// connectEventBus dials NATS with a short retry loop, mirroring the
// scheduler's own connectEventBus (services/scheduler/main.go) — NATS is
// not optional here, so a nil return is fatal to main, not a
// degrade-gracefully case.
func connectEventBus(ctx context.Context, logger *slog.Logger) eventbus.EventBus {
	url := config.String("CONTROLLER_NATS_URL", "nats://127.0.0.1:4222")
	deadline := time.Now().Add(30 * time.Second)
	for {
		bus, err := eventbus.Connect(url)
		if err == nil {
			return bus
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			logger.Error("controller-manager: nats not reachable", "url", url, "error", err)
			return nil
		}
		logger.Warn("controller-manager: nats not reachable yet, retrying", "url", url, "error", err)
		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
			return nil
		}
	}
}

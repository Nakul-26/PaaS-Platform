// Command worker is the per-node agent that drives containers via the
// ContainerRuntime interface (ARCHITECTURE.md §2.4). It exposes an internal
// HTTP contract (docs/worker-agent-contract.md) that the API server calls
// per ADR-0012 — Phase 1 has exactly one worker, called directly over HTTP;
// Phase 2 adds NATS-based assignment as an additive transport for the same
// contract, not a replacement for it.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"platform/internal/config"
	"platform/internal/runtime"
	"platform/services/worker/internal/server"
)

func main() {
	logger := slog.Default()

	rt, err := runtime.NewDockerRuntime()
	if err != nil {
		logger.Error("connecting to docker daemon", "error", err)
		os.Exit(1)
	}
	defer func() { _ = rt.Close() }()

	addr := config.String("WORKER_LISTEN_ADDR", ":7100")
	httpServer := &http.Server{
		Addr:              addr,
		Handler:           server.New(rt, logger).Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("worker: listening", "addr", addr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("worker: http server error", "error", err)
		}
	}()

	<-ctx.Done()
	logger.Info("worker: shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("worker: graceful shutdown failed", "error", err)
	}
}

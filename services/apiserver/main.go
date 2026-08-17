// Command apiserver is the public REST API and control-plane entry point
// (ARCHITECTURE.md §2.1). It talks to Postgres directly (internal/db,
// ADR-0010) and to the worker agent only over its published HTTP contract
// (services/apiserver/internal/workerclient, ADR-0012) — Phase 1 has
// exactly one worker, called directly (phase-1-mvp.md Task 5).
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

	"platform/internal/auth"
	"platform/internal/config"
	"platform/internal/db"
	"platform/services/apiserver/internal/server"
	"platform/services/apiserver/internal/workerclient"
)

func main() {
	logger := slog.Default()

	signingKey := config.String("JWT_SIGNING_KEY", "")
	issuer, err := auth.NewTokenIssuer(signingKey)
	if err != nil {
		logger.Error("configuring token issuer", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dbURL := config.String("APP_DATABASE_URL", "postgres://platform_app:platform_app@localhost:5432/platform?sslmode=disable")
	pool, err := db.Open(ctx, dbURL)
	if err != nil {
		logger.Error("connecting to postgres", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	worker := workerclient.New(config.String("WORKER_ADDR", "http://localhost:7100"))

	addr := config.String("APISERVER_LISTEN_ADDR", ":8080")
	httpServer := &http.Server{
		Addr:              addr,
		Handler:           server.New(pool, issuer, worker, logger).Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Info("apiserver: listening", "addr", addr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("apiserver: http server error", "error", err)
		}
	}()

	<-ctx.Done()
	logger.Info("apiserver: shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("apiserver: graceful shutdown failed", "error", err)
	}
}

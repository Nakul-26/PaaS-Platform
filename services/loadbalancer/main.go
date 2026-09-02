// Command loadbalancer is the tenant-facing reverse proxy (ARCHITECTURE.md
// §2.5, phase-4-service-discovery-lb.md Task 4). Talks to Postgres directly
// (internal/db) for its periodic full resync and to controller-manager only
// over the published NATS contract (docs/nats-contract.md, ADR-0012) — the
// request hot path itself never touches Postgres or the API server, only
// the in-memory registry (§2.5, §2.6).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"

	"platform/internal/config"
	"platform/internal/db"
	"platform/internal/eventbus"
	"platform/services/loadbalancer/internal/healthcheck"
	"platform/services/loadbalancer/internal/proxy"
	"platform/services/loadbalancer/internal/registry"
	"platform/services/loadbalancer/internal/strategy"
)

func main() {
	logger := slog.Default()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// services/service_instances carry no RLS (database-schema.md), same
	// precedent as nodes/containers, so this connects as the RLS-scoped
	// platform_app role apiserver/scheduler already use — no superuser
	// connection needed here, unlike controller-manager.
	dbURL := config.String("APP_DATABASE_URL", "postgres://platform_app:platform_app@localhost:5432/platform?sslmode=disable")
	pool, err := db.Open(ctx, dbURL)
	if err != nil {
		logger.Error("connecting to postgres", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	lbRepo := db.NewLoadBalancerRepository(pool.Conn())

	bus := connectEventBus(ctx, logger)
	if bus == nil {
		logger.Error("loadbalancer: giving up connecting to nats, cannot function without it")
		os.Exit(1)
	}
	defer func() { _ = bus.Close() }()

	reg := registry.New(strategy.NewRoundRobin())

	// Synchronous first resync so the registry isn't empty for whatever
	// request arrives the instant this process starts serving.
	resync(ctx, lbRepo, reg, logger)

	sub, err := bus.Subscribe(eventbus.ServiceUpdatedSubject, func(msg eventbus.Message) {
		handleServiceUpdated(ctx, lbRepo, reg, logger, msg.Data)
	})
	if err != nil {
		logger.Error("loadbalancer: subscribing to service.updated", "error", err)
		os.Exit(1)
	}
	defer func() { _ = sub.Unsubscribe() }()

	resyncInterval := config.Duration("LOADBALANCER_RESYNC_INTERVAL", 30*time.Second)
	go func() {
		ticker := time.NewTicker(resyncInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				resync(ctx, lbRepo, reg, logger)
			}
		}
	}()

	// Task 5's TTL safety net (ARCHITECTURE.md §2.6): independent of
	// whether resync/service.updated are currently working at all, an
	// instance not refreshed within instanceTTL is dropped outright.
	// instanceTTL is deliberately a multiple of resyncInterval (R9
	// placeholder posture, same as every other interval this codebase
	// tunes) so a single missed resync never trips it under normal
	// operation.
	instanceTTL := config.Duration("LOADBALANCER_INSTANCE_TTL", 3*resyncInterval)
	go func() {
		ticker := time.NewTicker(instanceTTL / 3)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				reg.Sweep(instanceTTL)
			}
		}
	}()

	// Task 5's active health checking (ARCHITECTURE.md §2.5): independent
	// of, and faster than, Task 3's Postgres-driven reconcile tick.
	healthCheckInterval := config.Duration("LOADBALANCER_HEALTH_CHECK_INTERVAL", 5*time.Second)
	healthCheckPath := config.String("LOADBALANCER_HEALTH_CHECK_PATH", "/")
	checker := healthcheck.New(reg, healthCheckPath, healthCheckInterval, 3*time.Second, logger)
	go checker.Run(ctx)

	// Task 5's passive removal (ARCHITECTURE.md §2.5): how long a backend
	// that just failed a proxied request is skipped before being
	// reconsidered, independent of the active checker's own interval.
	ejectionWindow := config.Duration("LOADBALANCER_EJECTION_WINDOW", 10*time.Second)

	addr := config.String("LOADBALANCER_LISTEN_ADDR", ":8090")
	httpServer := &http.Server{
		Addr:              addr,
		Handler:           proxy.New(reg, ejectionWindow, logger),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Info("loadbalancer: listening", "addr", addr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("loadbalancer: http server error", "error", err)
		}
	}()

	<-ctx.Done()
	logger.Info("loadbalancer: shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("loadbalancer: graceful shutdown failed", "error", err)
	}
}

// resync rebuilds the registry from scratch from Postgres — the periodic
// fallback (§2.6) for a missed service.updated event. A query failure
// leaves the previous registry contents in place (stale-but-serving beats
// empty-and-refusing-everything) rather than clearing the registry.
func resync(ctx context.Context, repo db.LoadBalancerRepository, reg *registry.Registry, logger *slog.Logger) {
	entries, err := repo.ListHealthyRegistry(ctx)
	if err != nil {
		logger.Error("loadbalancer: resync failed, keeping previous registry contents", "error", err)
		return
	}
	byDNS := make(map[string][]registry.Instance)
	for _, e := range entries {
		byDNS[e.DNSName] = append(byDNS[e.DNSName], registry.Instance{
			ContainerID: e.ContainerID,
			IP:          e.IP,
			Port:        e.Port,
		})
	}
	reg.ReplaceAll(byDNS)
}

// updatedMessage is service.updated's payload (docs/nats-contract.md,
// mirroring controller-manager's own unexported copy — ADR-0012, depend
// only on the published contract, never another service's package tree).
type updatedMessage struct {
	ContainerID string `json:"container_id"`
}

// handleServiceUpdated resolves one service.updated event into a targeted
// point lookup (never a full resync — that's the periodic fallback's job)
// and applies it to the registry: a healthy instance is upserted, anything
// else (removed, or turned unhealthy) is evicted.
func handleServiceUpdated(ctx context.Context, repo db.LoadBalancerRepository, reg *registry.Registry, logger *slog.Logger, data []byte) {
	var msg updatedMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		logger.Error("loadbalancer: decoding service.updated", "error", err)
		return
	}
	containerID, err := uuid.Parse(msg.ContainerID)
	if err != nil {
		logger.Error("loadbalancer: invalid container_id in service.updated", "container_id", msg.ContainerID, "error", err)
		return
	}

	entry, ok, err := repo.GetHealthyInstanceByContainer(ctx, containerID)
	if err != nil {
		logger.Error("loadbalancer: looking up instance for service.updated", "container_id", containerID, "error", err)
		return
	}
	if !ok {
		reg.Remove(containerID)
		return
	}
	reg.Upsert(entry.DNSName, registry.Instance{
		ContainerID: entry.ContainerID,
		IP:          entry.IP,
		Port:        entry.Port,
	})
}

// connectEventBus dials NATS with a short retry loop, mirroring
// scheduler's/controller-manager's own connectEventBus. Not optional here:
// the registry's freshness depends on it, so a nil return is fatal to main.
func connectEventBus(ctx context.Context, logger *slog.Logger) eventbus.EventBus {
	url := config.String("LOADBALANCER_NATS_URL", "nats://127.0.0.1:4222")
	deadline := time.Now().Add(30 * time.Second)
	for {
		bus, err := eventbus.Connect(url)
		if err == nil {
			return bus
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			logger.Error("loadbalancer: nats not reachable", "url", url, "error", err)
			return nil
		}
		logger.Warn("loadbalancer: nats not reachable yet, retrying", "url", url, "error", err)
		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
			return nil
		}
	}
}

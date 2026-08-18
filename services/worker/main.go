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
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"platform/internal/config"
	"platform/internal/eventbus"
	"platform/internal/runtime"
	"platform/services/worker/internal/nodeagent"
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

	bus := connectEventBus(ctx, logger)
	if bus != nil {
		defer func() { _ = bus.Close() }()
		go runNodeAgent(ctx, bus, logger)
	}

	<-ctx.Done()
	logger.Info("worker: shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("worker: graceful shutdown failed", "error", err)
	}
}

// connectEventBus dials NATS with a short retry loop — docker-compose brings
// nats and worker up concurrently with no way to healthcheck-gate on the nats
// image (infrastructure/docker-compose.yml's comment on the nats service),
// so the worker is expected to be the side that waits. Returns nil (not a
// fatal error) if NATS never becomes reachable: node registration/heartbeat
// is additive to the worker's HTTP contract, not a prerequisite for it.
func connectEventBus(ctx context.Context, logger *slog.Logger) eventbus.EventBus {
	url := config.String("WORKER_NATS_URL", "nats://127.0.0.1:4222")
	deadline := time.Now().Add(30 * time.Second)
	for {
		bus, err := eventbus.Connect(url)
		if err == nil {
			return bus
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			logger.Error("worker: giving up connecting to nats, node registration/heartbeat disabled", "url", url, "error", err)
			return nil
		}
		logger.Warn("worker: nats not reachable yet, retrying", "url", url, "error", err)
		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
			return nil
		}
	}
}

// runNodeAgent runs the node.<id>.register/heartbeat loop (phase-2-multi-node.md
// Task 3) until ctx is done.
func runNodeAgent(ctx context.Context, bus eventbus.EventBus, logger *slog.Logger) {
	nodeIDFile := config.String("WORKER_NODE_ID_FILE", "worker-node-id")
	nodeID, err := nodeagent.LoadOrCreateNodeID(nodeIDFile)
	if err != nil {
		logger.Error("worker: loading node id, node registration/heartbeat disabled", "path", nodeIDFile, "error", err)
		return
	}

	hostname := config.String("WORKER_HOSTNAME", "")
	if hostname == "" {
		if h, err := os.Hostname(); err == nil {
			hostname = h
		}
	}

	ip := config.String("WORKER_IP", "")
	if ip == "" {
		ip = outboundIP()
	}

	agent := nodeagent.New(bus, nodeagent.Config{
		NodeID:                nodeID,
		Hostname:              hostname,
		IP:                    ip,
		CPUCapacityMillicores: config.Int("WORKER_CPU_CAPACITY_MILLICORES", 2000),
		MemoryCapacityMB:      config.Int("WORKER_MEMORY_CAPACITY_MB", 2048),
		HeartbeatInterval:     config.Duration("WORKER_HEARTBEAT_INTERVAL", 5*time.Second),
	}, logger)

	logger.Info("worker: starting node agent", "node_id", nodeID, "hostname", hostname, "ip", ip)
	_ = agent.Run(ctx)
}

// outboundIP picks the local address the OS would route traffic to a public
// host through. UDP dial here never sends a packet — it only consults the
// routing table to pick a local source address — so this works offline too.
func outboundIP() string {
	conn, err := net.Dial("udp4", "8.8.8.8:80")
	if err != nil {
		return "127.0.0.1"
	}
	defer func() { _ = conn.Close() }()
	return conn.LocalAddr().(*net.UDPAddr).IP.String()
}

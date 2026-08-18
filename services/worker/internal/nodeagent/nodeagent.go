// Package nodeagent runs the worker's node.<id>.register/heartbeat loop
// over NATS (docs/nats-contract.md, phase-2-multi-node.md Task 3). It is
// purely additive to the worker's HTTP contract (worker-agent-contract.md,
// services/worker/internal/server) — the two run side by side, neither
// depends on the other.
package nodeagent

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"platform/internal/eventbus"
)

// Config describes this worker's identity and capacity, as published on
// node.<id>.register.
type Config struct {
	NodeID                uuid.UUID
	Hostname              string
	IP                    string
	CPUCapacityMillicores int
	MemoryCapacityMB      int
	Labels                map[string]string
	HeartbeatInterval     time.Duration
}

// Agent owns the register-then-heartbeat loop for one worker process.
type Agent struct {
	bus    eventbus.EventBus
	cfg    Config
	logger *slog.Logger
}

func New(bus eventbus.EventBus, cfg Config, logger *slog.Logger) *Agent {
	if logger == nil {
		logger = slog.Default()
	}
	return &Agent{bus: bus, cfg: cfg, logger: logger}
}

type registerMessage struct {
	NodeID                string            `json:"node_id"`
	Hostname              string            `json:"hostname"`
	IP                    string            `json:"ip"`
	CPUCapacityMillicores int               `json:"cpu_capacity_millicores"`
	MemoryCapacityMB      int               `json:"memory_capacity_mb"`
	Labels                map[string]string `json:"labels"`
}

type heartbeatMessage struct {
	NodeID    string    `json:"node_id"`
	Timestamp time.Time `json:"timestamp"`
}

// Run publishes this worker's registration once, then heartbeats on
// cfg.HeartbeatInterval until ctx is done. Publish failures are logged, not
// fatal: both subjects are core NATS (docs/nats-contract.md's transport
// table) precisely because a lost message here is self-healing — the next
// heartbeat re-establishes the node.
func (a *Agent) Run(ctx context.Context) error {
	if err := a.publishRegister(ctx); err != nil {
		a.logger.Error("publishing node registration", "node_id", a.cfg.NodeID, "error", err)
	} else {
		a.logger.Info("node registered", "node_id", a.cfg.NodeID, "hostname", a.cfg.Hostname)
	}

	ticker := time.NewTicker(a.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := a.publishHeartbeat(ctx); err != nil {
				a.logger.Error("publishing node heartbeat", "node_id", a.cfg.NodeID, "error", err)
			}
		}
	}
}

func (a *Agent) publishRegister(ctx context.Context) error {
	labels := a.cfg.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	data, err := json.Marshal(registerMessage{
		NodeID:                a.cfg.NodeID.String(),
		Hostname:              a.cfg.Hostname,
		IP:                    a.cfg.IP,
		CPUCapacityMillicores: a.cfg.CPUCapacityMillicores,
		MemoryCapacityMB:      a.cfg.MemoryCapacityMB,
		Labels:                labels,
	})
	if err != nil {
		return fmt.Errorf("marshaling registration message: %w", err)
	}
	return a.bus.Publish(ctx, eventbus.NodeRegisterSubject(a.cfg.NodeID.String()), data)
}

func (a *Agent) publishHeartbeat(ctx context.Context) error {
	data, err := json.Marshal(heartbeatMessage{
		NodeID:    a.cfg.NodeID.String(),
		Timestamp: time.Now().UTC(),
	})
	if err != nil {
		return fmt.Errorf("marshaling heartbeat message: %w", err)
	}
	return a.bus.Publish(ctx, eventbus.NodeHeartbeatSubject(a.cfg.NodeID.String()), data)
}

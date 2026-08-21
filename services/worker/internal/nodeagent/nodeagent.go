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
	"sync"
	"time"

	"github.com/google/uuid"

	"platform/internal/eventbus"
	"platform/internal/runtime"
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
	// HealthCheckInterval is how often tracked containers (phase-3-controllers.md
	// Task 1) are polled for a status change. A placeholder default (3s, see
	// main.go) until Phase 10's load-testing pass tunes it for real (§9 R9).
	HealthCheckInterval time.Duration
}

// trackedContainer is what health-check polling needs to detect a status
// change for one assignment: which container to inspect, and the status it
// last reported so a poll only republishes on an actual transition.
type trackedContainer struct {
	containerID string
	lastStatus  string
}

// Agent owns the register/heartbeat/assign loop for one worker process.
type Agent struct {
	bus    eventbus.EventBus
	rt     runtime.ContainerRuntime
	cfg    Config
	logger *slog.Logger

	mu      sync.Mutex
	tracked map[string]*trackedContainer // assignment_id -> tracked container
}

func New(bus eventbus.EventBus, rt runtime.ContainerRuntime, cfg Config, logger *slog.Logger) *Agent {
	if logger == nil {
		logger = slog.Default()
	}
	return &Agent{bus: bus, rt: rt, cfg: cfg, logger: logger, tracked: make(map[string]*trackedContainer)}
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

// Run publishes this worker's registration once, subscribes to its own
// node.<id>.assign and node.<id>.unassign subjects, then heartbeats on
// cfg.HeartbeatInterval (and polls tracked containers on
// cfg.HealthCheckInterval) until ctx is done. Register/heartbeat publish
// failures are logged, not fatal: both subjects are core NATS
// (docs/nats-contract.md's transport table) precisely because a lost message
// here is self-healing — the next heartbeat re-establishes the node. Failing
// to set up either subscription, by contrast, is fatal to Run: without them
// this worker can never receive placements or releases.
func (a *Agent) Run(ctx context.Context) error {
	if err := a.publishRegister(ctx); err != nil {
		a.logger.Error("publishing node registration", "node_id", a.cfg.NodeID, "error", err)
	} else {
		a.logger.Info("node registered", "node_id", a.cfg.NodeID, "hostname", a.cfg.Hostname)
	}

	assignSub, err := a.subscribeAssignments(ctx)
	if err != nil {
		return fmt.Errorf("subscribing to assignments: %w", err)
	}
	defer func() { _ = assignSub.Unsubscribe() }()

	unassignSub, err := a.subscribeUnassign(ctx)
	if err != nil {
		return fmt.Errorf("subscribing to unassign: %w", err)
	}
	defer func() { _ = unassignSub.Unsubscribe() }()

	ticker := time.NewTicker(a.cfg.HeartbeatInterval)
	defer ticker.Stop()

	healthInterval := a.cfg.HealthCheckInterval
	if healthInterval <= 0 {
		healthInterval = 3 * time.Second
	}
	healthTicker := time.NewTicker(healthInterval)
	defer healthTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := a.publishHeartbeat(ctx); err != nil {
				a.logger.Error("publishing node heartbeat", "node_id", a.cfg.NodeID, "error", err)
			}
		case <-healthTicker.C:
			a.checkTrackedContainers(ctx)
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

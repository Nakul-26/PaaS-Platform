// Package scheduler implements the node registry and placement
// responsibilities of phase-2-multi-node.md Task 5 — this is the one
// scheduler instance allowed for Phase 2 (ARCHITECTURE.md §9 R3: no leader
// election yet, running two would risk double-scheduling the same
// placement).
package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"platform/internal/db"
	"platform/internal/eventbus"
)

// Config tunes the liveness sweep (phase-2-multi-node.md Task 5's node
// registry: "marks a node unreachable once last_heartbeat_at exceeds the
// configurable timeout").
type Config struct {
	HeartbeatTimeout      time.Duration
	LivenessSweepInterval time.Duration
}

// Scheduler owns the node-registry and placement loops for the single
// Phase 2 scheduler instance.
type Scheduler struct {
	bus        eventbus.EventBus
	nodes      db.NodeRepository
	containers db.ContainerRepository
	cfg        Config
	logger     *slog.Logger
}

func New(bus eventbus.EventBus, nodes db.NodeRepository, containers db.ContainerRepository, cfg Config, logger *slog.Logger) *Scheduler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Scheduler{bus: bus, nodes: nodes, containers: containers, cfg: cfg, logger: logger}
}

// Run subscribes the node registry (register/heartbeat) and placement
// (placement.requested/node.*.status) consumers, then runs the liveness
// sweep on cfg.LivenessSweepInterval until ctx is done. Any subscription
// failure is fatal: without them the scheduler cannot do its job at all.
func (s *Scheduler) Run(ctx context.Context) error {
	regSub, hbSub, err := s.subscribeRegistry(ctx)
	if err != nil {
		return fmt.Errorf("subscribing to node registry subjects: %w", err)
	}
	defer func() { _ = regSub.Unsubscribe() }()
	defer func() { _ = hbSub.Unsubscribe() }()

	placementSub, err := s.subscribePlacement(ctx)
	if err != nil {
		return fmt.Errorf("subscribing to placement.requested: %w", err)
	}
	defer func() { _ = placementSub.Unsubscribe() }()

	statusSub, err := s.subscribeStatus(ctx)
	if err != nil {
		return fmt.Errorf("subscribing to node status: %w", err)
	}
	defer func() { _ = statusSub.Unsubscribe() }()

	ticker := time.NewTicker(s.cfg.LivenessSweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.sweepUnreachable(ctx)
		}
	}
}

package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"

	"platform/internal/db"
	"platform/internal/eventbus"
)

// registerMessage/heartbeatMessage mirror nodeagent's own (unexported)
// message types — docs/nats-contract.md's node.<id>.register and
// node.<id>.heartbeat schemas. Per ADR-0012 the scheduler depends only on
// the published NATS contract, never the worker's package tree, so these
// are the scheduler's own copies.
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

// subscribeRegistry subscribes core NATS (docs/nats-contract.md: both
// subjects are self-healing on loss, not worth JetStream's overhead) to
// every node's register/heartbeat subjects via wildcard.
func (s *Scheduler) subscribeRegistry(ctx context.Context) (eventbus.Subscription, eventbus.Subscription, error) {
	regSub, err := s.bus.Subscribe("node.*.register", func(msg eventbus.Message) {
		s.handleRegister(ctx, msg.Data)
	})
	if err != nil {
		return nil, nil, err
	}

	hbSub, err := s.bus.Subscribe("node.*.heartbeat", func(msg eventbus.Message) {
		s.handleHeartbeat(ctx, msg.Data)
	})
	if err != nil {
		_ = regSub.Unsubscribe()
		return nil, nil, err
	}

	return regSub, hbSub, nil
}

func (s *Scheduler) handleRegister(ctx context.Context, data []byte) {
	var msg registerMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		s.logger.Error("decoding registration message", "error", err)
		return
	}

	id, err := uuid.Parse(msg.NodeID)
	if err != nil {
		s.logger.Error("registration message has non-uuid node_id", "node_id", msg.NodeID, "error", err)
		return
	}

	if _, err := s.nodes.Register(ctx, id, msg.Hostname, msg.IP, msg.CPUCapacityMillicores, msg.MemoryCapacityMB, msg.Labels); err != nil {
		s.logger.Error("registering node", "node_id", msg.NodeID, "error", err)
		return
	}
	s.logger.Info("node registered", "node_id", msg.NodeID, "hostname", msg.Hostname)
}

func (s *Scheduler) handleHeartbeat(ctx context.Context, data []byte) {
	var msg heartbeatMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		s.logger.Error("decoding heartbeat message", "error", err)
		return
	}

	id, err := uuid.Parse(msg.NodeID)
	if err != nil {
		s.logger.Error("heartbeat message has non-uuid node_id", "node_id", msg.NodeID, "error", err)
		return
	}

	if err := s.nodes.Heartbeat(ctx, id, msg.Timestamp); err != nil {
		// A heartbeat racing ahead of that same node's register message is
		// expected under normal startup ordering and self-heals on the next
		// heartbeat, so this is a warning, not an error.
		if errors.Is(err, db.ErrNotFound) {
			s.logger.Warn("heartbeat for unregistered node, will self-heal on next register/heartbeat", "node_id", msg.NodeID)
			return
		}
		s.logger.Error("recording node heartbeat", "node_id", msg.NodeID, "error", err)
	}
}

// sweepUnreachable marks any node whose last_heartbeat_at exceeds
// cfg.HeartbeatTimeout as unreachable (phase-2-multi-node.md Task 5) —
// the exit criterion this satisfies: kill a worker, watch it drop out of
// the (healthy) node list within one sweep interval.
func (s *Scheduler) sweepUnreachable(ctx context.Context) {
	nodes, err := s.nodes.List(ctx)
	if err != nil {
		s.logger.Error("listing nodes for liveness sweep", "error", err)
		return
	}

	cutoff := time.Now().Add(-s.cfg.HeartbeatTimeout)
	for _, n := range nodes {
		if n.Status == db.NodeStatusUnreachable || n.LastHeartbeatAt.After(cutoff) {
			continue
		}
		if err := s.nodes.SetStatus(ctx, n.ID, db.NodeStatusUnreachable); err != nil {
			s.logger.Error("marking node unreachable", "node_id", n.ID, "error", err)
			continue
		}
		s.logger.Warn("node marked unreachable", "node_id", n.ID, "hostname", n.Hostname, "last_heartbeat_at", n.LastHeartbeatAt)
	}
}

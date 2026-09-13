package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"platform/internal/db"
	"platform/internal/eventbus"
)

// placementRequestedMessage/portBinding mirror docs/nats-contract.md's
// placement.requested schema. assignMessage/statusMessage mirror
// nodeagent's own (unexported) types for node.<id>.assign/status — per
// ADR-0012 the scheduler depends only on the published NATS contract,
// never the worker's package tree.
type placementRequestedMessage struct {
	DeploymentID  string            `json:"deployment_id"`
	ApplicationID string            `json:"application_id"`
	Image         string            `json:"image"`
	Env           map[string]string `json:"env,omitempty"`
	Ports         []portBinding     `json:"ports,omitempty"`
	Command       []string          `json:"command,omitempty"`
}

type portBinding struct {
	ContainerPort int    `json:"container_port"`
	HostPort      int    `json:"host_port,omitempty"`
	Protocol      string `json:"protocol,omitempty"`
}

type assignMessage struct {
	AssignmentID string            `json:"assignment_id"`
	DeploymentID string            `json:"deployment_id"`
	Image        string            `json:"image"`
	Env          map[string]string `json:"env,omitempty"`
	Ports        []portBinding     `json:"ports,omitempty"`
	Command      []string          `json:"command,omitempty"`
}

type statusMessage struct {
	AssignmentID string    `json:"assignment_id"`
	ContainerID  string    `json:"container_id"`
	Status       string    `json:"status"`
	ExitCode     int       `json:"exit_code"`
	Timestamp    time.Time `json:"timestamp"`
}

// subscribePlacement ensures the streams placement.requested consumption
// and node.<id>.assign publication need, then subscribes durably to
// placement.requested (docs/nats-contract.md: loss would strand a
// deployment in pending forever).
func (s *Scheduler) subscribePlacement(ctx context.Context) (eventbus.Subscription, error) {
	if err := s.bus.EnsureStream(ctx, eventbus.StreamConfig{
		Name:     eventbus.PlacementStream,
		Subjects: []string{eventbus.PlacementStreamFilter},
	}); err != nil {
		return nil, fmt.Errorf("ensuring %s stream: %w", eventbus.PlacementStream, err)
	}
	if err := s.bus.EnsureStream(ctx, eventbus.StreamConfig{
		Name:     eventbus.NodeAssignmentsStream,
		Subjects: []string{eventbus.NodeAssignmentsStreamFilter},
	}); err != nil {
		return nil, fmt.Errorf("ensuring %s stream: %w", eventbus.NodeAssignmentsStream, err)
	}

	return s.bus.SubscribeDurable(ctx, eventbus.PlacementStream, "scheduler-placement", eventbus.PlacementRequestedSubject, func(msg eventbus.Message) error {
		s.handlePlacement(ctx, msg.Data)
		return nil
	})
}

// subscribeStatus ensures NODE_STATUS exists, then subscribes durably to
// every node's status subject via wildcard filter.
func (s *Scheduler) subscribeStatus(ctx context.Context) (eventbus.Subscription, error) {
	if err := s.bus.EnsureStream(ctx, eventbus.StreamConfig{
		Name:     eventbus.NodeStatusStream,
		Subjects: []string{eventbus.NodeStatusStreamFilter},
	}); err != nil {
		return nil, fmt.Errorf("ensuring %s stream: %w", eventbus.NodeStatusStream, err)
	}

	return s.bus.SubscribeDurable(ctx, eventbus.NodeStatusStream, "scheduler-status", eventbus.NodeStatusStreamFilter, func(msg eventbus.Message) error {
		s.handleStatus(ctx, msg.Data)
		return nil
	})
}

// handlePlacement applies filter-then-score (ARCHITECTURE.md §2.2) to pick
// a node, writes the placement decision to Postgres, then publishes the
// assignment onto the winning node's subject. Failures (bad payload, no
// healthy node, DB/NATS errors) are logged and the message is still acked
// (see subscribePlacement's caller) — like Task 4's handleAssignment,
// there is no backoff-aware redelivery configured here, so retrying via
// NATS redelivery would just busy-loop instead of helping.
func (s *Scheduler) handlePlacement(ctx context.Context, data []byte) {
	var msg placementRequestedMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		s.logger.Error("decoding placement.requested message", "error", err)
		return
	}

	deploymentID, err := uuid.Parse(msg.DeploymentID)
	if err != nil {
		s.logger.Error("placement.requested message has non-uuid deployment_id", "deployment_id", msg.DeploymentID, "error", err)
		return
	}

	node, err := s.selectNode(ctx)
	if err != nil {
		s.logger.Error("no node available for placement", "deployment_id", msg.DeploymentID, "error", err)
		return
	}

	// Layer 2 of the three-layer quota scheme (ARCHITECTURE.md §2.9,
	// phase-6-multi-tenant-saas.md Task 6): re-verify quota at placement
	// time, the same moment selectNode just confirmed capacity — closes the
	// TOCTOU gap between the API server's own Layer 1 check and this
	// commit (two concurrent scale/deploy requests can both pass Layer 1
	// before either's placement actually lands). Rejected the same way an
	// unschedulable-for-capacity assignment already is above: logged, no
	// container row written, no assignment published — the deployment
	// stays in whatever state it was already in, same as "no node
	// available."
	if err := s.checkQuota(ctx, deploymentID); err != nil {
		s.logger.Warn("placement rejected: organization quota exceeded", "deployment_id", msg.DeploymentID, "error", err)
		return
	}

	container, err := s.containers.Create(ctx, deploymentID, node.ID)
	if err != nil {
		s.logger.Error("recording placement decision", "deployment_id", msg.DeploymentID, "node_id", node.ID, "error", err)
		return
	}

	assignData, err := json.Marshal(assignMessage{
		AssignmentID: container.ID.String(),
		DeploymentID: msg.DeploymentID,
		Image:        msg.Image,
		Env:          msg.Env,
		Ports:        msg.Ports,
		Command:      msg.Command,
	})
	if err != nil {
		s.logger.Error("marshaling assignment message", "assignment_id", container.ID, "error", err)
		return
	}

	if err := s.bus.PublishDurable(ctx, eventbus.NodeAssignSubject(node.ID.String()), assignData); err != nil {
		s.logger.Error("publishing assignment", "deployment_id", msg.DeploymentID, "node_id", node.ID, "assignment_id", container.ID, "error", err)
		return
	}

	s.logger.Info("placed deployment", "deployment_id", msg.DeploymentID, "node_id", node.ID, "assignment_id", container.ID)
}

// handleStatus records a worker's node.<id>.status observation onto the
// containers row that assignment_id (== the container's own id, minted by
// handlePlacement above) identifies.
func (s *Scheduler) handleStatus(ctx context.Context, data []byte) {
	var msg statusMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		s.logger.Error("decoding status message", "error", err)
		return
	}

	assignmentID, err := uuid.Parse(msg.AssignmentID)
	if err != nil {
		s.logger.Error("status message has non-uuid assignment_id", "assignment_id", msg.AssignmentID, "error", err)
		return
	}

	status := db.ContainerStatus(msg.Status)
	switch status {
	case db.ContainerStatusPending, db.ContainerStatusRunning, db.ContainerStatusCrashed, db.ContainerStatusStopped:
	default:
		s.logger.Error("status message has unrecognized status", "assignment_id", msg.AssignmentID, "status", msg.Status)
		return
	}

	var containerRuntimeID *string
	if msg.ContainerID != "" {
		containerRuntimeID = &msg.ContainerID
	}

	if err := s.containers.UpdateStatus(ctx, assignmentID, status, containerRuntimeID); err != nil {
		if errors.Is(err, db.ErrNotFound) {
			s.logger.Warn("status message for unknown assignment, dropping", "assignment_id", msg.AssignmentID)
			return
		}
		s.logger.Error("recording container status", "assignment_id", msg.AssignmentID, "error", err)
	}
}

// checkQuota resolves deploymentID's owning org (over the scheduler's
// admin/bypass connection — s.deployments/s.quotas have no RLS session to
// key off, same reasoning db.ReconcileRepository's doc comment already
// gives) and rejects if placing one more container would push the org's
// container count over its ceiling, or if it's already over its CPU/memory
// ceiling (checked, though always inert today — see
// services/apiserver/internal/server/quota.go's identical caveat on
// cpu_millicores/memory_mb never being set past zero by any route yet).
func (s *Scheduler) checkQuota(ctx context.Context, deploymentID uuid.UUID) error {
	orgID, err := s.deployments.OrgID(ctx, deploymentID)
	if err != nil {
		return fmt.Errorf("resolving deployment org for quota check: %w", err)
	}
	quota, err := s.quotas.Get(ctx, orgID)
	if err != nil {
		return fmt.Errorf("fetching quota for org %s: %w", orgID, err)
	}
	usage, err := s.quotas.CurrentUsage(ctx, orgID)
	if err != nil {
		return fmt.Errorf("computing usage for org %s: %w", orgID, err)
	}

	switch {
	case usage.Containers+1 > quota.MaxContainers:
		return fmt.Errorf("would exceed max containers for org %s (%d/%d)", orgID, usage.Containers+1, quota.MaxContainers)
	case usage.CPUMillicores > quota.MaxCPUMillicores:
		return fmt.Errorf("org %s already exceeds max cpu millicores (%d/%d)", orgID, usage.CPUMillicores, quota.MaxCPUMillicores)
	case usage.MemoryMB > quota.MaxMemoryMB:
		return fmt.Errorf("org %s already exceeds max memory mb (%d/%d)", orgID, usage.MemoryMB, quota.MaxMemoryMB)
	}
	return nil
}

// selectNode implements ARCHITECTURE.md §2.2's filter-then-score MVP
// algorithm: filter out nodes that aren't healthy or have no configured
// capacity, then score the rest by lowest current load (fewest
// non-terminal containers), picking the best. Per-workload resource
// requests aren't part of placement.requested's published schema yet
// (docs/nats-contract.md), so headroom filtering is limited to a node's
// own configured capacity rather than true bin-packing.
func (s *Scheduler) selectNode(ctx context.Context) (db.Node, error) {
	nodes, err := s.nodes.List(ctx)
	if err != nil {
		return db.Node{}, fmt.Errorf("listing nodes: %w", err)
	}

	var best *db.Node
	var bestLoad int
	for i := range nodes {
		n := nodes[i]
		if n.Status != db.NodeStatusHealthy || n.CPUCapacityMillicores <= 0 || n.MemoryCapacityMB <= 0 {
			continue
		}

		load, err := s.loadOf(ctx, n.ID)
		if err != nil {
			return db.Node{}, fmt.Errorf("scoring node %s: %w", n.ID, err)
		}

		if best == nil || load < bestLoad {
			best, bestLoad = &n, load
		}
	}

	if best == nil {
		return db.Node{}, errors.New("no healthy node with capacity available")
	}
	return *best, nil
}

// loadOf counts nodeID's non-terminal (pending or running) containers, the
// "current load" selectNode scores nodes by.
func (s *Scheduler) loadOf(ctx context.Context, nodeID uuid.UUID) (int, error) {
	containers, err := s.containers.ListByNode(ctx, nodeID)
	if err != nil {
		return 0, err
	}

	load := 0
	for _, c := range containers {
		if c.Status == db.ContainerStatusPending || c.Status == db.ContainerStatusRunning {
			load++
		}
	}
	return load, nil
}

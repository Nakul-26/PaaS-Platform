// Package controller implements the Deployment controller
// (ARCHITECTURE.md §2.3, phase-3-controllers.md Task 4): the loop that
// keeps each application's actual running-container count converged on its
// desired replica count, closing the gap Phase 2 left open (a worker
// publishes node.<id>.status exactly once, at start, so nothing ever
// notices a container that dies afterward, and nothing ever acts on
// applications.replicas_desired).
package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"platform/internal/db"
	"platform/internal/eventbus"
)

// Config tunes the reconcile loop.
type Config struct {
	// ReconcileInterval is how often every active deployment's desired vs.
	// actual replica count is re-diffed (placeholder default 5s, revisited
	// under load in Phase 10's failure-injection pass — same R9 posture
	// Phase 2 applied to heartbeat/liveness-sweep tuning).
	ReconcileInterval time.Duration
}

// Controller owns the single Phase 3 controller-manager instance's
// reconcile loop. Single-instance only (R2/R3's posture, same constraint
// Phase 2 placed on the scheduler) — no leader election yet.
type Controller struct {
	bus        eventbus.EventBus
	targets    db.ReconcileRepository
	containers db.ContainerRepository
	cfg        Config
	logger     *slog.Logger
}

func New(bus eventbus.EventBus, targets db.ReconcileRepository, containers db.ContainerRepository, cfg Config, logger *slog.Logger) *Controller {
	if logger == nil {
		logger = slog.Default()
	}
	return &Controller{bus: bus, targets: targets, containers: containers, cfg: cfg, logger: logger}
}

// placementRequestedMessage/portBinding mirror docs/nats-contract.md's
// placement.requested schema — controller-manager's own copy, since per
// ADR-0012 it may depend only on the published NATS contract, never the
// scheduler's package tree (the same reasoning apiserver's and scheduler's
// own copies of these shapes already document).
type placementRequestedMessage struct {
	DeploymentID  string        `json:"deployment_id"`
	ApplicationID string        `json:"application_id"`
	Image         string        `json:"image"`
	Ports         []portBinding `json:"ports,omitempty"`
}

type portBinding struct {
	ContainerPort int    `json:"container_port"`
	HostPort      int    `json:"host_port,omitempty"`
	Protocol      string `json:"protocol,omitempty"`
}

// unassignMessage mirrors docs/nats-contract.md's node.<id>.unassign
// schema — the same shape nodeagent's own (unexported) copy uses.
type unassignMessage struct {
	AssignmentID string `json:"assignment_id"`
	ContainerID  string `json:"container_id"`
}

// Run ensures the streams this controller publishes onto exist, then ticks
// on cfg.ReconcileInterval until ctx is done. Stream setup failure is fatal
// (without it, neither placement.requested nor node.<id>.unassign publishes
// could ever succeed); a single tick's errors are logged and self-correct
// on the next tick (see reconcileOne's doc comment).
func (c *Controller) Run(ctx context.Context) error {
	if err := c.bus.EnsureStream(ctx, eventbus.StreamConfig{
		Name:     eventbus.PlacementStream,
		Subjects: []string{eventbus.PlacementStreamFilter},
	}); err != nil {
		return fmt.Errorf("ensuring %s stream: %w", eventbus.PlacementStream, err)
	}
	if err := c.bus.EnsureStream(ctx, eventbus.StreamConfig{
		Name:     eventbus.NodeAssignmentsStream,
		Subjects: []string{eventbus.NodeAssignmentsStreamFilter, eventbus.NodeUnassignStreamFilter},
	}); err != nil {
		return fmt.Errorf("ensuring %s stream: %w", eventbus.NodeAssignmentsStream, err)
	}

	ticker := time.NewTicker(c.cfg.ReconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			c.tick(ctx)
		}
	}
}

// tick recomputes every active deployment's actual replica count fresh
// from Postgres and acts on any drift (R2: idempotent, concurrency-safe
// reconciliation — an in-flight placement from a previous tick is
// naturally accounted for by the time it lands, since the next tick
// re-reads actual rather than tracking its own in-memory belief of it).
func (c *Controller) tick(ctx context.Context) {
	targets, err := c.targets.ListActive(ctx)
	if err != nil {
		c.logger.Error("listing reconcile targets", "error", err)
		return
	}
	for _, t := range targets {
		c.reconcileOne(ctx, t)
	}
}

// reconcileOne diffs one deployment's desired vs. actual replica count and
// acts on the difference. Errors here (a failed publish, a failed query)
// are logged, not retried within the tick: a transient failure just means
// this deployment's drift persists one more tick, which the next tick's
// fresh actual-count read corrects on its own — no special-cased retry
// logic needed.
func (c *Controller) reconcileOne(ctx context.Context, t db.ReconcileTarget) {
	active, err := c.containers.CountActiveByDeployment(ctx, t.DeploymentID)
	if err != nil {
		c.logger.Error("counting active containers", "deployment_id", t.DeploymentID, "error", err)
		return
	}

	switch {
	case active < t.ReplicasDesired:
		c.scaleUp(ctx, t, t.ReplicasDesired-active)
	case active > t.ReplicasDesired:
		c.scaleDown(ctx, t, active-t.ReplicasDesired)
	}
}

// scaleUp requests one placement.requested per missing replica — the
// scheduler's existing filter-then-score placement handles each exactly
// like the deploy-time request it can't otherwise tell it apart from
// (phase-3-controllers.md Task 4: "reuses the scheduler's existing
// placement unchanged, no new placement machinery").
func (c *Controller) scaleUp(ctx context.Context, t db.ReconcileTarget, shortfall int) {
	ports := make([]portBinding, len(t.Ports))
	for i, p := range t.Ports {
		ports[i] = portBinding{ContainerPort: p.ContainerPort, HostPort: p.HostPort, Protocol: p.Protocol}
	}
	data, err := json.Marshal(placementRequestedMessage{
		DeploymentID:  t.DeploymentID.String(),
		ApplicationID: t.ApplicationID.String(),
		Image:         t.Image,
		Ports:         ports,
	})
	if err != nil {
		c.logger.Error("marshaling placement.requested", "deployment_id", t.DeploymentID, "error", err)
		return
	}

	for i := 0; i < shortfall; i++ {
		if err := c.bus.PublishDurable(ctx, eventbus.PlacementRequestedSubject, data); err != nil {
			c.logger.Error("publishing placement.requested", "deployment_id", t.DeploymentID, "error", err)
			return
		}
	}
	c.logger.Info("requested placement for shortfall", "deployment_id", t.DeploymentID, "shortfall", shortfall)
}

// scaleDown unassigns excess active containers, oldest-first
// (phase-3-controllers.md Task 4, open decision #3: "oldest-created-first
// is the working default"). containers carries no created_at (Task 3
// deliberately added none — no schema migration was needed for replica
// support), so started_at is the closest available signal: containers
// already running longest are removed first, and containers that haven't
// started yet (started_at is nil — still pending) sort last, since there's
// nothing "old" about a placement that hasn't taken effect yet. All active
// containers for one deployment run the same image/revision (Phase 3 has
// no rolling-deployment concept yet), so which ones are picked doesn't
// affect correctness — only determinism, which this ordering gives it.
func (c *Controller) scaleDown(ctx context.Context, t db.ReconcileTarget, excess int) {
	all, err := c.containers.ListByDeployment(ctx, t.DeploymentID)
	if err != nil {
		c.logger.Error("listing containers for scale-down", "deployment_id", t.DeploymentID, "error", err)
		return
	}

	var active []db.Container
	for _, cn := range all {
		if cn.Status == db.ContainerStatusPending || cn.Status == db.ContainerStatusRunning {
			active = append(active, cn)
		}
	}
	sort.Slice(active, func(i, j int) bool {
		ai, aj := active[i].StartedAt, active[j].StartedAt
		if ai == nil {
			return false
		}
		if aj == nil {
			return true
		}
		return ai.Before(*aj)
	})

	if excess > len(active) {
		excess = len(active)
	}

	for _, cn := range active[:excess] {
		var runtimeID string
		if cn.ContainerRuntimeID != nil {
			runtimeID = *cn.ContainerRuntimeID
		}
		data, err := json.Marshal(unassignMessage{AssignmentID: cn.ID.String(), ContainerID: runtimeID})
		if err != nil {
			c.logger.Error("marshaling unassign message", "container_id", cn.ID, "error", err)
			continue
		}
		if err := c.bus.PublishDurable(ctx, eventbus.NodeUnassignSubject(cn.NodeID.String()), data); err != nil {
			c.logger.Error("publishing unassign", "container_id", cn.ID, "node_id", cn.NodeID, "error", err)
			continue
		}
		c.logger.Info("requested unassign for excess replica", "deployment_id", t.DeploymentID, "container_id", cn.ID, "node_id", cn.NodeID)
	}
}

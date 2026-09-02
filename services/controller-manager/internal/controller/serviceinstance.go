package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"platform/internal/db"
	"platform/internal/eventbus"
)

// ServiceInstanceConfig tunes the Service Instance controller's reconcile
// loop (phase-4-service-discovery-lb.md Task 3).
type ServiceInstanceConfig struct {
	// ReconcileInterval is how often running containers are re-diffed
	// against services/service_instances (placeholder default 5s, same R9
	// posture Controller's own ReconcileInterval applies).
	ReconcileInterval time.Duration
}

// ServiceInstanceController is the second per-resource-type reconcile loop
// this binary runs (ARCHITECTURE.md §2.3's "one loop per resource type"),
// alongside Controller's Deployment loop — same binary, same
// single-instance-only posture (R3), no leader election. It turns "which
// containers are actually running, and where" into services/service_instances
// rows: the durable registry the load balancer (Task 4) reads on its request
// hot path, never Postgres or the API server directly.
type ServiceInstanceController struct {
	bus       eventbus.EventBus
	reconcile db.ServiceReconcileRepository
	services  db.ServiceRepository
	instances db.ServiceInstanceRepository
	cfg       ServiceInstanceConfig
	logger    *slog.Logger

	// portsByContainer caches the last node.<id>.status-reported port for a
	// container, keyed by containers.id (the status message's
	// assignment_id — see statusMessage's doc comment below), since Task 2's
	// port is never persisted to Postgres, only carried on this
	// JetStream-backed (and therefore replayable-from-the-start-on-restart)
	// event. §2.6's "never trust pub/sub alone for critical state" is
	// honored by tick() remaining the sole source of truth for *whether* an
	// instance should exist at all — this cache only ever supplies the port
	// for a container tick has already, independently, confirmed running.
	mu               sync.Mutex
	portsByContainer map[string]int
}

func NewServiceInstanceController(bus eventbus.EventBus, reconcile db.ServiceReconcileRepository, services db.ServiceRepository, instances db.ServiceInstanceRepository, cfg ServiceInstanceConfig, logger *slog.Logger) *ServiceInstanceController {
	if logger == nil {
		logger = slog.Default()
	}
	return &ServiceInstanceController{
		bus:              bus,
		reconcile:        reconcile,
		services:         services,
		instances:        instances,
		cfg:              cfg,
		logger:           logger,
		portsByContainer: make(map[string]int),
	}
}

// statusMessage mirrors nodeagent's own (unexported) copy of
// node.<id>.status's schema (docs/nats-contract.md) — per ADR-0012, this
// controller depends only on the published NATS contract, never the
// worker's package tree. assignment_id is containers.id; container_id is
// the runtime's own id (unused here).
type statusMessage struct {
	AssignmentID string        `json:"assignment_id"`
	Status       string        `json:"status"`
	Ports        []portBinding `json:"ports,omitempty"`
}

// updatedMessage is service.updated's payload (docs/nats-contract.md).
type updatedMessage struct {
	ContainerID string `json:"container_id"`
}

// Run subscribes durably to node.<id>.status (to learn Task 2's reported
// ports) and ticks on cfg.ReconcileInterval until ctx is done. The durable
// consumer's default deliver-from-start-of-stream behavior means a restart
// replays NODE_STATUS and rebuilds portsByContainer, within the stream's own
// retention — not a substitute for tick()'s Postgres-driven reconciliation,
// just this cache's own recovery path.
func (s *ServiceInstanceController) Run(ctx context.Context) error {
	if err := s.bus.EnsureStream(ctx, eventbus.StreamConfig{
		Name:     eventbus.NodeStatusStream,
		Subjects: []string{eventbus.NodeStatusStreamFilter},
	}); err != nil {
		return fmt.Errorf("ensuring %s stream: %w", eventbus.NodeStatusStream, err)
	}

	sub, err := s.bus.SubscribeDurable(ctx, eventbus.NodeStatusStream, "controller-manager-service-instance-ports", eventbus.NodeStatusStreamFilter, func(msg eventbus.Message) error {
		s.handleStatus(msg.Data)
		return nil
	})
	if err != nil {
		return fmt.Errorf("subscribing to %s: %w", eventbus.NodeStatusStreamFilter, err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	ticker := time.NewTicker(s.cfg.ReconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.tick(ctx)
		}
	}
}

func (s *ServiceInstanceController) handleStatus(data []byte) {
	var msg statusMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		s.logger.Error("decoding status message", "error", err)
		return
	}
	if msg.AssignmentID == "" || msg.Status != "running" || len(msg.Ports) == 0 {
		return
	}

	s.mu.Lock()
	s.portsByContainer[msg.AssignmentID] = msg.Ports[0].HostPort
	s.mu.Unlock()
}

func (s *ServiceInstanceController) portFor(containerID uuid.UUID) (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	port, ok := s.portsByContainer[containerID.String()]
	return port, ok
}

// tick recomputes which containers should back a service_instances row
// fresh from Postgres every time (same R2 idempotent-reconciliation posture
// Controller's own tick uses): candidates currently running are
// created/refreshed, and instances whose container fell out of 'running'
// are removed outright (Task 1's DeleteByContainer — a crashed/stopped
// container is gone for good, never revived under the same id).
func (s *ServiceInstanceController) tick(ctx context.Context) {
	candidates, err := s.reconcile.ListRunningCandidates(ctx)
	if err != nil {
		s.logger.Error("listing service instance candidates", "error", err)
	} else {
		for _, c := range candidates {
			s.reconcileCandidate(ctx, c)
		}
	}

	orphaned, err := s.reconcile.ListOrphanedInstances(ctx)
	if err != nil {
		s.logger.Error("listing orphaned service instances", "error", err)
		return
	}
	for _, containerID := range orphaned {
		if err := s.instances.DeleteByContainer(ctx, containerID); err != nil {
			if !errors.Is(err, db.ErrNotFound) {
				s.logger.Error("deleting orphaned service instance", "container_id", containerID, "error", err)
			}
			continue
		}
		s.logger.Info("removed service instance for no-longer-running container", "container_id", containerID)
		s.publishUpdated(ctx, containerID)

		s.mu.Lock()
		delete(s.portsByContainer, containerID.String())
		s.mu.Unlock()
	}
}

func (s *ServiceInstanceController) reconcileCandidate(ctx context.Context, c db.ServiceInstanceCandidate) {
	port, ok := s.portFor(c.ContainerID)
	if !ok {
		// Task 2's port hasn't been observed for this container yet (e.g.
		// this controller only just started and hasn't replayed that
		// container's last status message) — try again next tick rather
		// than guessing.
		return
	}

	svc, err := s.services.GetOrCreate(ctx, c.ApplicationID, dnsName(c.ProjectSlug, c.ApplicationName, c.ApplicationID))
	if err != nil {
		s.logger.Error("getting or creating service", "application_id", c.ApplicationID, "error", err)
		return
	}

	if _, err := s.instances.Upsert(ctx, svc.ID, c.ContainerID, c.NodeIP, port); err != nil {
		s.logger.Error("upserting service instance", "container_id", c.ContainerID, "error", err)
		return
	}
	s.publishUpdated(ctx, c.ContainerID)
}

func (s *ServiceInstanceController) publishUpdated(ctx context.Context, containerID uuid.UUID) {
	data, err := json.Marshal(updatedMessage{ContainerID: containerID.String()})
	if err != nil {
		s.logger.Error("marshaling service.updated", "container_id", containerID, "error", err)
		return
	}
	if err := s.bus.Publish(ctx, eventbus.ServiceUpdatedSubject, data); err != nil {
		s.logger.Error("publishing service.updated", "container_id", containerID, "error", err)
	}
}

var slugInvalidChars = regexp.MustCompile(`[^a-z0-9]+`)

// dnsName builds services.dns_name (phase-4-service-discovery-lb.md's open
// decision 1): "<project-slug>-<app-slug>-<app-id-suffix>.internal".
// applications.name carries no uniqueness constraint (unlike projects.slug,
// database-schema.md §3), so the application's own id is appended to
// guarantee dns_name's DB-level uniqueness even if two applications in the
// same project share a name — GetOrCreate only ever consults this value on
// an application's first call anyway (idempotent afterward), so it only has
// to be collision-free once.
func dnsName(projectSlug, applicationName string, applicationID uuid.UUID) string {
	appSlug := slugify(applicationName)
	idSuffix := strings.ReplaceAll(applicationID.String(), "-", "")[:8]
	return fmt.Sprintf("%s-%s-%s.internal", projectSlug, appSlug, idSuffix)
}

func slugify(s string) string {
	s = strings.ToLower(s)
	s = slugInvalidChars.ReplaceAllString(s, "-")
	return strings.Trim(s, "-")
}

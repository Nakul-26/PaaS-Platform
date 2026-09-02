// Package healthcheck is the load balancer's active health checking layer
// (ARCHITECTURE.md §2.5, phase-4-service-discovery-lb.md Task 5): the "is
// the app actually responding" check, independent of and faster than
// container-process-alive (Phase 3's worker-level concern) or Task 3's
// Postgres-driven reconcile tick. This is the piece that will eventually
// read applications.health_check_path per application (flagged, unused,
// since phase-3-controllers.md) — a single fixed path is the Phase 4
// placeholder (open decision 3, R9 posture).
package healthcheck

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"platform/services/loadbalancer/internal/registry"
)

// Checker periodically probes every instance the registry currently knows
// about (Registry.All — deliberately including already-unhealthy ones, so
// a recovered instance gets un-ejected) and marks each healthy or
// unhealthy based on the probe's outcome.
type Checker struct {
	registry *registry.Registry
	client   *http.Client
	path     string
	interval time.Duration
	logger   *slog.Logger
}

func New(reg *registry.Registry, path string, interval, timeout time.Duration, logger *slog.Logger) *Checker {
	if logger == nil {
		logger = slog.Default()
	}
	return &Checker{
		registry: reg,
		client:   &http.Client{Timeout: timeout},
		path:     path,
		interval: interval,
		logger:   logger,
	}
}

// Run probes every instance on every tick until ctx is done. An instance
// that fails a probe is marked unhealthy for one interval — long enough
// that Next skips it until this same loop's next tick gets a chance to
// re-probe and, if it now succeeds, clear the mark via MarkHealthy.
func (c *Checker) Run(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.probeAll(ctx)
		}
	}
}

func (c *Checker) probeAll(ctx context.Context) {
	for _, inst := range c.registry.All() {
		go c.probeOne(ctx, inst)
	}
}

func (c *Checker) probeOne(ctx context.Context, inst registry.Instance) {
	url := fmt.Sprintf("http://%s:%d%s", inst.IP, inst.Port, c.path)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		c.logger.Error("loadbalancer: building health check request", "container_id", inst.ContainerID, "url", url, "error", err)
		return
	}

	resp, err := c.client.Do(req)
	if err != nil {
		c.logger.Warn("loadbalancer: health check failed, ejecting", "container_id", inst.ContainerID, "url", url, "error", err)
		c.registry.MarkUnhealthy(inst.ContainerID, c.interval)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	// Anything below 500 counts as "the app answered" — a 404 on the probe
	// path still proves the process is alive and serving HTTP, which is all
	// this layer claims to check; applications.health_check_path (once
	// wired up) is what lets a tenant assert something stricter.
	if resp.StatusCode >= http.StatusInternalServerError {
		c.logger.Warn("loadbalancer: health check returned server error, ejecting", "container_id", inst.ContainerID, "url", url, "status", resp.StatusCode)
		c.registry.MarkUnhealthy(inst.ContainerID, c.interval)
		return
	}

	c.registry.MarkHealthy(inst.ContainerID)
}

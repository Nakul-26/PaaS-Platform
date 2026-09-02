// Package registry is the load balancer's in-memory service registry
// (ARCHITECTURE.md §2.6): the only thing the request hot path ever reads —
// never Postgres or the API server directly (§2.5). main.go is the only
// writer, keeping it fresh via three independent paths: Upsert/Remove
// driven by NATS service.updated events (Task 3's publisher), ReplaceAll
// driven by a periodic full resync from Postgres (the fallback for a missed
// event), and MarkUnhealthy/MarkHealthy driven by Task 5's passive
// (proxy-error-triggered) and active (periodic probe) health checking —
// three layers, same §2.6 "never trust pub/sub alone, always pair with
// reconciliation" principle applied one level further: pub/sub +
// Postgres-reconciliation tells the registry what *should* exist, health
// checking tells it what's actually *answering* right now, and Sweep's TTL
// is the backstop for both paths going stale at once.
package registry

import (
	"sync"
	"time"

	"github.com/google/uuid"

	"platform/services/loadbalancer/internal/strategy"
)

// Instance is one reachable backend: enough to dial it (ip:port) and enough
// to identify it for Upsert/Remove/MarkUnhealthy (container_id, matching
// service_instances.container_id).
type Instance struct {
	ContainerID uuid.UUID
	IP          string
	Port        int
}

// Registry is safe for concurrent use: every request-handling goroutine
// calls Next while main.go's NATS handler, resync ticker, TTL sweep
// ticker, and health checker all call the write methods concurrently.
type Registry struct {
	mu       sync.RWMutex
	byDNS    map[string][]Instance
	strategy strategy.BalancingStrategy

	// lastSeen is when an instance was last confirmed present, by either
	// Upsert or a ReplaceAll that still included it — Sweep's TTL clock.
	lastSeen map[uuid.UUID]time.Time
	// unhealthyUntil holds a future timestamp for an instance Next should
	// currently skip: set by passive ejection (a proxied request to it
	// failed) or an active health check probe failure, cleared by
	// MarkHealthy (a subsequent successful probe). Absence means healthy.
	unhealthyUntil map[uuid.UUID]time.Time
}

func New(strat strategy.BalancingStrategy) *Registry {
	return &Registry{
		byDNS:          make(map[string][]Instance),
		strategy:       strat,
		lastSeen:       make(map[uuid.UUID]time.Time),
		unhealthyUntil: make(map[uuid.UUID]time.Time),
	}
}

// ReplaceAll swaps the entire registry contents atomically — the periodic
// full-resync path. byDNS should be built fresh from
// db.LoadBalancerRepository.ListHealthyRegistry each time, never mutated
// incrementally, so a service/instance removed in Postgres since the last
// resync is also absent here. Every instance byDNS still mentions counts as
// freshly seen; bookkeeping for any instance it no longer mentions at all
// is dropped so it doesn't linger past its own removal.
func (r *Registry) ReplaceAll(byDNS map[string][]Instance) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.byDNS = byDNS

	now := time.Now()
	lastSeen := make(map[uuid.UUID]time.Time, len(r.lastSeen))
	for _, list := range byDNS {
		for _, inst := range list {
			lastSeen[inst.ContainerID] = now
		}
	}
	r.lastSeen = lastSeen

	for id := range r.unhealthyUntil {
		if _, ok := lastSeen[id]; !ok {
			delete(r.unhealthyUntil, id)
		}
	}
}

// Upsert adds inst under dnsName, or refreshes its ip/port in place if an
// instance with the same ContainerID is already registered under dnsName —
// the incremental path for a service.updated event reporting a
// create/refresh. Also refreshes lastSeen, same as ReplaceAll would.
func (r *Registry) Upsert(dnsName string, inst Instance) {
	r.mu.Lock()
	defer r.mu.Unlock()

	list := r.byDNS[dnsName]
	for i, existing := range list {
		if existing.ContainerID == inst.ContainerID {
			list[i] = inst
			r.lastSeen[inst.ContainerID] = time.Now()
			return
		}
	}
	r.byDNS[dnsName] = append(list, inst)
	r.lastSeen[inst.ContainerID] = time.Now()
}

// Remove evicts containerID from whichever service it's currently
// registered under, if any, and drops its bookkeeping — the incremental
// path for a service.updated event reporting a removal (Task 3's
// controller no longer sees the container as a healthy instance of
// anything).
func (r *Registry) Remove(containerID uuid.UUID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removeLocked(containerID)
}

func (r *Registry) removeLocked(containerID uuid.UUID) {
	for dnsName, list := range r.byDNS {
		for i, inst := range list {
			if inst.ContainerID == containerID {
				r.byDNS[dnsName] = append(list[:i], list[i+1:]...)
				delete(r.lastSeen, containerID)
				delete(r.unhealthyUntil, containerID)
				return
			}
		}
	}
}

// MarkUnhealthy makes Next skip containerID until duration from now —
// Task 5's shared mechanism for both passive ejection (the proxy calls this
// after a failed proxied request, with a short ejection window) and active
// health-check failures (the checker calls this after a failed probe, with
// its own probe interval as the window, so the *next* probe gets a chance
// to clear it via MarkHealthy).
func (r *Registry) MarkUnhealthy(containerID uuid.UUID, duration time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unhealthyUntil[containerID] = time.Now().Add(duration)
}

// MarkHealthy clears any unhealthy mark on containerID — called by the
// active health checker after a probe succeeds.
func (r *Registry) MarkHealthy(containerID uuid.UUID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.unhealthyUntil, containerID)
}

// Next picks the next instance registered under dnsName via the configured
// BalancingStrategy, skipping anything currently marked unhealthy.
// ok=false means no healthy instance is currently registered for dnsName —
// deliberately not falling back to an unhealthy one, since Task 5's whole
// point is that a proxied request should never be routed to a backend
// already known to be failing.
func (r *Registry) Next(dnsName string) (Instance, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	list := r.byDNS[dnsName]
	if len(list) == 0 {
		return Instance{}, false
	}

	now := time.Now()
	available := make([]Instance, 0, len(list))
	for _, inst := range list {
		if until, ok := r.unhealthyUntil[inst.ContainerID]; ok && until.After(now) {
			continue
		}
		available = append(available, inst)
	}
	if len(available) == 0 {
		return Instance{}, false
	}
	return available[r.strategy.Next(dnsName, len(available))], true
}

// All returns every instance currently registered across every service,
// regardless of current health state — the active health checker's own
// probe set. Probing ignores current health deliberately, so a previously
// unhealthy instance that has actually recovered gets noticed and
// un-ejected by the very next probe cycle.
func (r *Registry) All() []Instance {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var all []Instance
	for _, list := range r.byDNS {
		all = append(all, list...)
	}
	return all
}

// Sweep removes any instance not refreshed (via Upsert, or a ReplaceAll
// that still included it) within ttl — an independent safety net for the
// resync/event paths silently failing to keep the registry current at all
// (§2.6), not a substitute for either: under normal operation nothing is
// ever old enough to be swept, since a successful resync/event refreshes
// lastSeen well inside ttl.
func (r *Registry) Sweep(ttl time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	cutoff := time.Now().Add(-ttl)
	var stale []uuid.UUID
	for id, seen := range r.lastSeen {
		if seen.Before(cutoff) {
			stale = append(stale, id)
		}
	}
	for _, id := range stale {
		r.removeLocked(id)
	}
}

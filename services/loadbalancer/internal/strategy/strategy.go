// Package strategy holds the load balancer's pluggable backend-selection
// interface (ARCHITECTURE.md §2.5: "round robin first, pluggable strategy
// interface for weighted/least-connections later"; the package path itself
// is fixed by docs/modularity-and-extensibility.md's interface registry).
// Round robin is the only implementation Phase 4 needs — weighted and
// least-connections strategies are explicitly deferred
// (phase-4-service-discovery-lb.md's "Explicitly deferred out of Phase 4").
package strategy

import "sync"

// BalancingStrategy picks which of a service's n healthy instances (n > 0,
// guaranteed by the caller — registry.Registry.Next never calls this with
// an empty set) should serve the next request. key identifies the service
// (its dns_name) so an implementation can keep independent per-service
// state, as round robin's rotating counter does.
type BalancingStrategy interface {
	Next(key string, n int) int
}

// RoundRobin is the default BalancingStrategy: each call for a given key
// advances that key's counter by one, wrapping at n, so repeated calls
// cycle evenly through all n instances regardless of which specific
// instances they are — the registry is free to add/remove instances
// between calls without RoundRobin needing to know.
type RoundRobin struct {
	mu       sync.Mutex
	counters map[string]int
}

func NewRoundRobin() *RoundRobin {
	return &RoundRobin{counters: make(map[string]int)}
}

func (r *RoundRobin) Next(key string, n int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	i := r.counters[key] % n
	r.counters[key] = (i + 1) % n
	return i
}

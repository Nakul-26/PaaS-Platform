// Package proxy is the load balancer's request hot path (ARCHITECTURE.md
// §2.5): match a request to a service, pick a healthy instance from the
// in-memory registry (never Postgres, never the API server), and reverse
// proxy to it.
package proxy

import (
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"time"

	"platform/services/loadbalancer/internal/registry"
)

// ServiceHeader is the routing key this phase's requests are matched on
// (phase-4-service-discovery-lb.md's open decision 2). Real hostname/path
// routing is Phase 5's job (custom domains, TLS termination); until then,
// callers name the target service directly by its services.dns_name.
const ServiceHeader = "X-Platform-Service"

// Handler is the load balancer's http.Handler.
type Handler struct {
	registry       *registry.Registry
	ejectionWindow time.Duration
	logger         *slog.Logger
}

// New builds a Handler. ejectionWindow is Task 5's passive-removal window
// (ARCHITECTURE.md §2.5): how long a backend that just failed a proxied
// request is skipped by the registry before being reconsidered — a single
// bad request doesn't have to wait for the next active health-check probe,
// resync, or service.updated event.
func New(reg *registry.Registry, ejectionWindow time.Duration, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{registry: reg, ejectionWindow: ejectionWindow, logger: logger}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	dnsName := r.Header.Get(ServiceHeader)
	if dnsName == "" {
		http.Error(w, ServiceHeader+" header is required", http.StatusBadRequest)
		return
	}

	inst, ok := h.registry.Next(dnsName)
	if !ok {
		http.Error(w, "no healthy instance registered for "+dnsName, http.StatusServiceUnavailable)
		return
	}

	target := &url.URL{Scheme: "http", Host: inst.IP + ":" + strconv.Itoa(inst.Port)}
	rp := httputil.NewSingleHostReverseProxy(target)
	rp.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		h.logger.Error("loadbalancer: proxy request failed, ejecting backend", "service", dnsName, "target", target.Host, "error", err)
		h.registry.MarkUnhealthy(inst.ContainerID, h.ejectionWindow)
		http.Error(w, "bad gateway", http.StatusBadGateway)
	}
	rp.ServeHTTP(w, r)
}

// Package proxy is the load balancer's request hot path (ARCHITECTURE.md
// §2.5): match a request to a service, pick a healthy instance from the
// in-memory registry (never Postgres, never the API server), and reverse
// proxy to it.
package proxy

import (
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"time"

	"platform/services/loadbalancer/internal/registry"
)

// ServiceHeader is Phase 4's routing key (open decision 2), still
// consulted as a fallback now that real hostname routing exists (Task 4,
// phase-5-networking-ingress.md): a request whose Host isn't a registered
// domain gets one more chance via this header before ServeHTTP gives up
// with 404 — keeping Phase 4's own integration tests, and this internal
// testing path, working unchanged.
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
	dnsName, ok := h.resolveDNSName(r)
	if !ok {
		http.Error(w, "no route registered for this host, and no "+ServiceHeader+" header", http.StatusNotFound)
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

// resolveDNSName matches primarily on r.Host (port suffix stripped, per
// Task 4): a registered domain wins if there is one. Otherwise it falls
// back to the ServiceHeader, exactly Phase 4's only routing mechanism.
// ok=false means neither matched.
func (h *Handler) resolveDNSName(r *http.Request) (string, bool) {
	host := r.Host
	if stripped, _, err := net.SplitHostPort(host); err == nil {
		host = stripped
	}
	if dnsName, ok := h.registry.ResolveHost(host); ok {
		return dnsName, true
	}
	if header := r.Header.Get(ServiceHeader); header != "" {
		return header, true
	}
	return "", false
}

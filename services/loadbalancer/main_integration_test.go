//go:build integration

// Package main's integration test covers Task 4's acceptance
// (phase-4-service-discovery-lb.md): against a real Postgres, a real NATS
// instance, a real scheduler, a real worker, a real controller-manager, and
// the real loadbalancer binary, deploy an application at 3 replicas, wait
// for all 3 to become healthy service instances, then send repeated HTTP
// requests through the load balancer's actual listening port and confirm
// they land on all 3 distinct backends — the phase's literal exit-criteria
// scenario ("scale an app to 3 replicas, send repeated requests through the
// LB, observe distribution across all 3").
package main

import (
	"bytes"
	"context"
	cryptotls "crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go/modules/nats"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"platform/internal/db"
	"platform/internal/eventbus"
	"platform/internal/runtime"
	"platform/services/loadbalancer/internal/proxy"
)

func TestLoadBalancer_DistributesAcrossHealthyInstances(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	rt, err := runtime.NewDockerRuntime()
	if err != nil {
		t.Skipf("docker daemon not available, skipping: %v", err)
	}
	defer func() { _ = rt.Close() }()

	adminDSN, appDSN, adminDB, pool := startTestPostgres(t, ctx)
	defer func() { _ = adminDB.Close() }()

	applicationID, deploymentID := seedApplicationWithReplicas(t, ctx, adminDB, 3)

	natsContainer, err := nats.Run(ctx, "nats:2.11.7")
	if err != nil {
		t.Fatalf("starting nats container: %v", err)
	}
	t.Cleanup(func() { _ = natsContainer.Terminate(context.Background()) })

	natsURL, err := natsContainer.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("nats connection string: %v", err)
	}

	bus, err := eventbus.Connect(natsURL)
	if err != nil {
		t.Fatalf("connecting eventbus: %v", err)
	}
	t.Cleanup(func() { _ = bus.Close() })

	// The scheduler ensures PLACEMENT/NODE_ASSIGNMENTS on startup; this
	// test's own EnsureStream is an idempotent no-op layered on top, same
	// precedent as controller-manager's own integration tests.
	if err := bus.EnsureStream(ctx, eventbus.StreamConfig{
		Name:     eventbus.PlacementStream,
		Subjects: []string{eventbus.PlacementStreamFilter},
	}); err != nil {
		t.Fatalf("ensuring %s stream: %v", eventbus.PlacementStream, err)
	}

	startTestScheduler(t, ctx, appDSN, natsURL)
	startTestWorker(t, ctx, natsURL)
	startTestControllerManager(t, ctx, adminDSN, natsURL, "1s", "1s")
	lbAddr, _ := startTestLoadBalancer(t, ctx, appDSN, adminDSN, natsURL, "5s")

	containers := db.NewContainerRepository(pool.Conn())
	services := db.NewServiceRepository(pool.Conn())
	instances := db.NewServiceInstanceRepository(pool.Conn())

	svc := waitForHealthyServiceInstances(t, ctx, services, instances, applicationID, 3, 90*time.Second)

	running, err := containers.ListByDeployment(ctx, deploymentID)
	if err != nil {
		t.Fatalf("ListByDeployment: %v", err)
	}
	for _, c := range running {
		if c.Status != db.ContainerStatusRunning || c.ContainerRuntimeID == nil {
			continue
		}
		runtimeID := *c.ContainerRuntimeID
		t.Cleanup(func() {
			_ = rt.StopContainer(context.Background(), runtimeID, 5*time.Second)
			_ = rt.RemoveContainer(context.Background(), runtimeID)
		})
	}

	// Send repeated requests through the load balancer's real HTTP port
	// (never touching Postgres or the containers directly) and collect
	// each response's distinguishing "Hostname:" line (traefik/whoami's
	// own container hostname, which Docker defaults to the container id) —
	// this is the only way to observe, from outside, which of the 3
	// identical backends actually served a given request.
	client := &http.Client{Timeout: 5 * time.Second}
	seen := make(map[string]bool)
	deadline := time.Now().Add(60 * time.Second)
	var lastErr error
	for len(seen) < 3 && time.Now().Before(deadline) {
		hostname, err := requestBackendHostname(client, lbAddr, svc.DNSName)
		if err != nil {
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		}
		seen[hostname] = true
	}
	if len(seen) != 3 {
		t.Fatalf("expected requests through the load balancer to hit 3 distinct backends, saw %d: %v (last error: %v)", len(seen), seen, lastErr)
	}
}

// requestBackendHostname sends one request through the load balancer at
// lbAddr, routed to dnsName via proxy.ServiceHeader, and extracts
// traefik/whoami's "Hostname: <id>" line from the response body.
func requestBackendHostname(client *http.Client, lbAddr, dnsName string) (string, error) {
	req, err := http.NewRequest(http.MethodGet, "http://"+lbAddr+"/", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set(proxy.ServiceHeader, dnsName)
	return doRequestAndParseHostname(client, req)
}

// requestHostnameDirect queries addr (ip:port) directly, bypassing the load
// balancer entirely — Task 5's test uses this to learn which backend a
// given service_instances row actually is before deliberately stopping it.
func requestHostnameDirect(client *http.Client, addr string) (string, error) {
	req, err := http.NewRequest(http.MethodGet, "http://"+addr+"/", nil)
	if err != nil {
		return "", err
	}
	return doRequestAndParseHostname(client, req)
}

func doRequestAndParseHostname(client *http.Client, req *http.Request) (string, error) {
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status %d: %s", resp.StatusCode, body)
	}
	for _, line := range strings.Split(string(body), "\n") {
		if hostname, ok := strings.CutPrefix(line, "Hostname: "); ok {
			return strings.TrimSpace(hostname), nil
		}
	}
	return "", fmt.Errorf("no Hostname line in response body: %s", body)
}

// TestLoadBalancer_EjectsDeadBackendViaActiveHealthCheck covers Task 5's
// acceptance (phase-4-service-discovery-lb.md): against a real Postgres, a
// real NATS instance, a real scheduler, a real worker, a real
// controller-manager (its own reconcile tick deliberately slow, so it
// cannot be what removes the dead backend within this test's window), and
// the real loadbalancer binary (with a fast active health-check interval),
// deploy 3 replicas, stop one replica's container process directly (not
// via scale-down — that's Task 3's already-tested removal path), and
// confirm the load balancer stops routing to it within about one
// health-check interval while continuing to route normally to the other
// two.
func TestLoadBalancer_EjectsDeadBackendViaActiveHealthCheck(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	rt, err := runtime.NewDockerRuntime()
	if err != nil {
		t.Skipf("docker daemon not available, skipping: %v", err)
	}
	defer func() { _ = rt.Close() }()

	adminDSN, appDSN, adminDB, pool := startTestPostgres(t, ctx)
	defer func() { _ = adminDB.Close() }()

	applicationID, deploymentID := seedApplicationWithReplicas(t, ctx, adminDB, 3)

	natsContainer, err := nats.Run(ctx, "nats:2.11.7")
	if err != nil {
		t.Fatalf("starting nats container: %v", err)
	}
	t.Cleanup(func() { _ = natsContainer.Terminate(context.Background()) })

	natsURL, err := natsContainer.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("nats connection string: %v", err)
	}

	bus, err := eventbus.Connect(natsURL)
	if err != nil {
		t.Fatalf("connecting eventbus: %v", err)
	}
	t.Cleanup(func() { _ = bus.Close() })

	if err := bus.EnsureStream(ctx, eventbus.StreamConfig{
		Name:     eventbus.PlacementStream,
		Subjects: []string{eventbus.PlacementStreamFilter},
	}); err != nil {
		t.Fatalf("ensuring %s stream: %v", eventbus.PlacementStream, err)
	}

	startTestScheduler(t, ctx, appDSN, natsURL)
	startTestWorker(t, ctx, natsURL)
	// The Deployment controller's reconcile interval stays fast (1s, same
	// as every other test in this file) — 3 replicas need to actually come
	// up promptly, and any replacement it schedules mid-test is harmless,
	// unrelated behavior cleaned up below. Only the *service* reconcile
	// interval is deliberately slow: this test must prove the load
	// balancer's own active health check removed the dead backend, not
	// Task 3's service.updated removal path beating it to the same outcome.
	startTestControllerManager(t, ctx, adminDSN, natsURL, "1s", "60s")
	const healthCheckInterval = 1 * time.Second
	lbAddr, _ := startTestLoadBalancer(t, ctx, appDSN, adminDSN, natsURL, healthCheckInterval.String())

	containers := db.NewContainerRepository(pool.Conn())
	services := db.NewServiceRepository(pool.Conn())
	instances := db.NewServiceInstanceRepository(pool.Conn())

	svc := waitForHealthyServiceInstances(t, ctx, services, instances, applicationID, 3, 90*time.Second)

	running, err := containers.ListByDeployment(ctx, deploymentID)
	if err != nil {
		t.Fatalf("ListByDeployment: %v", err)
	}
	for _, c := range running {
		if c.Status != db.ContainerStatusRunning || c.ContainerRuntimeID == nil {
			continue
		}
		runtimeID := *c.ContainerRuntimeID
		t.Cleanup(func() {
			_ = rt.StopContainer(context.Background(), runtimeID, 5*time.Second)
			_ = rt.RemoveContainer(context.Background(), runtimeID)
		})
	}

	healthy, err := instances.ListHealthyByService(ctx, svc.ID)
	if err != nil {
		t.Fatalf("ListHealthyByService: %v", err)
	}
	if len(healthy) != 3 {
		t.Fatalf("expected 3 healthy instances, got %d: %+v", len(healthy), healthy)
	}

	// Pick the first running container as the one to stop, and learn its
	// whoami hostname by querying it directly — bypassing the load
	// balancer entirely — before touching it.
	var target db.Container
	for _, c := range running {
		if c.Status == db.ContainerStatusRunning && c.ContainerRuntimeID != nil {
			target = c
			break
		}
	}
	if target.ContainerRuntimeID == nil {
		t.Fatalf("no running container with a runtime id found among %+v", running)
	}
	var targetInstance db.ServiceInstance
	found := false
	for _, si := range healthy {
		if si.ContainerID == target.ID {
			targetInstance = si
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("no healthy service instance found for container %s", target.ID)
	}

	client := &http.Client{Timeout: 5 * time.Second}
	stoppedHostname, err := requestHostnameDirect(client, fmt.Sprintf("%s:%d", targetInstance.IP, targetInstance.Port))
	if err != nil {
		t.Fatalf("querying target instance directly before stopping it: %v", err)
	}

	// Stop the container's process directly against the real docker daemon
	// — not via scale-down — so only the load balancer's own active health
	// check (Task 5) can notice.
	if err := rt.StopContainer(ctx, *target.ContainerRuntimeID, 5*time.Second); err != nil {
		t.Fatalf("stopping container %s out-of-band: %v", *target.ContainerRuntimeID, err)
	}

	// Send requests through the load balancer for a window comfortably
	// longer than a couple of health-check intervals, recording which
	// hostname served each one and how long into the window. cutover marks
	// the point by which the stopped backend must have vanished from
	// rotation for good, while the other two keep serving normally.
	windowDuration := 8 * healthCheckInterval
	cutover := 3 * healthCheckInterval

	start := time.Now()
	lastSeen := make(map[string]time.Duration)
	for time.Since(start) < windowDuration {
		hostname, err := requestBackendHostname(client, lbAddr, svc.DNSName)
		if err == nil {
			lastSeen[hostname] = time.Since(start)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// The Deployment controller's own reconcile tick (kept fast, unlike
	// serviceReconcileInterval above — this test only needs Task 3's
	// service-instance removal path held back, not Task 3-from-Phase-3's
	// ordinary replica-count reconciliation) may well have already
	// scheduled a replacement for the stopped replica by now. That's
	// expected, unrelated behavior this test doesn't assert on either way —
	// but still needs cleaning up, registered before any assertion below
	// might fail this test early.
	if finalRunning, err := containers.ListByDeployment(ctx, deploymentID); err == nil {
		for _, c := range finalRunning {
			if c.Status != db.ContainerStatusRunning || c.ContainerRuntimeID == nil {
				continue
			}
			runtimeID := *c.ContainerRuntimeID
			t.Cleanup(func() {
				_ = rt.StopContainer(context.Background(), runtimeID, 5*time.Second)
				_ = rt.RemoveContainer(context.Background(), runtimeID)
			})
		}
	}

	if elapsed, ok := lastSeen[stoppedHostname]; ok && elapsed >= cutover {
		t.Fatalf("expected the load balancer to stop routing to the stopped backend %q well before %s elapsed, but it was still served at %s (all: %+v)", stoppedHostname, cutover, elapsed, lastSeen)
	}

	survivors := 0
	for hostname, elapsed := range lastSeen {
		if hostname != stoppedHostname && elapsed >= cutover {
			survivors++
		}
	}
	if survivors == 0 {
		t.Fatalf("expected the load balancer to keep routing to the surviving backends throughout the window, last-seen times: %+v", lastSeen)
	}
}

// TestLoadBalancer_RoutesByHostHeader is Task 4's acceptance
// (phase-5-networking-ingress.md): two applications, one registered domain
// each, one load balancer instance — confirm a request's Host header
// routes it to its own application's backend (not the other's), and that
// an unregistered Host gets 404 rather than falling through to anything.
// Domain rows are seeded directly via admin SQL (no apiserver involved in
// this test, same precedent as seedApplicationWithReplicas below) — this
// test's job is the load balancer's resync + routing, not the API route
// Task 2 already covers.
func TestLoadBalancer_RoutesByHostHeader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	rt, err := runtime.NewDockerRuntime()
	if err != nil {
		t.Skipf("docker daemon not available, skipping: %v", err)
	}
	defer func() { _ = rt.Close() }()

	adminDSN, appDSN, adminDB, pool := startTestPostgres(t, ctx)
	defer func() { _ = adminDB.Close() }()

	appAID, appBID, depAID, depBID := seedTwoApplications(t, ctx, adminDB)

	natsContainer, err := nats.Run(ctx, "nats:2.11.7")
	if err != nil {
		t.Fatalf("starting nats container: %v", err)
	}
	t.Cleanup(func() { _ = natsContainer.Terminate(context.Background()) })

	natsURL, err := natsContainer.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("nats connection string: %v", err)
	}

	bus, err := eventbus.Connect(natsURL)
	if err != nil {
		t.Fatalf("connecting eventbus: %v", err)
	}
	t.Cleanup(func() { _ = bus.Close() })
	if err := bus.EnsureStream(ctx, eventbus.StreamConfig{
		Name:     eventbus.PlacementStream,
		Subjects: []string{eventbus.PlacementStreamFilter},
	}); err != nil {
		t.Fatalf("ensuring %s stream: %v", eventbus.PlacementStream, err)
	}

	startTestScheduler(t, ctx, appDSN, natsURL)
	startTestWorker(t, ctx, natsURL)
	startTestControllerManager(t, ctx, adminDSN, natsURL, "1s", "1s")
	lbAddr, _ := startTestLoadBalancer(t, ctx, appDSN, adminDSN, natsURL, "5s")

	containers := db.NewContainerRepository(pool.Conn())
	services := db.NewServiceRepository(pool.Conn())
	instances := db.NewServiceInstanceRepository(pool.Conn())

	waitForHealthyServiceInstances(t, ctx, services, instances, appAID, 1, 90*time.Second)
	waitForHealthyServiceInstances(t, ctx, services, instances, appBID, 1, 90*time.Second)

	for _, depID := range []uuid.UUID{depAID, depBID} {
		running, err := containers.ListByDeployment(ctx, depID)
		if err != nil {
			t.Fatalf("ListByDeployment: %v", err)
		}
		for _, c := range running {
			if c.Status != db.ContainerStatusRunning || c.ContainerRuntimeID == nil {
				continue
			}
			runtimeID := *c.ContainerRuntimeID
			t.Cleanup(func() {
				_ = rt.StopContainer(context.Background(), runtimeID, 5*time.Second)
				_ = rt.RemoveContainer(context.Background(), runtimeID)
			})
		}
	}

	const hostnameA = "app-a.e2e-domains.test"
	const hostnameB = "app-b.e2e-domains.test"
	if _, err := adminDB.ExecContext(ctx,
		`INSERT INTO domains (org_id, project_id, application_id, hostname) VALUES
		 ((SELECT org_id FROM applications WHERE id = $1), (SELECT project_id FROM applications WHERE id = $1), $1, $2),
		 ((SELECT org_id FROM applications WHERE id = $3), (SELECT project_id FROM applications WHERE id = $3), $3, $4)`,
		appAID, hostnameA, appBID, hostnameB,
	); err != nil {
		t.Fatalf("seeding domains: %v", err)
	}

	client := &http.Client{Timeout: 5 * time.Second}

	// The load balancer's resync ticker (1s, set by startTestLoadBalancer)
	// needs a moment to pick up the domain rows just inserted — poll rather
	// than sleep a fixed guess.
	var hostnameSeenA, hostnameSeenB string
	deadline := time.Now().Add(30 * time.Second)
	for {
		hostnameSeenA, err = requestBackendHostnameByHost(client, lbAddr, hostnameA)
		if err == nil {
			hostnameSeenB, err = requestBackendHostnameByHost(client, lbAddr, hostnameB)
		}
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for the load balancer to pick up the registered domains: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	if hostnameSeenA == "" || hostnameSeenB == "" || hostnameSeenA == hostnameSeenB {
		t.Fatalf("expected Host %q and Host %q to reach two distinct backends, got %q and %q", hostnameA, hostnameB, hostnameSeenA, hostnameSeenB)
	}

	// An unregistered Host, and no X-Platform-Service header, must 404 —
	// not silently fall through to either application.
	req, err := http.NewRequest(http.MethodGet, "http://"+lbAddr+"/", nil)
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	req.Host = "unregistered.e2e-domains.test"
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("requesting unregistered host: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unregistered host: status = %d, want 404", resp.StatusCode)
	}
}

// requestBackendHostnameByHost sends one request through the load balancer
// at lbAddr with its Host header set to host (Task 4's real routing key,
// as opposed to requestBackendHostname's ServiceHeader), and extracts
// traefik/whoami's "Hostname: <id>" line from the response body.
func requestBackendHostnameByHost(client *http.Client, lbAddr, host string) (string, error) {
	req, err := http.NewRequest(http.MethodGet, "http://"+lbAddr+"/", nil)
	if err != nil {
		return "", err
	}
	req.Host = host
	return doRequestAndParseHostname(client, req)
}

// TestLoadBalancer_TLSTerminatesWithSelfSignedCert is Task 5's acceptance
// (phase-5-networking-ingress.md): a TLS client dialing the HTTPS listener
// with InsecureSkipVerify and a registered hostname's SNI reaches the right
// backend, and the plain HTTP listener keeps routing the very same hostname
// unchanged alongside it (open decision 4 — no forced redirect). The domain
// row is seeded directly via admin SQL, same precedent as
// TestLoadBalancer_RoutesByHostHeader — this test's job is the TLS
// listener/CertProvider, not the API route Task 2 already covers.
func TestLoadBalancer_TLSTerminatesWithSelfSignedCert(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	rt, err := runtime.NewDockerRuntime()
	if err != nil {
		t.Skipf("docker daemon not available, skipping: %v", err)
	}
	defer func() { _ = rt.Close() }()

	adminDSN, appDSN, adminDB, pool := startTestPostgres(t, ctx)
	defer func() { _ = adminDB.Close() }()

	applicationID, deploymentID := seedApplicationWithReplicas(t, ctx, adminDB, 1)

	natsContainer, err := nats.Run(ctx, "nats:2.11.7")
	if err != nil {
		t.Fatalf("starting nats container: %v", err)
	}
	t.Cleanup(func() { _ = natsContainer.Terminate(context.Background()) })

	natsURL, err := natsContainer.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("nats connection string: %v", err)
	}

	bus, err := eventbus.Connect(natsURL)
	if err != nil {
		t.Fatalf("connecting eventbus: %v", err)
	}
	t.Cleanup(func() { _ = bus.Close() })
	if err := bus.EnsureStream(ctx, eventbus.StreamConfig{
		Name:     eventbus.PlacementStream,
		Subjects: []string{eventbus.PlacementStreamFilter},
	}); err != nil {
		t.Fatalf("ensuring %s stream: %v", eventbus.PlacementStream, err)
	}

	startTestScheduler(t, ctx, appDSN, natsURL)
	startTestWorker(t, ctx, natsURL)
	startTestControllerManager(t, ctx, adminDSN, natsURL, "1s", "1s")
	httpAddr, tlsAddr := startTestLoadBalancer(t, ctx, appDSN, adminDSN, natsURL, "5s")

	containers := db.NewContainerRepository(pool.Conn())
	services := db.NewServiceRepository(pool.Conn())
	instances := db.NewServiceInstanceRepository(pool.Conn())

	waitForHealthyServiceInstances(t, ctx, services, instances, applicationID, 1, 90*time.Second)

	running, err := containers.ListByDeployment(ctx, deploymentID)
	if err != nil {
		t.Fatalf("ListByDeployment: %v", err)
	}
	for _, c := range running {
		if c.Status != db.ContainerStatusRunning || c.ContainerRuntimeID == nil {
			continue
		}
		runtimeID := *c.ContainerRuntimeID
		t.Cleanup(func() {
			_ = rt.StopContainer(context.Background(), runtimeID, 5*time.Second)
			_ = rt.RemoveContainer(context.Background(), runtimeID)
		})
	}

	const hostname = "app.e2e-tls.test"
	if _, err := adminDB.ExecContext(ctx,
		`INSERT INTO domains (org_id, project_id, application_id, hostname)
		 SELECT org_id, project_id, id, $2 FROM applications WHERE id = $1`,
		applicationID, hostname,
	); err != nil {
		t.Fatalf("seeding domain: %v", err)
	}

	tlsClient := newSNIClient(tlsAddr)
	httpClient := &http.Client{Timeout: 5 * time.Second}

	// The resync ticker (1s, set by startTestLoadBalancer) needs a moment to
	// pick up the domain row just inserted — poll rather than sleep a fixed
	// guess.
	var tlsHostname string
	deadline := time.Now().Add(30 * time.Second)
	for {
		tlsHostname, err = requestBackendHostnameTLS(tlsClient, hostname)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for the tls listener to route %q: %v", hostname, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if tlsHostname == "" {
		t.Fatalf("expected a non-empty backend hostname via tls")
	}

	// Open decision 4: plain HTTP keeps routing the same hostname
	// unchanged alongside TLS, no forced redirect.
	httpHostname, err := requestBackendHostnameByHost(httpClient, httpAddr, hostname)
	if err != nil {
		t.Fatalf("requesting via plain http after tls succeeded: %v", err)
	}
	if httpHostname != tlsHostname {
		t.Fatalf("expected http and https to reach the same backend for %q, got http=%q tls=%q", hostname, httpHostname, tlsHostname)
	}
}

// newSNIClient builds an http.Client whose https requests always dial
// tlsAddr directly (the load balancer's real TLS listener in this test,
// bypassing DNS entirely) while still sending the request URL's own
// hostname as the TLS ClientHello's SNI — exactly what a real client
// reaches a self-signed-cert-fronted domain with. InsecureSkipVerify is the
// only honest way to test a self-signed cert (Task 5's own acceptance
// wording, phase-5-networking-ingress.md).
func newSNIClient(tlsAddr string) *http.Client {
	return &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				serverName := addr
				if h, _, err := net.SplitHostPort(addr); err == nil {
					serverName = h
				}
				rawConn, err := (&net.Dialer{}).DialContext(ctx, "tcp", tlsAddr)
				if err != nil {
					return nil, err
				}
				tlsConn := cryptotls.Client(rawConn, &cryptotls.Config{
					ServerName:         serverName,
					InsecureSkipVerify: true,
				})
				if err := tlsConn.HandshakeContext(ctx); err != nil {
					_ = rawConn.Close()
					return nil, err
				}
				return tlsConn, nil
			},
		},
	}
}

// requestBackendHostnameTLS sends one HTTPS request through client (built
// by newSNIClient) to hostname, and extracts traefik/whoami's own
// "Hostname: <id>" line from the response body.
func requestBackendHostnameTLS(client *http.Client, hostname string) (string, error) {
	req, err := http.NewRequest(http.MethodGet, "https://"+hostname+"/", nil)
	if err != nil {
		return "", err
	}
	return doRequestAndParseHostname(client, req)
}

// seedTwoApplications inserts one organization/user/project and two
// applications (traefik/whoami, 1 replica each) with one deployment each —
// Task 4's own two-tenant-application scenario, distinct from
// seedApplicationWithReplicas' single-application/N-replica one. A single
// shared org/project is deliberate (avoids seedApplicationWithReplicas'
// hardcoded organization slug colliding if called twice) and realistic
// (one tenant can register domains for more than one of their own
// applications).
func seedTwoApplications(t *testing.T, ctx context.Context, adminDB *sql.DB) (appAID, appBID, depAID, depBID uuid.UUID) {
	t.Helper()

	var orgID, userID, projectID uuid.UUID
	if err := adminDB.QueryRowContext(ctx,
		`INSERT INTO organizations (name, slug) VALUES ('Org', 'org') RETURNING id`,
	).Scan(&orgID); err != nil {
		t.Fatalf("seeding organization: %v", err)
	}
	if err := adminDB.QueryRowContext(ctx,
		`INSERT INTO users (email, password_hash) VALUES ('user@example.com', 'hash') RETURNING id`,
	).Scan(&userID); err != nil {
		t.Fatalf("seeding user: %v", err)
	}
	if err := adminDB.QueryRowContext(ctx,
		`INSERT INTO projects (org_id, name, slug) VALUES ($1, 'Project', 'project') RETURNING id`, orgID,
	).Scan(&projectID); err != nil {
		t.Fatalf("seeding project: %v", err)
	}

	seedOne := func(name, slug string) (applicationID, deploymentID uuid.UUID) {
		if err := adminDB.QueryRowContext(ctx,
			`INSERT INTO applications (org_id, project_id, name, image, replicas_desired, ports)
			 VALUES ($1, $2, $3, 'traefik/whoami', 1, '[{"container_port": 80}]'::jsonb) RETURNING id`,
			orgID, projectID, name,
		).Scan(&applicationID); err != nil {
			t.Fatalf("seeding application %q: %v", name, err)
		}
		if err := adminDB.QueryRowContext(ctx,
			`INSERT INTO deployments (org_id, application_id, image, revision, created_by) VALUES ($1, $2, 'traefik/whoami', 1, $3) RETURNING id`,
			orgID, applicationID, userID,
		).Scan(&deploymentID); err != nil {
			t.Fatalf("seeding deployment for %q: %v", name, err)
		}
		return applicationID, deploymentID
	}

	appAID, depAID = seedOne("app-a", "app-a")
	appBID, depBID = seedOne("app-b", "app-b")
	return appAID, appBID, depAID, depBID
}

// seedApplicationWithReplicas inserts one organization/user/project/
// application/deployment row (as the admin, bypassing RLS) with
// replicas_desired set to replicas, image traefik/whoami (its response body
// names the exact container that served a request — this test's only way
// to observe which of 3 identical backends the load balancer picked), and a
// single exposed container port so a real host port actually gets bound
// (Task 2) for the Service Instance controller (Task 3) to observe. Mirrors
// controller-manager's own copy of this helper (duplicated rather than
// imported, ADR-0012's "no service depends on another's exact shape"
// reasoning applied to test helpers too).
func seedApplicationWithReplicas(t *testing.T, ctx context.Context, adminDB *sql.DB, replicas int) (applicationID, deploymentID uuid.UUID) {
	t.Helper()

	var orgID, userID, projectID uuid.UUID
	if err := adminDB.QueryRowContext(ctx,
		`INSERT INTO organizations (name, slug) VALUES ('Org', 'org') RETURNING id`,
	).Scan(&orgID); err != nil {
		t.Fatalf("seeding organization: %v", err)
	}
	if err := adminDB.QueryRowContext(ctx,
		`INSERT INTO users (email, password_hash) VALUES ('user@example.com', 'hash') RETURNING id`,
	).Scan(&userID); err != nil {
		t.Fatalf("seeding user: %v", err)
	}
	if err := adminDB.QueryRowContext(ctx,
		`INSERT INTO projects (org_id, name, slug) VALUES ($1, 'Project', 'project') RETURNING id`, orgID,
	).Scan(&projectID); err != nil {
		t.Fatalf("seeding project: %v", err)
	}
	if err := adminDB.QueryRowContext(ctx,
		`INSERT INTO applications (org_id, project_id, name, image, replicas_desired, ports)
		 VALUES ($1, $2, 'app', 'traefik/whoami', $3, '[{"container_port": 80}]'::jsonb) RETURNING id`,
		orgID, projectID, replicas,
	).Scan(&applicationID); err != nil {
		t.Fatalf("seeding application: %v", err)
	}
	if err := adminDB.QueryRowContext(ctx,
		`INSERT INTO deployments (org_id, application_id, image, revision, created_by) VALUES ($1, $2, 'traefik/whoami', 1, $3) RETURNING id`,
		orgID, applicationID, userID,
	).Scan(&deploymentID); err != nil {
		t.Fatalf("seeding deployment: %v", err)
	}
	return applicationID, deploymentID
}

// waitForHealthyServiceInstances polls until applicationID's service has
// exactly want healthy instances, returning the service row once it does.
// Mirrors controller-manager's own copy of this helper (ADR-0012).
func waitForHealthyServiceInstances(t *testing.T, ctx context.Context, services db.ServiceRepository, instances db.ServiceInstanceRepository, applicationID uuid.UUID, want int, timeout time.Duration) db.Service {
	t.Helper()

	deadline := time.After(timeout)
	for {
		svc, err := services.GetByApplication(ctx, applicationID)
		switch {
		case err == nil:
			healthy, err := instances.ListHealthyByService(ctx, svc.ID)
			if err != nil {
				t.Fatalf("ListHealthyByService: %v", err)
			}
			if len(healthy) == want {
				return svc
			}
		case errors.Is(err, db.ErrNotFound):
			// Service not lazily created yet; keep polling.
		default:
			t.Fatalf("GetByApplication: %v", err)
		}

		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %d healthy service instances for application %s", want, applicationID)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// startTestPostgres starts a real Postgres container, applies migrations,
// and returns the admin (superuser) connection string, the app-role
// connection string, an admin *sql.DB (for seeding), and an app-role
// *db.Pool (for reading back results). Mirrors controller-manager's own
// copy of this helper (ADR-0012).
func startTestPostgres(t *testing.T, ctx context.Context) (adminDSN, appDSN string, adminDB *sql.DB, pool *db.Pool) {
	t.Helper()

	container, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("platform"),
		postgres.WithUsername("platform"),
		postgres.WithPassword("platform"),
	)
	if err != nil {
		t.Fatalf("starting postgres container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	adminConnStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("admin connection string: %v", err)
	}
	adminDB, err = sql.Open("pgx", adminConnStr)
	if err != nil {
		t.Fatalf("opening admin connection: %v", err)
	}

	var pingErr error
	for i := 0; i < 30; i++ {
		if pingErr = adminDB.PingContext(ctx); pingErr == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if pingErr != nil {
		t.Fatalf("waiting for postgres to accept connections: %v", pingErr)
	}

	migrationsDir := filepath.Join("..", "..", "infrastructure", "postgres", "migrations")
	if err := goose.SetDialect("postgres"); err != nil {
		t.Fatalf("goose.SetDialect: %v", err)
	}
	if err := goose.Up(adminDB, migrationsDir); err != nil {
		t.Fatalf("applying migrations: %v", err)
	}

	endpoint, err := container.PortEndpoint(ctx, "5432/tcp", "")
	if err != nil {
		t.Fatalf("resolving container endpoint: %v", err)
	}
	appConnStr := fmt.Sprintf("postgres://platform_app:platform_app@%s/platform?sslmode=disable", endpoint)

	pool, err = db.Open(ctx, appConnStr)
	if err != nil {
		t.Fatalf("opening app-role pool: %v", err)
	}
	t.Cleanup(pool.Close)

	return adminConnStr, appConnStr, adminDB, pool
}

// startTestScheduler builds and runs the real scheduler binary as a
// separate OS process (ADR-0012), pointed at dbURL/natsURL with fast
// liveness-sweep settings. Mirrors controller-manager's own copy.
func startTestScheduler(t *testing.T, ctx context.Context, dbURL, natsURL string) {
	t.Helper()

	goBin, err := goBinary()
	if err != nil {
		t.Fatalf("locating go toolchain: %v", err)
	}

	binPath := filepath.Join(t.TempDir(), "scheduler-under-test.exe")
	// #nosec G204 -- goBin is resolved by this file's own goBinary(), never
	// from request/environment-controlled input; this is test setup, not a
	// request-handling path.
	build := exec.CommandContext(ctx, goBin, "build", "-o", binPath, "platform/services/scheduler")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building scheduler binary: %v\n%s", err, out)
	}

	// #nosec G204 -- binPath is the binary this same test just built into
	// t.TempDir(), not external input.
	cmd := exec.CommandContext(ctx, binPath)
	cmd.Env = append(os.Environ(),
		"APP_DATABASE_URL="+dbURL,
		"SCHEDULER_NATS_URL="+natsURL,
		"SCHEDULER_HEARTBEAT_TIMEOUT=5s",
		"SCHEDULER_LIVENESS_SWEEP_INTERVAL=1s",
	)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting scheduler process: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if t.Failed() {
			t.Logf("scheduler process output:\n%s", output.String())
		}
	})
}

// startTestWorker builds and runs a real worker binary as a separate OS
// process, pointed at natsURL. Mirrors controller-manager's own copy.
func startTestWorker(t *testing.T, ctx context.Context, natsURL string) {
	t.Helper()

	goBin, err := goBinary()
	if err != nil {
		t.Fatalf("locating go toolchain: %v", err)
	}

	binPath := filepath.Join(t.TempDir(), "worker-under-test.exe")
	// #nosec G204 -- see startTestScheduler's identical justification above.
	build := exec.CommandContext(ctx, goBin, "build", "-o", binPath, "platform/services/worker")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building worker binary: %v\n%s", err, out)
	}

	nodeIDFile := filepath.Join(t.TempDir(), "worker-node-id")

	// #nosec G204 -- binPath is the binary this same test just built into
	// t.TempDir(), not external input.
	cmd := exec.CommandContext(ctx, binPath)
	cmd.Env = append(os.Environ(),
		"WORKER_LISTEN_ADDR=127.0.0.1:0",
		"WORKER_NATS_URL="+natsURL,
		"WORKER_NODE_ID_FILE="+nodeIDFile,
		"WORKER_HEARTBEAT_INTERVAL=1s",
		"WORKER_HEALTH_CHECK_INTERVAL=1s",
		"WORKER_CPU_CAPACITY_MILLICORES=1500",
	)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting worker process: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if t.Failed() {
			t.Logf("worker process output:\n%s", output.String())
		}
	})
}

// startTestControllerManager builds and runs the real controller-manager
// binary under test as a separate OS process (ADR-0012), pointed at the
// admin DSN (db.ReconcileRepository/ServiceReconcileRepository both require
// an RLS-bypass connection). Both reconcile intervals are exposed
// explicitly (rather than hardcoded fast, like the other test helpers in
// this file) so Task 5's test can set them deliberately slow — proving the
// load balancer's own active health check removes a dead backend well
// before either of controller-manager's own reconcile loops ever would,
// not by accident of both being fast. reconcileInterval slow is just as
// necessary as serviceReconcileInterval slow here: otherwise the Deployment
// controller (phase-3-controllers.md Task 4) would notice the stopped
// container's replica count deficit and schedule a replacement mid-test,
// leaking an extra container this test's own cleanup never accounts for.
// Mirrors controller-manager's own copy.
func startTestControllerManager(t *testing.T, ctx context.Context, adminDSN, natsURL, reconcileInterval, serviceReconcileInterval string) {
	t.Helper()

	goBin, err := goBinary()
	if err != nil {
		t.Fatalf("locating go toolchain: %v", err)
	}

	binPath := filepath.Join(t.TempDir(), "controller-manager-under-test.exe")
	// #nosec G204 -- goBin is resolved by this file's own goBinary(), never
	// from request/environment-controlled input; this is test setup, not a
	// request-handling path.
	build := exec.CommandContext(ctx, goBin, "build", "-o", binPath, "platform/services/controller-manager")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building controller-manager binary: %v\n%s", err, out)
	}

	// #nosec G204 -- binPath is the binary this same test just built into
	// t.TempDir(), not external input.
	cmd := exec.CommandContext(ctx, binPath)
	cmd.Env = append(os.Environ(),
		"DATABASE_URL="+adminDSN,
		"CONTROLLER_NATS_URL="+natsURL,
		"CONTROLLER_RECONCILE_INTERVAL="+reconcileInterval,
		"CONTROLLER_SERVICE_RECONCILE_INTERVAL="+serviceReconcileInterval,
	)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting controller-manager process: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if t.Failed() {
			t.Logf("controller-manager process output:\n%s", output.String())
		}
	})
}

// startTestLoadBalancer builds and runs the real loadbalancer binary under
// test as a separate OS process (ADR-0012), pointed at dbURL/natsURL, with
// a fast resync interval and a reserved free port to actually send HTTP
// requests against. adminDBURL backs the Task 4 (phase-5-networking-
// ingress.md) DomainRoutingRepository connection — without it the process
// falls back to LOADBALANCER_ADMIN_DATABASE_URL's own production default
// and fails to start against this test's actual (randomly-ported)
// container. healthCheckInterval is exposed explicitly so Task 5's test
// can set it fast (its own acceptance criterion is "within one
// health-check interval"). Returns the plain-HTTP and TLS addresses it's
// listening on (Task 5, phase-5-networking-ingress.md).
func startTestLoadBalancer(t *testing.T, ctx context.Context, dbURL, adminDBURL, natsURL, healthCheckInterval string) (httpAddr, tlsAddr string) {
	t.Helper()

	goBin, err := goBinary()
	if err != nil {
		t.Fatalf("locating go toolchain: %v", err)
	}

	binPath := filepath.Join(t.TempDir(), "loadbalancer-under-test.exe")
	// #nosec G204 -- goBin is resolved by this file's own goBinary(), never
	// from request/environment-controlled input; this is test setup, not a
	// request-handling path.
	build := exec.CommandContext(ctx, goBin, "build", "-o", binPath, "platform/services/loadbalancer")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building loadbalancer binary: %v\n%s", err, out)
	}

	httpAddr = fmt.Sprintf("127.0.0.1:%d", freeTCPPort(t))
	tlsAddr = fmt.Sprintf("127.0.0.1:%d", freeTCPPort(t))

	// #nosec G204 -- binPath is the binary this same test just built into
	// t.TempDir(), not external input.
	cmd := exec.CommandContext(ctx, binPath)
	cmd.Env = append(os.Environ(),
		"APP_DATABASE_URL="+dbURL,
		"LOADBALANCER_ADMIN_DATABASE_URL="+adminDBURL,
		"LOADBALANCER_NATS_URL="+natsURL,
		"LOADBALANCER_LISTEN_ADDR="+httpAddr,
		"LOADBALANCER_TLS_LISTEN_ADDR="+tlsAddr,
		"LOADBALANCER_RESYNC_INTERVAL=1s",
		"LOADBALANCER_HEALTH_CHECK_INTERVAL="+healthCheckInterval,
	)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting loadbalancer process: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if t.Failed() {
			t.Logf("loadbalancer process output:\n%s", output.String())
		}
	})
	return httpAddr, tlsAddr
}

// freeTCPPort reserves an OS-assigned free TCP port and immediately
// releases it, for a subprocess to bind moments later. Small inherent race
// (another process could grab it first) accepted as standard practice for
// this kind of subprocess integration test.
func freeTCPPort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving a free tcp port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// goBinary locates the go toolchain. Mirrors controller-manager's own
// identical copy of this helper (this shell's PATH doesn't reliably
// include Go's bin directory).
func goBinary() (string, error) {
	if p, err := exec.LookPath("go"); err == nil {
		return p, nil
	}

	name := "go"
	if goruntime.GOOS == "windows" {
		name = "go.exe"
	}
	if goroot := os.Getenv("GOROOT"); goroot != "" {
		candidate := filepath.Join(goroot, "bin", name)
		if fileExists(candidate) {
			return candidate, nil
		}
	}
	for _, candidate := range []string{`C:\Program Files\Go\bin\go.exe`, "/usr/local/go/bin/go"} {
		if fileExists(candidate) {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("go toolchain not found on PATH, GOROOT, or a known install location")
}

func fileExists(path string) bool {
	// #nosec G703 -- path is always either $GOROOT/bin/go(.exe) or one of
	// this function's own hardcoded candidates, never external input.
	_, err := os.Stat(path)
	return err == nil
}

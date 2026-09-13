//go:build e2e

package e2e

import (
	"bytes"
	"context"
	cryptotls "crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestE2E_Phase5ExitCriteria automates the exit-criteria script named in
// ARCHITECTURE.md §10 for Phase 5, restated at the top of
// phase-5-networking-ingress.md — this test running green is what actually
// certifies Phase 5 done, exactly as TestE2E_Phase1ExitCriteria through
// TestE2E_Phase4ExitCriteria did for their phases (phase-5-networking-
// ingress.md Task 6):
//
//	two different applications reachable via two different hostnames
//	through the same load balancer
//
// The full script this test drives: deploy two applications, register one
// domain per application, send requests through the load balancer with
// each distinct Host header (both plain HTTP and HTTPS with
// InsecureSkipVerify), confirm each reaches its own application and not
// the other's.
//
// Reuses phase1_test.go's/phase2_test.go's/phase3_test.go's process/
// testcontainer helpers (same package: startPostgres, startNATS,
// startScheduler, startWorker, startControllerManager, buildBinary,
// newCLIRunner, requireContains, dockerContainerIDs, removeNewContainers,
// mustFreeAddr, goBinary, exeSuffix, ...) plus a bespoke startLoadBalancer
// helper here, since Task 5's second TLS listener means the load balancer
// (unlike every other service phase4_test.go starts via the shared
// startService) needs two free ports and two env vars, not one.
func TestE2E_Phase5ExitCriteria(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	goBin := goBinary(t)
	binDir := t.TempDir()

	containersBefore := dockerContainerIDs(t, "traefik/whoami")
	t.Cleanup(func() { removeNewContainers(t, "traefik/whoami", containersBefore) })

	adminDBURL, dbURL := startPostgres(t, ctx)
	natsURL := startNATS(t, ctx)

	startScheduler(t, ctx, goBin, binDir, adminDBURL, dbURL, natsURL)

	workerBinPath := filepath.Join(binDir, "worker"+exeSuffix())
	buildBinary(t, ctx, goBin, workerBinPath, "platform/services/worker")
	worker := startWorker(t, ctx, workerBinPath, "e2e-worker-1", natsURL)

	// Fast reconcile ticks (placeholder production defaults are 5s) so this
	// test doesn't wait out production tuning, same precedent as
	// TestE2E_Phase4ExitCriteria.
	startControllerManager(t, ctx, goBin, binDir, adminDBURL, natsURL,
		"CONTROLLER_RECONCILE_INTERVAL=1s",
		"CONTROLLER_SERVICE_RECONCILE_INTERVAL=1s",
	)

	// Fast resync (placeholder production default is 30s) — Task 4's
	// domain-routing table refresh has no push path (open decision 2), so
	// this is the only clock a registered domain becomes routable on.
	httpURL, tlsAddr := startLoadBalancer(t, ctx, goBin, binDir, dbURL, adminDBURL, natsURL,
		"LOADBALANCER_RESYNC_INTERVAL=1s",
	)

	apiURL := startService(t, ctx, goBin, binDir, "apiserver", "platform/services/apiserver",
		"APISERVER_LISTEN_ADDR", []string{
			"APP_DATABASE_URL=" + dbURL,
			"WORKER_ADDR=" + worker.baseURL,
			"APISERVER_NATS_URL=" + natsURL,
			"JWT_SIGNING_KEY=e2e-test-signing-key",
		}, "/")

	cliPath := filepath.Join(binDir, "platform"+exeSuffix())
	buildBinary(t, ctx, goBin, cliPath, "platform/apps/cli")
	run := newCLIRunner(t, ctx, cliPath, t.TempDir(), apiURL)

	email := fmt.Sprintf("e2e-phase5-%d@example.com", time.Now().UnixNano())
	out := run("signup", "--email", email, "--password", "hunter22-hunter22")
	requireContains(t, out, "Signed up as "+email)

	out = run("create", "project", "demo")
	requireContains(t, out, "Created project demo")

	// deploy two applications
	out = run("deploy", "app-a", "--image", "traefik/whoami", "--port", "80")
	requireContains(t, out, "revision 1")
	out = run("deploy", "app-b", "--image", "traefik/whoami", "--port", "80")
	requireContains(t, out, "revision 1")

	// register one domain per application
	const hostnameA = "app-a.e2e-phase5.test"
	const hostnameB = "app-b.e2e-phase5.test"
	out = run("create", "domain", hostnameA, "--app", "app-a")
	requireContains(t, out, "Registered domain "+hostnameA)
	out = run("create", "domain", hostnameB, "--app", "app-b")
	requireContains(t, out, "Registered domain "+hostnameB)

	out = run("get", "domains")
	requireContains(t, out, hostnameA)
	requireContains(t, out, hostnameB)

	// send requests through the load balancer with each distinct Host
	// header, plain HTTP first — poll rather than sleep a fixed guess,
	// since this needs both the worker's container to actually be up and
	// the load balancer's resync tick to have picked up the domain rows
	// just registered.
	httpClient := &http.Client{Timeout: 5 * time.Second}
	var httpHostnameA, httpHostnameB string
	deadline := time.Now().Add(60 * time.Second)
	var lastErr error
	for {
		httpHostnameA, lastErr = requestBackendHostnameByHost(httpClient, httpURL, hostnameA)
		if lastErr == nil {
			httpHostnameB, lastErr = requestBackendHostnameByHost(httpClient, httpURL, hostnameB)
		}
		if lastErr == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for the load balancer to route both registered domains over http: %v", lastErr)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// confirm each reaches its own application, not the other's
	if httpHostnameA == "" || httpHostnameB == "" || httpHostnameA == httpHostnameB {
		t.Fatalf("expected Host %q and Host %q to reach two distinct backends over http, got %q and %q", hostnameA, hostnameB, httpHostnameA, httpHostnameB)
	}

	// and again over HTTPS with InsecureSkipVerify — Task 5's own
	// acceptance wording, the only honest way to test a self-signed cert.
	tlsClient := newSNIClient(tlsAddr)
	tlsHostnameA, err := requestBackendHostnameTLS(tlsClient, hostnameA)
	if err != nil {
		t.Fatalf("requesting %q over https: %v", hostnameA, err)
	}
	tlsHostnameB, err := requestBackendHostnameTLS(tlsClient, hostnameB)
	if err != nil {
		t.Fatalf("requesting %q over https: %v", hostnameB, err)
	}
	if tlsHostnameA != httpHostnameA {
		t.Fatalf("expected %q to reach the same backend over http and https, got http=%q https=%q", hostnameA, httpHostnameA, tlsHostnameA)
	}
	if tlsHostnameB != httpHostnameB {
		t.Fatalf("expected %q to reach the same backend over http and https, got http=%q https=%q", hostnameB, httpHostnameB, tlsHostnameB)
	}

	// An unregistered Host must not silently fall through to either
	// application (proxy.ServeHTTP's 404 behavior, Task 4).
	req, err := http.NewRequest(http.MethodGet, httpURL+"/", nil)
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	req.Host = "unregistered.e2e-phase5.test"
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("requesting unregistered host: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unregistered host: status = %d, want 404", resp.StatusCode)
	}
}

// startLoadBalancer builds and runs the real loadbalancer binary as a
// separate OS process, bound to two free ports — plain HTTP
// (LOADBALANCER_LISTEN_ADDR) and TLS (LOADBALANCER_TLS_LISTEN_ADDR, Task 5)
// — since the shared startService helper only manages one listener.
// Readiness is polled on the plain-HTTP port only: both listeners share the
// same underlying registry and handler (main.go), so one becoming
// reachable means the process is fully up. Returns the plain-HTTP base URL
// and the TLS listener's address.
func startLoadBalancer(t *testing.T, ctx context.Context, goBin, binDir, dbURL, adminDBURL, natsURL string, extraEnv ...string) (httpURL, tlsAddr string) {
	t.Helper()

	binPath := filepath.Join(binDir, "loadbalancer"+exeSuffix())
	buildBinary(t, ctx, goBin, binPath, "platform/services/loadbalancer")

	httpAddr := mustFreeAddr(t)
	tlsAddr = mustFreeAddr(t)

	env := append([]string{
		"APP_DATABASE_URL=" + dbURL,
		"LOADBALANCER_ADMIN_DATABASE_URL=" + adminDBURL,
		"LOADBALANCER_NATS_URL=" + natsURL,
		"LOADBALANCER_LISTEN_ADDR=" + httpAddr,
		"LOADBALANCER_TLS_LISTEN_ADDR=" + tlsAddr,
	}, extraEnv...)

	// #nosec G204 -- binPath is a binary this same test just built into
	// t.TempDir(), not external input.
	cmd := exec.CommandContext(ctx, binPath)
	cmd.Env = append(os.Environ(), env...)
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

	httpURL = "http://" + httpAddr
	deadline := time.Now().Add(20 * time.Second)
	client := &http.Client{Timeout: 2 * time.Second}
	for time.Now().Before(deadline) {
		if resp, err := client.Get(httpURL + "/"); err == nil {
			_ = resp.Body.Close()
			return httpURL, tlsAddr
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("loadbalancer process at %s did not become ready in time", httpURL)
	return "", ""
}

// newSNIClient builds an http.Client whose https requests always dial
// tlsAddr directly (the load balancer's real TLS listener, bypassing DNS
// entirely) while still sending the request URL's own hostname as the TLS
// ClientHello's SNI — exactly what a real client reaches a self-signed-
// cert-fronted domain with. InsecureSkipVerify is the only honest way to
// test a self-signed cert (Task 5's own acceptance wording, phase-5-
// networking-ingress.md). Mirrors services/loadbalancer/main_integration_
// test.go's own helper of the same shape (duplicated, not imported —
// internal/proxy isn't importable from outside services/loadbalancer, and
// e2e is held to the same external-contract-only boundary, ADR-0012).
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

// requestBackendHostnameByHost sends one plain-HTTP request through the
// load balancer at baseURL with its Host header set to host (Task 4's real
// routing key), and returns traefik/whoami's own "Hostname: <id>" line
// from the response body — the only way to observe, from outside, which
// backend actually served a given request.
func requestBackendHostnameByHost(client *http.Client, baseURL, host string) (string, error) {
	req, err := http.NewRequest(http.MethodGet, baseURL+"/", nil)
	if err != nil {
		return "", err
	}
	req.Host = host
	return doRequestAndParseHostname(client, req)
}

// requestBackendHostnameTLS sends one HTTPS request through client (built
// by newSNIClient) to hostname, and returns traefik/whoami's own
// "Hostname: <id>" line from the response body.
func requestBackendHostnameTLS(client *http.Client, hostname string) (string, error) {
	req, err := http.NewRequest(http.MethodGet, "https://"+hostname+"/", nil)
	if err != nil {
		return "", err
	}
	return doRequestAndParseHostname(client, req)
}

// doRequestAndParseHostname executes req and extracts traefik/whoami's own
// "Hostname: <id>" line from the response body — the only way to observe,
// from outside, which of the identical backends actually served a given
// request. Mirrors services/loadbalancer/main_integration_test.go's own
// helper of the same name (duplicated, not imported, for the same reason
// as newSNIClient above).
func doRequestAndParseHostname(client *http.Client, req *http.Request) (string, error) {
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()

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

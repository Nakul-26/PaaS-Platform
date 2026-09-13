//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestE2E_Phase6ExitCriteria automates ARCHITECTURE.md §10's Phase 6 exit
// criteria, restated at the top of phase-6-multi-tenant-saas.md (Task 9):
//
//	R4's cross-tenant-access integration tests pass; a developer-role user
//	is denied an organization.manage action server-side even if the
//	frontend check is bypassed
//
// The script (phase-6-multi-tenant-saas.md Task 9): two users, two orgs,
// with a developer-role member seeded into org A → the cross-tenant
// scenario (org A's developer cannot read/write org B's resources) → the
// literal exit-criteria assertion, sent directly over raw HTTP rather than
// through the CLI (proving the enforcement is real even if a client-side
// check were bypassed, exactly the exit criteria's own wording): a
// developer-role user's request to PATCH /v1/orgs/:orgId (an
// organization.manage action) is rejected 403.
//
// Every "attacker's-eye-view" request in this test — org A's developer
// acting against org B, and against org A's own organization.manage route
// — goes over raw net/http with a bare bearer token, never through the
// CLI: the CLI's session model tracks exactly one "current org" per
// signup/login (cfg.OrgID, phase-6-multi-tenant-saas.md Task 3's own
// design — see get_org.go), so a user who is a *member of two orgs* (their
// own from signup, plus the one they were invited into) has no CLI verb to
// act as the invited role in the second org anyway. Raw HTTP is therefore
// not just the exit criteria's own wording, it's the only way to drive
// this scenario at all.
//
// Reuses phase1_test.go's process/testcontainer helpers (startPostgres,
// startNATS, startService, buildBinary, newCLIRunner, requireContains,
// goBinary, exeSuffix, ...). No scheduler/worker/controller-manager/load-
// balancer processes: unlike Phases 1-5, this phase's own exit-criteria
// script never deploys or routes a container — it's entirely org/project/
// membership/permission surface — so apiserver is the only separate
// process needed.
func TestE2E_Phase6ExitCriteria(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	goBin := goBinary(t)
	binDir := t.TempDir()

	_, dbURL := startPostgres(t, ctx)
	natsURL := startNATS(t, ctx)

	apiURL := startService(t, ctx, goBin, binDir, "apiserver", "platform/services/apiserver",
		"APISERVER_LISTEN_ADDR", []string{
			"APP_DATABASE_URL=" + dbURL,
			"APISERVER_NATS_URL=" + natsURL,
			"JWT_SIGNING_KEY=e2e-test-signing-key",
		}, "/")

	cliPath := filepath.Join(binDir, "platform"+exeSuffix())
	buildBinary(t, ctx, goBin, cliPath, "platform/apps/cli")

	// Org A: owner signs up via the real CLI, exactly like every other
	// e2e test's setup steps.
	runA := newCLIRunner(t, ctx, cliPath, t.TempDir(), apiURL)
	emailA := fmt.Sprintf("e2e-phase6-owner-a-%d@example.com", time.Now().UnixNano())
	out := runA("signup", "--email", emailA, "--password", "hunter22-hunter22")
	requireContains(t, out, "Signed up as "+emailA)
	out = runA("create", "project", "demo")
	requireContains(t, out, "Created project demo")
	orgAID := extractOrgID(t, runA("get", "org"))

	// Org B: a second, unrelated owner/org — the "two orgs" half of the
	// script, and the cross-tenant target below.
	runB := newCLIRunner(t, ctx, cliPath, t.TempDir(), apiURL)
	emailB := fmt.Sprintf("e2e-phase6-owner-b-%d@example.com", time.Now().UnixNano())
	out = runB("signup", "--email", emailB, "--password", "hunter22-hunter22")
	requireContains(t, out, "Signed up as "+emailB)
	orgBID := extractOrgID(t, runB("get", "org"))

	// A third, already-registered user (open decision 3: invite only works
	// for an existing account) — signed up over raw HTTP directly, since
	// the only thing needed from this account is its own access token, not
	// a CLI session tracking its own (irrelevant) default org.
	emailD := fmt.Sprintf("e2e-phase6-developer-%d@example.com", time.Now().UnixNano())
	tokenD := signupRawHTTP(t, ctx, apiURL, emailD, "hunter22-hunter22")

	// Owner A invites D into org A as developer (seeded developer-role
	// member, phase-6-multi-tenant-saas.md Task 9's script).
	out = runA("invite", emailD, "--role", "developer")
	requireContains(t, out, fmt.Sprintf("Added %s to", emailD))
	out = runA("get", "members")
	requireContains(t, out, emailD)
	requireContains(t, out, "developer")

	// Sanity: D really is authenticated and really is a member of org A —
	// a plain read of their own org succeeds. Without this, a bug that
	// rejected every request with 403/404 regardless of reason would make
	// the denial assertions below pass for the wrong reason.
	status, _ := doRawHTTP(t, ctx, http.MethodGet, apiURL+"/v1/orgs/"+orgAID, tokenD, nil)
	if status != http.StatusOK {
		t.Fatalf("GET org A as its own developer member: status = %d, want 200 (sanity check that D's token/membership actually work)", status)
	}

	// Cross-tenant scenario: org A's developer cannot read org B's
	// resources — D has no membership in org B at all, so this must be
	// 404 (don't leak org B's existence to a non-member), never 403, per
	// rbac-multitenancy.md §5's don't-leak-existence principle.
	status, _ = doRawHTTP(t, ctx, http.MethodGet, apiURL+"/v1/orgs/"+orgBID, tokenD, nil)
	if status != http.StatusNotFound {
		t.Fatalf("GET org B as org A's developer (cross-tenant read): status = %d, want 404", status)
	}

	// ...nor write them: creating a project inside org B as a non-member
	// of org B.
	status, _ = doRawHTTP(t, ctx, http.MethodPost, apiURL+"/v1/orgs/"+orgBID+"/projects", tokenD,
		map[string]any{"name": "cross-tenant-write-attempt"})
	if status != http.StatusNotFound {
		t.Fatalf("POST project into org B as org A's developer (cross-tenant write): status = %d, want 404", status)
	}

	// The literal exit-criteria assertion: a developer-role user's raw
	// HTTP request to PATCH /v1/orgs/:orgId (an organization.manage
	// action) is rejected 403 server-side — this time same-org (D really
	// is a member of org A), so the denial must be 403 (insufficient
	// role), not 404 (not a member at all).
	status, _ = doRawHTTP(t, ctx, http.MethodPatch, apiURL+"/v1/orgs/"+orgAID, tokenD,
		map[string]any{"name": "hostile-takeover"})
	if status != http.StatusForbidden {
		t.Fatalf("PATCH org A as its own developer member (organization.manage): status = %d, want 403", status)
	}

	// Contrast check: the same action, same org, performed by the actual
	// owner through the CLI, succeeds — proving the 403 above is really
	// about D's role, not a broken/misrouted PATCH route.
	out = runA("update", "org", "--name", "Org A Renamed")
	requireContains(t, out, "-> name \"Org A Renamed\"")
}

// extractOrgID parses the "ID:         <uuid>" line `platform get org`
// prints (get_org.go) — the CLI's only surface for an org's own ID; there
// is no `--output json` mode to parse instead.
func extractOrgID(t *testing.T, out string) string {
	t.Helper()
	for _, line := range strings.Split(out, "\n") {
		if id, ok := strings.CutPrefix(line, "ID:"); ok {
			return strings.TrimSpace(id)
		}
	}
	t.Fatalf("expected a line starting with %q in `platform get org` output, got:\n%s", "ID:", out)
	return ""
}

// signupRawHTTP signs a brand-new account up directly over HTTP (never
// through the CLI) and returns its access token — used for the one account
// in this test (D) whose CLI session/default org is never used, only its
// bearer token, which every subsequent request in this test also sends
// directly over raw HTTP per the exit criteria's own wording.
func signupRawHTTP(t *testing.T, ctx context.Context, apiURL, email, password string) string {
	t.Helper()
	status, body := doRawHTTP(t, ctx, http.MethodPost, apiURL+"/v1/auth/signup", "", map[string]any{
		"email":    email,
		"password": password,
	})
	if status != http.StatusCreated {
		t.Fatalf("signing up %s over raw HTTP: status = %d, body: %s", email, status, body)
	}
	var resp struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decoding signup response for %s: %v, body: %s", email, err, body)
	}
	if resp.AccessToken == "" {
		t.Fatalf("signup response for %s had no access_token, body: %s", email, body)
	}
	return resp.AccessToken
}

// doRawHTTP sends one request directly over net/http — no CLI, no
// apiclient package — with an optional bearer token and a JSON body,
// returning the response status and raw body. This is the "even if the
// frontend check is bypassed" half of the exit criteria: every assertion
// this test makes about server-side enforcement goes through this
// function, never through the CLI's own request-building code, so a bug
// that only the CLI happened to work around could never hide here.
func doRawHTTP(t *testing.T, ctx context.Context, method, url, bearerToken string, body any) (status int, respBody []byte) {
	t.Helper()

	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshaling request body for %s %s: %v", method, url, err)
		}
		reader = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		t.Fatalf("building request %s %s: %v", method, url, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+bearerToken)
	}

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("performing request %s %s: %v", method, url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading response body for %s %s: %v", method, url, err)
	}
	return resp.StatusCode, respBody
}

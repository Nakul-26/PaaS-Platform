//go:build integration

package server

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	"platform/internal/auth"
	"platform/internal/db"
	"platform/internal/eventbus"
)

// TestAPIServer_RBACPermissionMatrixSweep is Task 8's acceptance
// (phase-6-multi-tenant-saas.md, rbac-multitenancy.md §5): a consolidated
// pass confirming every row of rbac-multitenancy.md §2 has real permission-
// matrix and cross-tenant-denial coverage. This is a coverage-audit task,
// not new product surface (the phase doc's own framing) — most rows already
// have full coverage in an existing test, listed below rather than
// duplicated here. This file closes the specific gaps that audit found:
// earlier tests exercised the success path (usually just the owner) and the
// cross-tenant path for *some* routes, but never ran every role against
// project.create, application.create/delete/deploy/scale — and
// TestAPIServer_ScaleApplication in particular had no permission or
// cross-tenant coverage at all, only a functional happy path.
//
// Coverage already living elsewhere (not duplicated here):
//   - organization.manage / organization.delete: TestAPIServer_Organizations
//   - members.invite / members.remove / members.change_role: TestAPIServer_Members
//   - domains.manage (developer allowed, viewer denied) + cross-tenant: TestAPIServer_Domains
//   - audit_logs.view: TestAPIServer_AuditLogs
//   - api_keys.create / api_keys.revoke: TestAPIServer_APIKeys
//   - project.create's quota interaction (not its permission matrix): TestAPIServer_Quotas
//   - deployment.rollback, env_vars.write, metrics.view, billing.view,
//     project.delete: no route exists yet (constants-only per Task 2's own
//     doc comment) — covered at the abstract level by
//     internal/auth/rbac_test.go's TestHasPermission_Matrix (all 20
//     permissions × all 4 roles), which is the only test that can exist
//     for a permission with nothing gating it yet.
//
// Closed here: project.create, application.create/delete/deploy/scale
// (matrix + cross-tenant), domains.manage's missing admin case, and
// logs.view's viewer-is-not-forbidden case.
func TestAPIServer_RBACPermissionMatrixSweep(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	_, _, pool := startTestPostgres(t, ctx)
	natsURL := startTestNATS(t, ctx)

	// A real bus, not nil: application.deploy's matrix cases need to
	// distinguish "denied by permission" (403) from "allowed" (201) — with a
	// nil bus, an allowed deploy would fail at the post-commit publish step
	// and come back 502 instead, indistinguishable from a real problem. No
	// scheduler/worker is needed alongside it: only the deploy route's own
	// permission check and its publish succeeding are under test here, not
	// placement (same reasoning TestAPIServer_Quotas already documents).
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
	// Same reasoning for the git-deploy mode added in
	// phase-7-deployment-platform.md Task 4: an allowed git deploy publishes
	// build.requested after committing, so without this stream it would come
	// back 502 rather than 202 and its matrix cases would be unreadable. No
	// image-builder is needed alongside it — only the route's permission
	// check and its publish succeeding are under test.
	if err := bus.EnsureStream(ctx, eventbus.StreamConfig{
		Name:     eventbus.BuildsStream,
		Subjects: eventbus.BuildsStreamSubjects,
	}); err != nil {
		t.Fatalf("ensuring %s stream: %v", eventbus.BuildsStream, err)
	}

	issuer, err := auth.NewTokenIssuer("test-signing-key")
	if err != nil {
		t.Fatalf("NewTokenIssuer: %v", err)
	}
	srv := New(pool, issuer, nil, bus, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(srv.Routes())
	t.Cleanup(ts.Close)

	client := &http.Client{Timeout: 30 * time.Second}

	// --- org A: owner (from signup) plus seeded admin/developer/viewer
	// memberships, covering every role the matrix distinguishes — same
	// seeding shape as TestAPIServer_Members/APIKeys/AuditLogs. ---
	signupOwner := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "rbac-sweep-owner@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	tokenOwner := signupOwner["access_token"].(string)
	orgA := signupOwner["org"].(map[string]any)["id"].(string)
	orgAID, err := uuid.Parse(orgA)
	if err != nil {
		t.Fatalf("parsing org A id: %v", err)
	}
	ownerID, err := uuid.Parse(signupOwner["user"].(map[string]any)["id"].(string))
	if err != nil {
		t.Fatalf("parsing owner id: %v", err)
	}

	signupAdmin := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "rbac-sweep-admin@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	tokenAdmin := signupAdmin["access_token"].(string)
	adminID, err := uuid.Parse(signupAdmin["user"].(map[string]any)["id"].(string))
	if err != nil {
		t.Fatalf("parsing admin id: %v", err)
	}

	signupDev := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "rbac-sweep-dev@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	tokenDev := signupDev["access_token"].(string)
	devID, err := uuid.Parse(signupDev["user"].(map[string]any)["id"].(string))
	if err != nil {
		t.Fatalf("parsing developer id: %v", err)
	}

	signupViewer := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "rbac-sweep-viewer@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	tokenViewer := signupViewer["access_token"].(string)
	viewerID, err := uuid.Parse(signupViewer["user"].(map[string]any)["id"].(string))
	if err != nil {
		t.Fatalf("parsing viewer id: %v", err)
	}

	if err := pool.WithTx(ctx, ownerID, orgAID, func(ctx context.Context, conn db.Conn) error {
		memberships := db.NewMembershipRepository(conn)
		if _, err := memberships.Create(ctx, orgAID, adminID, db.MembershipRoleAdmin); err != nil {
			return err
		}
		if _, err := memberships.Create(ctx, orgAID, devID, db.MembershipRoleDeveloper); err != nil {
			return err
		}
		_, err := memberships.Create(ctx, orgAID, viewerID, db.MembershipRoleViewer)
		return err
	}); err != nil {
		t.Fatalf("seeding org A admin/developer/viewer memberships: %v", err)
	}

	// --- org B: unrelated, for cross-tenant denial ---
	signupB := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "rbac-sweep-b@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	tokenB := signupB["access_token"].(string)

	t.Run("project.create", func(t *testing.T) {
		assertStatus(t, ctx, client, http.MethodPost, ts.URL+"/v1/orgs/"+orgA+"/projects", tokenViewer,
			map[string]string{"name": "P-Viewer"}, http.StatusForbidden)
		doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/orgs/"+orgA+"/projects", tokenDev,
			map[string]string{"name": "P-Dev"}, http.StatusCreated)
		doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/orgs/"+orgA+"/projects", tokenAdmin,
			map[string]string{"name": "P-Admin"}, http.StatusCreated)
		doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/orgs/"+orgA+"/projects", tokenOwner,
			map[string]string{"name": "P-Owner"}, http.StatusCreated)
		// cross-tenant: org B isn't a member of org A at all, so the same
		// requirePermission path that denies a wrong role denies a stranger
		// too — but as 404, never 403 (rbac-multitenancy.md §5's
		// don't-leak-existence rule).
		assertStatus(t, ctx, client, http.MethodPost, ts.URL+"/v1/orgs/"+orgA+"/projects", tokenB,
			map[string]string{"name": "P-CrossTenant"}, http.StatusNotFound)
	})

	// A shared project for the application-level matrix checks below —
	// created by the owner, not under test itself (project.create's own
	// matrix was just checked above).
	sweepProject := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/orgs/"+orgA+"/projects", tokenOwner,
		map[string]string{"name": "RBAC Sweep Project"}, http.StatusCreated)
	sweepProjectID := sweepProject["id"].(string)

	t.Run("application.create", func(t *testing.T) {
		assertStatus(t, ctx, client, http.MethodPost, ts.URL+"/v1/projects/"+sweepProjectID+"/applications", tokenViewer,
			map[string]any{"name": "a-viewer", "image": "nginx:latest"}, http.StatusForbidden)
		doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/projects/"+sweepProjectID+"/applications", tokenDev,
			map[string]any{"name": "a-dev", "image": "nginx:latest"}, http.StatusCreated)
		doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/projects/"+sweepProjectID+"/applications", tokenAdmin,
			map[string]any{"name": "a-admin", "image": "nginx:latest"}, http.StatusCreated)
		// cross-tenant: org B targeting org A's project id
		assertStatus(t, ctx, client, http.MethodPost, ts.URL+"/v1/projects/"+sweepProjectID+"/applications", tokenB,
			map[string]any{"name": "a-crosstenant", "image": "nginx:latest"}, http.StatusNotFound)
	})

	// createApp is a small helper local to this test: the owner creates a
	// fresh, uniquely-named application for one role's delete/deploy/scale
	// case to act on, so each sub-test gets an isolated resource rather than
	// racing to mutate a shared one.
	createApp := func(t *testing.T, name string) string {
		t.Helper()
		app := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/projects/"+sweepProjectID+"/applications", tokenOwner,
			map[string]any{"name": name, "image": "nginx:latest"}, http.StatusCreated)
		return app["id"].(string)
	}

	t.Run("application.delete", func(t *testing.T) {
		viewerAppID := createApp(t, "del-viewer")
		assertStatus(t, ctx, client, http.MethodDelete, ts.URL+"/v1/applications/"+viewerAppID, tokenViewer, nil, http.StatusForbidden)

		devAppID := createApp(t, "del-dev")
		assertStatus(t, ctx, client, http.MethodDelete, ts.URL+"/v1/applications/"+devAppID, tokenDev, nil, http.StatusNoContent)

		adminAppID := createApp(t, "del-admin")
		assertStatus(t, ctx, client, http.MethodDelete, ts.URL+"/v1/applications/"+adminAppID, tokenAdmin, nil, http.StatusNoContent)

		crossAppID := createApp(t, "del-crosstenant")
		assertStatus(t, ctx, client, http.MethodDelete, ts.URL+"/v1/applications/"+crossAppID, tokenB, nil, http.StatusNotFound)
	})

	t.Run("application.deploy", func(t *testing.T) {
		viewerAppID := createApp(t, "deploy-viewer")
		assertStatus(t, ctx, client, http.MethodPost, ts.URL+"/v1/applications/"+viewerAppID+"/deployments", tokenViewer, nil, http.StatusForbidden)

		devAppID := createApp(t, "deploy-dev")
		doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/applications/"+devAppID+"/deployments", tokenDev, nil, http.StatusCreated)

		adminAppID := createApp(t, "deploy-admin")
		doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/applications/"+adminAppID+"/deployments", tokenAdmin, nil, http.StatusCreated)

		crossAppID := createApp(t, "deploy-crosstenant")
		assertStatus(t, ctx, client, http.MethodPost, ts.URL+"/v1/applications/"+crossAppID+"/deployments", tokenB, nil, http.StatusNotFound)
	})

	// The git mode of the same route (phase-7-deployment-platform.md Task 4)
	// is gated by the same application.deploy permission —
	// rbac-multitenancy.md §2 doesn't distinguish how a deploy was triggered
	// — but takes a different code path and answers 202 with a build rather
	// than 201 with a deployment, so its matrix is asserted rather than
	// assumed from the image-mode rows above.
	t.Run("application.deploy_git", func(t *testing.T) {
		gitBody := map[string]any{"git_url": "https://example.com/repo.git", "git_ref": "main"}

		viewerAppID := createApp(t, "gitdeploy-viewer")
		assertStatus(t, ctx, client, http.MethodPost, ts.URL+"/v1/applications/"+viewerAppID+"/deployments", tokenViewer, gitBody, http.StatusForbidden)

		devAppID := createApp(t, "gitdeploy-dev")
		doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/applications/"+devAppID+"/deployments", tokenDev, gitBody, http.StatusAccepted)

		adminAppID := createApp(t, "gitdeploy-admin")
		doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/applications/"+adminAppID+"/deployments", tokenAdmin, gitBody, http.StatusAccepted)

		crossAppID := createApp(t, "gitdeploy-crosstenant")
		assertStatus(t, ctx, client, http.MethodPost, ts.URL+"/v1/applications/"+crossAppID+"/deployments", tokenB, gitBody, http.StatusNotFound)
	})

	// GET .../builds takes the same membership-only posture as GET
	// .../deployments (read-only — every role, viewer included), so like
	// logs.view above there is no 403 case for any valid member; what's
	// worth confirming is that a viewer isn't blocked and a stranger gets
	// 404 rather than a leak.
	t.Run("builds.view", func(t *testing.T) {
		buildsApp := createApp(t, "builds-viewer-app")
		doJSON(t, ctx, client, http.MethodGet, ts.URL+"/v1/applications/"+buildsApp+"/builds", tokenViewer, nil, http.StatusOK)
		assertStatus(t, ctx, client, http.MethodGet, ts.URL+"/v1/applications/"+buildsApp+"/builds", tokenB, nil, http.StatusNotFound)
	})

	t.Run("application.scale", func(t *testing.T) {
		viewerAppID := createApp(t, "scale-viewer")
		assertStatus(t, ctx, client, http.MethodPatch, ts.URL+"/v1/applications/"+viewerAppID, tokenViewer,
			map[string]any{"replicas_desired": 2}, http.StatusForbidden)

		devAppID := createApp(t, "scale-dev")
		doJSON(t, ctx, client, http.MethodPatch, ts.URL+"/v1/applications/"+devAppID, tokenDev,
			map[string]any{"replicas_desired": 2}, http.StatusOK)

		adminAppID := createApp(t, "scale-admin")
		doJSON(t, ctx, client, http.MethodPatch, ts.URL+"/v1/applications/"+adminAppID, tokenAdmin,
			map[string]any{"replicas_desired": 2}, http.StatusOK)

		crossAppID := createApp(t, "scale-crosstenant")
		assertStatus(t, ctx, client, http.MethodPatch, ts.URL+"/v1/applications/"+crossAppID, tokenB,
			map[string]any{"replicas_desired": 2}, http.StatusNotFound)
	})

	// domains.manage: TestAPIServer_Domains already covers developer
	// allowed / viewer denied / cross-tenant; the one role that test never
	// exercised is admin, which this closes.
	t.Run("domains.manage_admin_allowed", func(t *testing.T) {
		domainApp := createApp(t, "domains-admin-app")
		doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/projects/"+sweepProjectID+"/domains", tokenAdmin,
			map[string]any{"hostname": "rbac-sweep-admin.example.com", "application_id": domainApp}, http.StatusCreated)
	})

	// logs.view: handleGetLogs deliberately checks requireMembership, not
	// requirePermission (rbac-multitenancy.md §2: every role, including
	// viewer, is allowed) — so there is no 403 case to assert for any valid
	// member. What's worth confirming is that a viewer specifically isn't
	// blocked by permission: with no deployment ever created for this
	// application, the only way to reach the "never been deployed" 404 is
	// for the membership check to have already let the request through.
	t.Run("logs.view_viewer_not_forbidden", func(t *testing.T) {
		logsApp := createApp(t, "logs-viewer-app")
		assertStatus(t, ctx, client, http.MethodGet, ts.URL+"/v1/applications/"+logsApp+"/logs", tokenViewer, nil, http.StatusNotFound)
	})
}

// TestAPIServer_RoleChangeTakesEffectImmediately is rbac-multitenancy.md
// §5's third required item: change a membership's role mid-session (the
// old role's JWT is still unexpired and unrevoked) and confirm the very
// next request re-checks the role from Postgres, not from any claim baked
// into the token — ADR-0008's own reasoning for why the JWT carries no role
// claim at all. requirePermission already always calls
// MembershipRepository.RoleForUser fresh on every request (permission.go),
// so this test is really confirming that architecture holds end to end,
// using project.create (developer allowed, viewer denied) as the concrete
// action whose allow/deny result changes are observed both directions:
// demoting mid-session immediately blocks it, and promoting back mid-
// session immediately un-blocks it — both using the one token minted at
// signup, which is never refreshed or reissued anywhere in this test.
func TestAPIServer_RoleChangeTakesEffectImmediately(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	_, _, pool := startTestPostgres(t, ctx)

	issuer, err := auth.NewTokenIssuer("test-signing-key")
	if err != nil {
		t.Fatalf("NewTokenIssuer: %v", err)
	}
	srv := New(pool, issuer, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(srv.Routes())
	t.Cleanup(ts.Close)

	client := &http.Client{Timeout: 30 * time.Second}

	signupOwner := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "role-change-owner@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	tokenOwner := signupOwner["access_token"].(string)
	orgA := signupOwner["org"].(map[string]any)["id"].(string)
	orgAID, err := uuid.Parse(orgA)
	if err != nil {
		t.Fatalf("parsing org id: %v", err)
	}
	ownerID, err := uuid.Parse(signupOwner["user"].(map[string]any)["id"].(string))
	if err != nil {
		t.Fatalf("parsing owner id: %v", err)
	}

	// The subject whose role changes mid-session. Signed up independently
	// (so it has its own real JWT from its own login, not a manufactured
	// one), then added to org A as a developer.
	signupSubject := doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/auth/signup", "",
		map[string]string{"email": "role-change-subject@example.com", "password": "hunter22-hunter22"}, http.StatusCreated)
	// This is the one token used for every request below — never reissued,
	// refreshed, or re-logged-in, so any change in what it's allowed to do
	// can only be explained by the server re-checking Postgres each time.
	subjectToken := signupSubject["access_token"].(string)
	subjectID, err := uuid.Parse(signupSubject["user"].(map[string]any)["id"].(string))
	if err != nil {
		t.Fatalf("parsing subject id: %v", err)
	}

	if err := pool.WithTx(ctx, ownerID, orgAID, func(ctx context.Context, conn db.Conn) error {
		_, err := db.NewMembershipRepository(conn).Create(ctx, orgAID, subjectID, db.MembershipRoleDeveloper)
		return err
	}); err != nil {
		t.Fatalf("seeding subject as a developer: %v", err)
	}

	// --- as a developer, project.create succeeds ---
	doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/orgs/"+orgA+"/projects", subjectToken,
		map[string]string{"name": "Before Demotion"}, http.StatusCreated)

	// --- owner demotes the subject to viewer, mid-session — no new token
	// is issued to the subject, nothing on their end changes at all ---
	demoted := doJSON(t, ctx, client, http.MethodPatch, ts.URL+"/v1/orgs/"+orgA+"/members/"+subjectID.String(), tokenOwner,
		map[string]string{"role": "viewer"}, http.StatusOK)
	if demoted["role"] != "viewer" {
		t.Fatalf("demoted role = %v, want viewer", demoted["role"])
	}

	// --- the very next request, using the exact same still-valid JWT,
	// is now denied — proving the check re-read Postgres, not stale claims
	// (the JWT itself carries no role claim to have gone stale in the first
	// place, ADR-0008) ---
	assertStatus(t, ctx, client, http.MethodPost, ts.URL+"/v1/orgs/"+orgA+"/projects", subjectToken,
		map[string]string{"name": "After Demotion"}, http.StatusForbidden)

	// --- owner promotes the subject back to developer, mid-session ---
	promoted := doJSON(t, ctx, client, http.MethodPatch, ts.URL+"/v1/orgs/"+orgA+"/members/"+subjectID.String(), tokenOwner,
		map[string]string{"role": "developer"}, http.StatusOK)
	if promoted["role"] != "developer" {
		t.Fatalf("promoted role = %v, want developer", promoted["role"])
	}

	// --- the very next request, same token throughout, succeeds again —
	// the upgrade takes effect immediately too, not just the downgrade ---
	doJSON(t, ctx, client, http.MethodPost, ts.URL+"/v1/orgs/"+orgA+"/projects", subjectToken,
		map[string]string{"name": "After Repromotion"}, http.StatusCreated)
}

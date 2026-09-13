package auth

// Role is a membership's role within one organization
// (docs/rbac-multitenancy.md §1). Checked server-side against the caller's
// `memberships.role` row for the target org — never trusted from a client
// claim (rbac-multitenancy.md §2).
type Role string

const (
	RoleOwner     Role = "owner"
	RoleAdmin     Role = "admin"
	RoleDeveloper Role = "developer"
	RoleViewer    Role = "viewer"
)

// Permission is one row of the matrix in docs/rbac-multitenancy.md §2. As of
// Phase 6 Task 2, every row of that table has a constant here — including
// several with no gating route yet (env_vars.write, billing.view,
// metrics.view, deployment.rollback, api_keys.create/revoke ahead of Task 5)
// — kept so the matrix and its unit test (rbac_test.go) stay the complete,
// literal transcription rbac-multitenancy.md §2 says it should be, per
// docs/phases/phase-6-multi-tenant-saas.md Task 2. A constant with no route
// is inert until a handler calls requirePermission with it.
type Permission string

const (
	PermOrganizationManage Permission = "organization.manage"
	PermOrganizationDelete Permission = "organization.delete"
	PermMembersInvite      Permission = "members.invite"
	PermMembersRemove      Permission = "members.remove"
	PermMembersChangeRole  Permission = "members.change_role"
	PermProjectCreate      Permission = "project.create"
	PermProjectDelete      Permission = "project.delete"
	PermApplicationCreate  Permission = "application.create"
	PermApplicationDelete  Permission = "application.delete"
	PermApplicationDeploy  Permission = "application.deploy"
	// PermApplicationScale is deliberately its own constant rather than a
	// reuse of PermApplicationDeploy, even though they share an allow-set
	// today (phase-6-multi-tenant-saas.md open decision 1) — the matrix
	// lists them as two rows so a future role split doesn't require a
	// handler change.
	PermApplicationScale   Permission = "application.scale"
	PermDeploymentRollback Permission = "deployment.rollback"
	PermEnvVarsWrite       Permission = "env_vars.write"
	PermDomainsManage      Permission = "domains.manage"
	PermLogsView           Permission = "logs.view"
	PermMetricsView        Permission = "metrics.view"
	PermAuditLogsView      Permission = "audit_logs.view"
	PermAPIKeysCreate      Permission = "api_keys.create"
	PermAPIKeysRevoke      Permission = "api_keys.revoke"
	PermBillingView        Permission = "billing.view"
)

// permissionMatrix mirrors docs/rbac-multitenancy.md §2 exactly — that
// table is this matrix's spec and its test data (rbac_test.go).
var permissionMatrix = map[Permission]map[Role]bool{
	PermOrganizationManage: {RoleOwner: true, RoleAdmin: true, RoleDeveloper: false, RoleViewer: false},
	PermOrganizationDelete: {RoleOwner: true, RoleAdmin: false, RoleDeveloper: false, RoleViewer: false},
	PermMembersInvite:      {RoleOwner: true, RoleAdmin: true, RoleDeveloper: false, RoleViewer: false},
	PermMembersRemove:      {RoleOwner: true, RoleAdmin: true, RoleDeveloper: false, RoleViewer: false},
	PermMembersChangeRole:  {RoleOwner: true, RoleAdmin: true, RoleDeveloper: false, RoleViewer: false},
	PermProjectCreate:      {RoleOwner: true, RoleAdmin: true, RoleDeveloper: true, RoleViewer: false},
	PermProjectDelete:      {RoleOwner: true, RoleAdmin: true, RoleDeveloper: true, RoleViewer: false},
	PermApplicationCreate:  {RoleOwner: true, RoleAdmin: true, RoleDeveloper: true, RoleViewer: false},
	PermApplicationDelete:  {RoleOwner: true, RoleAdmin: true, RoleDeveloper: true, RoleViewer: false},
	PermApplicationDeploy:  {RoleOwner: true, RoleAdmin: true, RoleDeveloper: true, RoleViewer: false},
	PermApplicationScale:   {RoleOwner: true, RoleAdmin: true, RoleDeveloper: true, RoleViewer: false},
	PermDeploymentRollback: {RoleOwner: true, RoleAdmin: true, RoleDeveloper: true, RoleViewer: false},
	PermEnvVarsWrite:       {RoleOwner: true, RoleAdmin: true, RoleDeveloper: true, RoleViewer: false},
	PermDomainsManage:      {RoleOwner: true, RoleAdmin: true, RoleDeveloper: true, RoleViewer: false},
	PermLogsView:           {RoleOwner: true, RoleAdmin: true, RoleDeveloper: true, RoleViewer: true},
	PermMetricsView:        {RoleOwner: true, RoleAdmin: true, RoleDeveloper: true, RoleViewer: true},
	PermAuditLogsView:      {RoleOwner: true, RoleAdmin: true, RoleDeveloper: false, RoleViewer: false},
	PermAPIKeysCreate:      {RoleOwner: true, RoleAdmin: true, RoleDeveloper: false, RoleViewer: false},
	PermAPIKeysRevoke:      {RoleOwner: true, RoleAdmin: true, RoleDeveloper: false, RoleViewer: false},
	PermBillingView:        {RoleOwner: true, RoleAdmin: true, RoleDeveloper: false, RoleViewer: false},
}

// HasPermission reports whether role grants perm, per the matrix in
// docs/rbac-multitenancy.md §2. An unknown role (e.g. a stale/corrupt
// membership row) is always denied — deny-by-default, never fail open.
func HasPermission(role Role, perm Permission) bool {
	return permissionMatrix[perm][role]
}

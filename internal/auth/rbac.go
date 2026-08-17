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

// Permission is one row of the matrix in docs/rbac-multitenancy.md §2. Only
// the permissions Phase 1's routes actually check are defined here — add
// more as routes that need them land, not speculatively ahead of them.
type Permission string

const (
	PermProjectCreate     Permission = "project.create"
	PermProjectDelete     Permission = "project.delete"
	PermApplicationCreate Permission = "application.create"
	PermApplicationDelete Permission = "application.delete"
	PermApplicationDeploy Permission = "application.deploy"
	PermLogsView          Permission = "logs.view"
)

// permissionMatrix mirrors docs/rbac-multitenancy.md §2 exactly — that
// table is this matrix's spec and its test data (rbac_test.go).
var permissionMatrix = map[Permission]map[Role]bool{
	PermProjectCreate:     {RoleOwner: true, RoleAdmin: true, RoleDeveloper: true, RoleViewer: false},
	PermProjectDelete:     {RoleOwner: true, RoleAdmin: true, RoleDeveloper: true, RoleViewer: false},
	PermApplicationCreate: {RoleOwner: true, RoleAdmin: true, RoleDeveloper: true, RoleViewer: false},
	PermApplicationDelete: {RoleOwner: true, RoleAdmin: true, RoleDeveloper: true, RoleViewer: false},
	PermApplicationDeploy: {RoleOwner: true, RoleAdmin: true, RoleDeveloper: true, RoleViewer: false},
	PermLogsView:          {RoleOwner: true, RoleAdmin: true, RoleDeveloper: true, RoleViewer: true},
}

// HasPermission reports whether role grants perm, per the matrix in
// docs/rbac-multitenancy.md §2. An unknown role (e.g. a stale/corrupt
// membership row) is always denied — deny-by-default, never fail open.
func HasPermission(role Role, perm Permission) bool {
	return permissionMatrix[perm][role]
}

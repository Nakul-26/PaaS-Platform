package auth

import "testing"

// TestHasPermission_Matrix is the required permission-matrix test
// (docs/rbac-multitenancy.md §5): for every permission this package knows
// about, assert the exact allow/deny for every role against the published
// matrix (rbac-multitenancy.md §2).
func TestHasPermission_Matrix(t *testing.T) {
	tests := []struct {
		perm    Permission
		allowed map[Role]bool
	}{
		{
			perm: PermOrganizationManage,
			allowed: map[Role]bool{
				RoleOwner: true, RoleAdmin: true, RoleDeveloper: false, RoleViewer: false,
			},
		},
		{
			perm: PermOrganizationDelete,
			allowed: map[Role]bool{
				RoleOwner: true, RoleAdmin: false, RoleDeveloper: false, RoleViewer: false,
			},
		},
		{
			perm: PermMembersInvite,
			allowed: map[Role]bool{
				RoleOwner: true, RoleAdmin: true, RoleDeveloper: false, RoleViewer: false,
			},
		},
		{
			perm: PermMembersRemove,
			allowed: map[Role]bool{
				RoleOwner: true, RoleAdmin: true, RoleDeveloper: false, RoleViewer: false,
			},
		},
		{
			perm: PermMembersChangeRole,
			allowed: map[Role]bool{
				RoleOwner: true, RoleAdmin: true, RoleDeveloper: false, RoleViewer: false,
			},
		},
		{
			perm: PermProjectCreate,
			allowed: map[Role]bool{
				RoleOwner: true, RoleAdmin: true, RoleDeveloper: true, RoleViewer: false,
			},
		},
		{
			perm: PermProjectDelete,
			allowed: map[Role]bool{
				RoleOwner: true, RoleAdmin: true, RoleDeveloper: true, RoleViewer: false,
			},
		},
		{
			perm: PermApplicationCreate,
			allowed: map[Role]bool{
				RoleOwner: true, RoleAdmin: true, RoleDeveloper: true, RoleViewer: false,
			},
		},
		{
			perm: PermApplicationDelete,
			allowed: map[Role]bool{
				RoleOwner: true, RoleAdmin: true, RoleDeveloper: true, RoleViewer: false,
			},
		},
		{
			perm: PermApplicationDeploy,
			allowed: map[Role]bool{
				RoleOwner: true, RoleAdmin: true, RoleDeveloper: true, RoleViewer: false,
			},
		},
		{
			perm: PermApplicationScale,
			allowed: map[Role]bool{
				RoleOwner: true, RoleAdmin: true, RoleDeveloper: true, RoleViewer: false,
			},
		},
		{
			perm: PermDeploymentRollback,
			allowed: map[Role]bool{
				RoleOwner: true, RoleAdmin: true, RoleDeveloper: true, RoleViewer: false,
			},
		},
		{
			perm: PermEnvVarsWrite,
			allowed: map[Role]bool{
				RoleOwner: true, RoleAdmin: true, RoleDeveloper: true, RoleViewer: false,
			},
		},
		{
			perm: PermDomainsManage,
			allowed: map[Role]bool{
				RoleOwner: true, RoleAdmin: true, RoleDeveloper: true, RoleViewer: false,
			},
		},
		{
			perm: PermLogsView,
			allowed: map[Role]bool{
				RoleOwner: true, RoleAdmin: true, RoleDeveloper: true, RoleViewer: true,
			},
		},
		{
			perm: PermMetricsView,
			allowed: map[Role]bool{
				RoleOwner: true, RoleAdmin: true, RoleDeveloper: true, RoleViewer: true,
			},
		},
		{
			perm: PermAuditLogsView,
			allowed: map[Role]bool{
				RoleOwner: true, RoleAdmin: true, RoleDeveloper: false, RoleViewer: false,
			},
		},
		{
			perm: PermAPIKeysCreate,
			allowed: map[Role]bool{
				RoleOwner: true, RoleAdmin: true, RoleDeveloper: false, RoleViewer: false,
			},
		},
		{
			perm: PermAPIKeysRevoke,
			allowed: map[Role]bool{
				RoleOwner: true, RoleAdmin: true, RoleDeveloper: false, RoleViewer: false,
			},
		},
		{
			perm: PermBillingView,
			allowed: map[Role]bool{
				RoleOwner: true, RoleAdmin: true, RoleDeveloper: false, RoleViewer: false,
			},
		},
	}

	for _, tt := range tests {
		for role, want := range tt.allowed {
			t.Run(string(tt.perm)+"/"+string(role), func(t *testing.T) {
				if got := HasPermission(role, tt.perm); got != want {
					t.Errorf("HasPermission(%s, %s) = %v, want %v", role, tt.perm, got, want)
				}
			})
		}
	}
}

func TestHasPermission_UnknownRoleDenied(t *testing.T) {
	if HasPermission(Role("bogus"), PermLogsView) {
		t.Fatal("an unrecognized role must never be granted a permission")
	}
}

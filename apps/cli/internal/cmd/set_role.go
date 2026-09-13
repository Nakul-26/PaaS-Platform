package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// newSetRoleCmd is `platform set-role <email> <role>`
// (phase-6-multi-tenant-saas.md Task 4) — a top-level command, matching the
// phase doc's own invocation shape.
func newSetRoleCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "set-role <email> <role>",
		Short: "Change a member's role in the current organization",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, client, err := requireLogin()
			if err != nil {
				return err
			}
			email, role := args[0], args[1]
			userID, err := resolveMemberUserID(cmd, cfg, client, email)
			if err != nil {
				return err
			}
			m, err := client.ChangeMemberRole(cmd.Context(), cfg.OrgID, userID, role)
			if err != nil {
				return err
			}
			fmt.Printf("Set %s's role to %s in %s\n", email, m.Role, cfg.OrgSlug)
			return nil
		},
	}
	return c
}

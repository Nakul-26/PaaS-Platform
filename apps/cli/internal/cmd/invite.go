package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// newInviteCmd is `platform invite <email> --role <role>`
// (phase-6-multi-tenant-saas.md Task 4) — a top-level command (not nested
// under create/get), matching the phase doc's own invocation shape. Only
// works for an already-registered user (open decision 3: no email-sending
// infrastructure exists anywhere in this codebase) — the server returns a
// clear error if the email has no account yet.
func newInviteCmd() *cobra.Command {
	var role string
	c := &cobra.Command{
		Use:   "invite <email>",
		Short: "Add an already-registered user to the current organization",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, client, err := requireLogin()
			if err != nil {
				return err
			}
			if role == "" {
				return fmt.Errorf("--role is required")
			}
			email := args[0]
			m, err := client.InviteMember(cmd.Context(), cfg.OrgID, email, role)
			if err != nil {
				return err
			}
			fmt.Printf("Added %s to %s as %s\n", email, cfg.OrgSlug, m.Role)
			return nil
		},
	}
	c.Flags().StringVar(&role, "role", "", "role to grant: owner, admin, developer, or viewer (required)")
	return c
}

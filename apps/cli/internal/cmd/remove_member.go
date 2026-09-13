package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"platform/apps/cli/internal/apiclient"
	"platform/apps/cli/internal/config"
)

// newRemoveMemberCmd is `platform remove-member <email>`
// (phase-6-multi-tenant-saas.md Task 4) — a top-level command, matching the
// phase doc's own invocation shape.
func newRemoveMemberCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "remove-member <email>",
		Short: "Remove a member from the current organization",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, client, err := requireLogin()
			if err != nil {
				return err
			}
			userID, err := resolveMemberUserID(cmd, cfg, client, args[0])
			if err != nil {
				return err
			}
			if err := client.RemoveMember(cmd.Context(), cfg.OrgID, userID); err != nil {
				return err
			}
			fmt.Printf("Removed %s from %s\n", args[0], cfg.OrgSlug)
			return nil
		},
	}
	return c
}

// resolveMemberUserID looks up email's user_id via the members list —
// there's no local email -> ID cache (unlike cfg.Applications for app
// names), same lightweight-resource precedent as delete_domain.go's
// hostname matching. Shared by newRemoveMemberCmd and newSetRoleCmd, both
// of which take an email but the server routes by user_id.
func resolveMemberUserID(cmd *cobra.Command, cfg *config.Config, client *apiclient.Client, email string) (string, error) {
	page, err := client.ListMembers(cmd.Context(), cfg.OrgID)
	if err != nil {
		return "", err
	}
	for _, m := range page.Data {
		if m.Email == email {
			return m.UserID, nil
		}
	}
	return "", fmt.Errorf("no known member %q in organization %q", email, cfg.OrgSlug)
}

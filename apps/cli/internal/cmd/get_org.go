package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// newGetOrgCmd is `platform get org` (phase-6-multi-tenant-saas.md Task 3).
// Unlike get_deployments.go/get_domains.go, this takes no argument and
// isn't project-scoped — the CLI already tracks exactly one current org per
// session (cfg.OrgID, set at signup/login), so there's no "which org" to
// select, only "confirm the one I'm already in."
func newGetOrgCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "org",
		Short: "Show the current organization",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, client, err := requireLogin()
			if err != nil {
				return err
			}
			org, err := client.GetOrganization(cmd.Context(), cfg.OrgID)
			if err != nil {
				return err
			}
			fmt.Printf("ID:         %s\nName:       %s\nSlug:       %s\nCreated At: %s\n",
				org.ID, org.Name, org.Slug, org.CreatedAt.Format("2006-01-02T15:04:05Z"))
			return nil
		},
	}
	return c
}

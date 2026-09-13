package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// newUpdateCmd is the `platform update ...` parent command
// (phase-6-multi-tenant-saas.md Task 3) — the first command tree to need a
// generic "update" verb; every prior phase's mutations were either creates,
// deletes, or a dedicated verb of their own (scale).
func newUpdateCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "update",
		Short: "Update a resource",
	}
	c.AddCommand(newUpdateOrgCmd())
	return c
}

// newUpdateOrgCmd is `platform update org --name <name>`, calling
// PATCH /v1/orgs/:orgId. Like newGetOrgCmd, it acts on the CLI session's
// single current org (cfg.OrgID) — no org-selection concept to add.
func newUpdateOrgCmd() *cobra.Command {
	var name string
	c := &cobra.Command{
		Use:   "org",
		Short: "Update the current organization's settings",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, client, err := requireLogin()
			if err != nil {
				return err
			}
			if name == "" {
				return fmt.Errorf("--name is required")
			}
			org, err := client.UpdateOrganization(cmd.Context(), cfg.OrgID, name)
			if err != nil {
				return err
			}
			fmt.Printf("Updated organization %s -> name %q\n", org.ID, org.Name)
			return nil
		},
	}
	c.Flags().StringVar(&name, "name", "", "new organization name (required)")
	return c
}

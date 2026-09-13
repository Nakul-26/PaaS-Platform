package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// newDeleteAPIKeyCmd is `platform delete api-key <name>`
// (phase-6-multi-tenant-saas.md Task 5), a subcommand of the existing
// `platform delete <app-name>` (delete.go), same precedent as
// delete_domain.go: cobra only dispatches here when the first positional
// arg is literally "api-key". No local name -> ID cache (same reasoning as
// delete_domain.go's hostname matching) — lists the current org's keys and
// matches by name client-side.
func newDeleteAPIKeyCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "api-key <name>",
		Short: "Revoke an API key",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, client, err := requireLogin()
			if err != nil {
				return err
			}
			name := args[0]

			page, err := client.ListAPIKeys(cmd.Context(), cfg.OrgID)
			if err != nil {
				return err
			}
			var keyID string
			for _, k := range page.Data {
				if k.Name == name {
					keyID = k.ID
					break
				}
			}
			if keyID == "" {
				return fmt.Errorf("no known API key %q in organization %q", name, cfg.OrgSlug)
			}

			if err := client.DeleteAPIKey(cmd.Context(), keyID); err != nil {
				return err
			}
			fmt.Printf("Revoked API key %s\n", name)
			return nil
		},
	}
	return c
}

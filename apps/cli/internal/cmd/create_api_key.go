package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// newCreateAPIKeyCmd is `platform create api-key <name>`
// (phase-6-multi-tenant-saas.md Task 5). The plaintext key is printed
// exactly once here — the server never returns it again on any later call
// (GetAPIKeys/ListAPIKeys only ever return the metadata), so this prints an
// explicit "save this now" warning rather than assuming the user will
// notice.
func newCreateAPIKeyCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "api-key <name>",
		Short: "Create an API key for the current organization",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, client, err := requireLogin()
			if err != nil {
				return err
			}
			key, err := client.CreateAPIKey(cmd.Context(), cfg.OrgID, args[0], nil)
			if err != nil {
				return err
			}
			fmt.Printf("Created API key %s (%s)\n", key.Name, key.ID)
			fmt.Printf("Key: %s\n", key.Key)
			fmt.Println("Save this now — it will not be shown again.")
			return nil
		},
	}
	return c
}

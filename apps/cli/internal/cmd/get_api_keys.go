package cmd

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

// newGetAPIKeysCmd is `platform get api-keys` (phase-6-multi-tenant-saas.md
// Task 5) — no argument, same "current org" precedent as get_org.go. Only
// an owner/admin session can call this successfully (api_keys.create gates
// the underlying route). Never shows a plaintext key — only
// `create api-key` ever prints one, at creation.
func newGetAPIKeysCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "api-keys",
		Short: "List API keys for the current organization",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, client, err := requireLogin()
			if err != nil {
				return err
			}
			page, err := client.ListAPIKeys(cmd.Context(), cfg.OrgID)
			if err != nil {
				return err
			}
			if len(page.Data) == 0 {
				fmt.Println("No API keys.")
				return nil
			}
			tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
			_, _ = fmt.Fprintln(tw, "NAME\tID\tSTATUS\tCREATED_AT")
			for _, k := range page.Data {
				status := "active"
				if k.RevokedAt != nil {
					status = "revoked"
				}
				_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", k.Name, k.ID, status, k.CreatedAt.Format("2006-01-02T15:04:05Z"))
			}
			return tw.Flush()
		},
	}
	return c
}

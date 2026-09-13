package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// newDeleteDomainCmd is `platform delete domain <hostname>`
// (phase-5-networking-ingress.md Task 3), added as a subcommand of the
// existing `platform delete <app-name>` (delete.go) rather than replacing
// it: cobra only dispatches to a subcommand when the first positional arg
// matches its name, so `platform delete demo` still falls through to
// delete.go's own RunE exactly as it did before Phase 5 — the literal
// invocation Phase 1's exit-criteria script (ARCHITECTURE.md,
// e2e/phase1_test.go onward) depends on. There's no local hostname -> ID
// cache (unlike cfg.Applications for app names — domains are looked up
// fresh, they're a lighter-weight resource than applications), so this
// lists the current project's domains and matches by hostname client-side.
func newDeleteDomainCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "domain <hostname>",
		Short: "Delete a registered domain",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, client, err := requireProject()
			if err != nil {
				return err
			}
			hostname := args[0]

			page, err := client.ListDomains(cmd.Context(), cfg.ProjectID)
			if err != nil {
				return err
			}
			var domainID string
			for _, d := range page.Data {
				if d.Hostname == hostname {
					domainID = d.ID
					break
				}
			}
			if domainID == "" {
				return fmt.Errorf("no known domain %q in project %q", hostname, cfg.ProjectName)
			}

			if err := client.DeleteDomain(cmd.Context(), domainID); err != nil {
				return err
			}
			fmt.Printf("Deleted domain %s\n", hostname)
			return nil
		},
	}
	return c
}

package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// newDeleteCmd is `platform delete <app-name>` — kept as the flat, no-
// subcommand-required form Phase 1 shipped (ARCHITECTURE.md's exit-
// criteria script, e2e/phase1_test.go onward, all invoke it exactly this
// way). Phase 5 Task 3 added `delete domain <hostname>` as a child command
// rather than restructuring this one: cobra dispatches to a subcommand
// only when args[0] matches its name, so `platform delete demo` still
// falls through unchanged to this RunE as long as no application is ever
// named "domain".
func newDeleteCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "delete <app-name>",
		Short: "Delete an application, stopping and removing its running container",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, client, err := requireProject()
			if err != nil {
				return err
			}
			name := args[0]
			appID, ok := cfg.Applications[name]
			if !ok {
				return fmt.Errorf("no known application %q in project %q", name, cfg.ProjectName)
			}
			if err := client.DeleteApplication(cmd.Context(), appID); err != nil {
				return err
			}
			delete(cfg.Applications, name)
			if err := cfg.Save(); err != nil {
				return fmt.Errorf("saving session: %w", err)
			}
			fmt.Printf("Deleted application %s\n", name)
			return nil
		},
	}
	c.AddCommand(newDeleteDomainCmd())
	c.AddCommand(newDeleteAPIKeyCmd())
	return c
}

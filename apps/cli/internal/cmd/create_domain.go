package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// newCreateDomainCmd is `platform create domain <hostname> --app
// <app-name>` (phase-5-networking-ingress.md Task 3) — resolves --app to
// an ID via cfg.Applications the same way deploy/scale already do, so it
// only ever works for an application already deployed at least once in the
// current project.
func newCreateDomainCmd() *cobra.Command {
	var appName string
	c := &cobra.Command{
		Use:   "domain <hostname>",
		Short: "Register a hostname, routing it to an application in the current project",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, client, err := requireProject()
			if err != nil {
				return err
			}
			if appName == "" {
				return fmt.Errorf("--app is required")
			}
			appID, ok := cfg.Applications[appName]
			if !ok {
				return fmt.Errorf("no known application %q in project %q — deploy it first", appName, cfg.ProjectName)
			}

			hostname := args[0]
			domain, err := client.CreateDomain(cmd.Context(), cfg.ProjectID, hostname, appID)
			if err != nil {
				return err
			}
			fmt.Printf("Registered domain %s (%s) -> %s\n", domain.Hostname, domain.ID, appName)
			return nil
		},
	}
	c.Flags().StringVar(&appName, "app", "", "application (in the current project) to route this hostname to (required)")
	return c
}

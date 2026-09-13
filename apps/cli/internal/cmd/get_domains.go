package cmd

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

// newGetDomainsCmd is `platform get domains` (phase-5-networking-ingress.md
// Task 3), listing every domain registered under the current project.
// Unlike get_deployments.go's per-application listing, this takes no
// argument — domains are listed project-wide, matching
// GET /v1/projects/:projectId/domains.
func newGetDomainsCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "domains",
		Short: "List domains registered in the current project",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, client, err := requireProject()
			if err != nil {
				return err
			}

			page, err := client.ListDomains(cmd.Context(), cfg.ProjectID)
			if err != nil {
				return err
			}
			if len(page.Data) == 0 {
				fmt.Println("No domains.")
				return nil
			}
			// cfg.Applications maps name -> ID; invert it so the table can
			// show the application name a caller actually typed, falling
			// back to the raw ID for an application this CLI session never
			// resolved a name for (e.g. created by someone else).
			appNames := make(map[string]string, len(cfg.Applications))
			for name, id := range cfg.Applications {
				appNames[id] = name
			}

			tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
			_, _ = fmt.Fprintln(tw, "HOSTNAME\tAPPLICATION\tTLS_STATUS\tCREATED_AT")
			for _, d := range page.Data {
				app := d.ApplicationID
				if name, ok := appNames[d.ApplicationID]; ok {
					app = name
				}
				_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", d.Hostname, app, d.TLSStatus, d.CreatedAt.Format("2006-01-02T15:04:05Z"))
			}
			return tw.Flush()
		},
	}
	return c
}

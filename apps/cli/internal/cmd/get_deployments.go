package cmd

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

// newGetCmd is the `platform get ...` parent command, mirroring the
// exit-criteria script's `platform get deployments` invocation shape.
func newGetCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "get",
		Short: "Get a resource",
	}
	c.AddCommand(newGetDeploymentsCmd())
	c.AddCommand(newGetNodesCmd())
	return c
}

func newGetDeploymentsCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "deployments <app-name>",
		Short: "List deployments for an application",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, client, err := requireProject()
			if err != nil {
				return err
			}
			appID, ok := cfg.Applications[args[0]]
			if !ok {
				return fmt.Errorf("no known application %q in project %q — deploy it first", args[0], cfg.ProjectName)
			}

			page, err := client.ListDeployments(cmd.Context(), appID)
			if err != nil {
				return err
			}
			if len(page.Data) == 0 {
				fmt.Println("No deployments.")
				return nil
			}
			tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
			_, _ = fmt.Fprintln(tw, "REVISION\tSTATUS\tIMAGE\tNODE\tCONTAINER_STATUS\tCREATED_AT")
			for _, d := range page.Data {
				node := "-"
				if d.NodeID != nil {
					node = *d.NodeID
				}
				containerStatus := "-"
				if d.ContainerStatus != nil {
					containerStatus = *d.ContainerStatus
				}
				_, _ = fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\t%s\n", d.Revision, d.Status, d.Image, node, containerStatus, d.CreatedAt.Format("2006-01-02T15:04:05Z"))
			}
			return tw.Flush()
		},
	}
	return c
}

package cmd

import (
	"fmt"
	"os"
	"strings"
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
			_, _ = fmt.Fprintln(tw, "REVISION\tSTATUS\tIMAGE\tREPLICAS\tNODES\tCREATED_AT")
			for _, d := range page.Data {
				nodes := "-"
				if len(d.Containers) > 0 {
					ids := make([]string, len(d.Containers))
					for i, c := range d.Containers {
						ids[i] = c.NodeID
					}
					nodes = strings.Join(ids, ",")
				}
				replicas := fmt.Sprintf("%d/%d", d.ReplicasRunning, d.ReplicasDesired)
				_, _ = fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\t%s\n", d.Revision, d.Status, d.Image, replicas, nodes, d.CreatedAt.Format("2006-01-02T15:04:05Z"))
			}
			return tw.Flush()
		},
	}
	return c
}

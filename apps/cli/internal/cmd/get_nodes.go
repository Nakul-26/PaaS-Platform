package cmd

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

// newGetNodesCmd is `platform get nodes` (phase-2-multi-node.md Task 7),
// listing every registered worker node. Unlike `get deployments`, this
// isn't project-scoped — GET /v1/nodes is gated server-side by
// requirePlatformAdmin instead, so this only needs a logged-in session.
func newGetNodesCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "nodes",
		Short: "List registered worker nodes",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			_, client, err := requireLogin()
			if err != nil {
				return err
			}

			page, err := client.ListNodes(cmd.Context())
			if err != nil {
				return err
			}
			if len(page.Data) == 0 {
				fmt.Println("No nodes.")
				return nil
			}
			tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
			_, _ = fmt.Fprintln(tw, "ID\tHOSTNAME\tSTATUS\tLAST_HEARTBEAT_AT")
			for _, n := range page.Data {
				_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", n.ID, n.Hostname, n.Status, n.LastHeartbeatAt.Format("2006-01-02T15:04:05Z"))
			}
			return tw.Flush()
		},
	}
	return c
}

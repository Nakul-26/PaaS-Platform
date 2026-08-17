package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func newLogsCmd() *cobra.Command {
	var follow bool
	c := &cobra.Command{
		Use:   "logs <app-name>",
		Short: "Show logs for an application's current deployment",
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
			return client.StreamLogs(cmd.Context(), appID, follow, os.Stdout)
		},
	}
	c.Flags().BoolVarP(&follow, "follow", "f", false, "keep streaming new log lines as they arrive")
	return c
}

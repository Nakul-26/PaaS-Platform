package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newScaleCmd() *cobra.Command {
	var replicas int
	c := &cobra.Command{
		Use:   "scale <app-name>",
		Short: "Change an application's desired replica count",
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

			app, err := client.ScaleApplication(cmd.Context(), appID, replicas)
			if err != nil {
				return err
			}
			fmt.Printf("Application %s scaled to %d replicas (desired)\n", name, app.ReplicasDesired)
			return nil
		},
	}
	c.Flags().IntVar(&replicas, "replicas", -1, "desired replica count (required)")
	_ = c.MarkFlagRequired("replicas")
	return c
}

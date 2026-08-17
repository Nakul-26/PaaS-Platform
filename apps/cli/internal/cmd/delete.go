package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

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
	return c
}

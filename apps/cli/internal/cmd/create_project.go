package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// newCreateCmd is the `platform create ...` parent command, mirroring the
// exit-criteria script's `platform create project demo` invocation shape
// (phase-1-mvp.md).
func newCreateCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "create",
		Short: "Create a resource",
	}
	c.AddCommand(newCreateProjectCmd())
	return c
}

func newCreateProjectCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "project <name>",
		Short: "Create a project and make it the current project",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, client, err := requireLogin()
			if err != nil {
				return err
			}
			project, err := client.CreateProject(cmd.Context(), cfg.OrgID, args[0])
			if err != nil {
				return err
			}
			cfg.SetProject(project.ID, project.Name)
			if err := cfg.Save(); err != nil {
				return fmt.Errorf("saving session: %w", err)
			}
			fmt.Printf("Created project %s (%s), set as current project\n", project.Name, project.ID)
			return nil
		},
	}
	return c
}

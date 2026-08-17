package cmd

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"platform/apps/cli/internal/apiclient"
	"platform/apps/cli/internal/config"
)

func newDeployCmd() *cobra.Command {
	var image string
	var ports []string
	c := &cobra.Command{
		Use:   "deploy <app-name>",
		Short: "Deploy an application under the current project, creating it first if it doesn't exist yet",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, client, err := requireProject()
			if err != nil {
				return err
			}
			name := args[0]

			portSpecs, err := parsePortFlags(ports)
			if err != nil {
				return err
			}

			appID, err := resolveApplication(cmd.Context(), cfg, client, name, image, portSpecs)
			if err != nil {
				return err
			}

			deployment, err := client.Deploy(cmd.Context(), appID, image)
			if err != nil {
				return err
			}
			fmt.Printf("Deployment %s: revision %d, status %s\n", deployment.ID, deployment.Revision, deployment.Status)
			return nil
		},
	}
	c.Flags().StringVar(&image, "image", "", "container image to deploy (required the first time an application is created)")
	c.Flags().StringArrayVar(&ports, "port", nil, "container port to publish, as containerPort or containerPort:hostPort (0 or omitted host port picks an ephemeral one); only takes effect when the application is first created")
	return c
}

// parsePortFlags turns repeated --port containerPort[:hostPort] flags into
// the create-application request's port specs.
func parsePortFlags(raw []string) ([]apiclient.PortSpec, error) {
	specs := make([]apiclient.PortSpec, 0, len(raw))
	for _, r := range raw {
		containerPart, hostPart, hasHost := strings.Cut(r, ":")
		containerPort, err := strconv.Atoi(containerPart)
		if err != nil {
			return nil, fmt.Errorf("invalid --port %q: container port must be a number", r)
		}
		spec := apiclient.PortSpec{ContainerPort: containerPort, Protocol: "tcp"}
		if hasHost && hostPart != "" {
			hostPort, err := strconv.Atoi(hostPart)
			if err != nil {
				return nil, fmt.Errorf("invalid --port %q: host port must be a number", r)
			}
			spec.HostPort = hostPort
		}
		specs = append(specs, spec)
	}
	return specs, nil
}

// resolveApplication returns the ID for an application name within the
// current project, creating the application server-side the first time
// it's deployed — the CLI never invents an ID itself, it only remembers
// the one the server assigns.
func resolveApplication(ctx context.Context, cfg *config.Config, client *apiclient.Client, name, image string, ports []apiclient.PortSpec) (string, error) {
	if id, ok := cfg.Applications[name]; ok {
		return id, nil
	}
	if image == "" {
		return "", fmt.Errorf("application %q doesn't exist yet in project %q — pass --image to create it", name, cfg.ProjectName)
	}
	app, err := client.CreateApplication(ctx, cfg.ProjectID, name, image, ports)
	if err != nil {
		return "", err
	}
	cfg.Applications[name] = app.ID
	if err := cfg.Save(); err != nil {
		return "", fmt.Errorf("saving session: %w", err)
	}
	return app.ID, nil
}

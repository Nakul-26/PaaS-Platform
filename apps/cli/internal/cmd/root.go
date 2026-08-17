// Package cmd wires the CLI's cobra command tree (ADR-0007, phase-1-mvp.md
// Task 6). Every command is a thin call into apiclient — no business logic
// (quota checks, RBAC, validation) is duplicated here; the server is the
// only authority on whether an action is allowed, and its response is
// surfaced as-is.
package cmd

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"platform/apps/cli/internal/apiclient"
	"platform/apps/cli/internal/config"
)

var apiURLFlag string

// Execute runs the CLI, returning the exit code the process should use.
func Execute() int {
	root := &cobra.Command{
		Use:           "platform",
		Short:         "Thin CLI client for the platform API server",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.PersistentFlags().StringVar(&apiURLFlag, "api-url", "", "API server base URL (default: saved config, then http://localhost:8080)")

	root.AddCommand(
		newSignupCmd(),
		newLoginCmd(),
		newCreateCmd(),
		newDeployCmd(),
		newGetCmd(),
		newLogsCmd(),
		newDeleteCmd(),
	)

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", describeErr(err))
		return 1
	}
	return 0
}

// session loads the local config and builds an apiclient.Client against
// it, resolving the API base URL as: --api-url flag > saved config > the
// local-dev default.
func session() (*config.Config, *apiclient.Client, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, nil, fmt.Errorf("loading CLI config: %w", err)
	}
	if apiURLFlag != "" {
		cfg.APIURL = apiURLFlag
	} else if cfg.APIURL == "" {
		cfg.APIURL = "http://localhost:8080"
	}
	return cfg, apiclient.New(cfg.APIURL, cfg), nil
}

// requireLogin is like session but fails fast with a clear message if
// there's no saved access token, instead of letting every subsequent call
// fail with an opaque 401.
func requireLogin() (*config.Config, *apiclient.Client, error) {
	cfg, client, err := session()
	if err != nil {
		return nil, nil, err
	}
	if cfg.AccessToken == "" {
		return nil, nil, fmt.Errorf("not logged in — run `platform login` first")
	}
	return cfg, client, nil
}

// requireProject is like requireLogin but also fails fast if no project is
// current yet.
func requireProject() (*config.Config, *apiclient.Client, error) {
	cfg, client, err := requireLogin()
	if err != nil {
		return nil, nil, err
	}
	if cfg.ProjectID == "" {
		return nil, nil, fmt.Errorf("no current project — run `platform create project <name>` first")
	}
	return cfg, client, nil
}

func describeErr(err error) string {
	var apiErr *apiclient.APIError
	if errors.As(err, &apiErr) {
		return apiErr.Message
	}
	return err.Error()
}

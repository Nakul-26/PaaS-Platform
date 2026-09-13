package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// newGetQuotaCmd is `platform get quota` (phase-6-multi-tenant-saas.md
// Task 6) — no argument, current-org precedent as get_org.go/get_api_keys.go.
// CLI-only: there is no `platform set quota` (quota limits are
// platform-operator-set, not self-service, per the phase doc's own open
// decision).
func newGetQuotaCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "quota",
		Short: "Show current usage against the organization's resource quota",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, client, err := requireLogin()
			if err != nil {
				return err
			}
			q, err := client.GetQuota(cmd.Context(), cfg.OrgID)
			if err != nil {
				return err
			}
			fmt.Printf("RESOURCE          USED   MAX\n")
			fmt.Printf("projects          %-6d %d\n", q.UsedProjects, q.MaxProjects)
			fmt.Printf("containers        %-6d %d\n", q.UsedContainers, q.MaxContainers)
			fmt.Printf("cpu (millicores)  %-6d %d\n", q.UsedCPUMillicores, q.MaxCPUMillicores)
			fmt.Printf("memory (MB)       %-6d %d\n", q.UsedMemoryMB, q.MaxMemoryMB)
			fmt.Printf("deploys (24h)     %-6d %d\n", q.UsedDeploymentsToday, q.MaxDeploymentsPerDay)
			return nil
		},
	}
	return c
}

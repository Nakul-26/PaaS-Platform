package cmd

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

// newGetAuditLogCmd is `platform get audit-log` (phase-6-multi-tenant-saas.md
// Task 7) — no argument, current-org precedent as get_quota.go/get_org.go.
// Only an owner/admin session can call this successfully (audit_logs.view
// gates the underlying route). Fetches one page at a time, newest first,
// same as api-conventions.md §4's own cursor example for this exact route —
// pass --cursor with a prior call's printed cursor to page further back.
func newGetAuditLogCmd() *cobra.Command {
	var cursor string
	c := &cobra.Command{
		Use:   "audit-log",
		Short: "Show the organization's audit log, newest first",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, client, err := requireLogin()
			if err != nil {
				return err
			}
			page, err := client.ListAuditLogs(cmd.Context(), cfg.OrgID, cursor)
			if err != nil {
				return err
			}
			if len(page.Data) == 0 {
				fmt.Println("No audit log entries.")
				return nil
			}
			tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
			_, _ = fmt.Fprintln(tw, "CREATED_AT\tACTION\tTARGET_TYPE\tTARGET_ID\tACTOR_USER_ID")
			for _, e := range page.Data {
				actor := "-"
				if e.ActorUserID != nil {
					actor = *e.ActorUserID
				}
				_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", e.CreatedAt.Format("2006-01-02T15:04:05Z"), e.Action, e.TargetType, e.TargetID, actor)
			}
			if err := tw.Flush(); err != nil {
				return err
			}
			if page.NextCursor != nil {
				fmt.Printf("\nmore entries available — pass --cursor %s to see the next page\n", *page.NextCursor)
			}
			return nil
		},
	}
	c.Flags().StringVar(&cursor, "cursor", "", "opaque pagination cursor from a previous call's next-page hint")
	return c
}

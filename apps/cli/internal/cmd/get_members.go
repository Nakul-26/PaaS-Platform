package cmd

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

// newGetMembersCmd is `platform get members` (phase-6-multi-tenant-saas.md
// Task 4), listing the current org's members. Acts on the CLI session's
// single current org (cfg.OrgID), same posture as newGetOrgCmd. What comes
// back depends on the caller's own role — see apiclient.Client.ListMembers's
// doc comment: an owner/admin sees the full roster, any other role sees
// only its own row.
func newGetMembersCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "members",
		Short: "List members of the current organization",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, client, err := requireLogin()
			if err != nil {
				return err
			}
			page, err := client.ListMembers(cmd.Context(), cfg.OrgID)
			if err != nil {
				return err
			}
			if len(page.Data) == 0 {
				fmt.Println("No members.")
				return nil
			}
			tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
			_, _ = fmt.Fprintln(tw, "EMAIL\tROLE\tCREATED_AT")
			for _, m := range page.Data {
				_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\n", m.Email, m.Role, m.CreatedAt.Format("2006-01-02T15:04:05Z"))
			}
			return tw.Flush()
		},
	}
	return c
}

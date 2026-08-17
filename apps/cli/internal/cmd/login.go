package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newLoginCmd() *cobra.Command {
	var email, password string
	c := &cobra.Command{
		Use:   "login",
		Short: "Authenticate and save a session",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, client, err := session()
			if err != nil {
				return err
			}
			resp, err := client.Login(cmd.Context(), email, password)
			if err != nil {
				return err
			}
			cfg.SetSession(resp.Org.ID, resp.Org.Slug, resp.AccessToken, resp.RefreshToken)
			if err := cfg.Save(); err != nil {
				return fmt.Errorf("saving session: %w", err)
			}
			fmt.Printf("Logged in as %s, organization %s (%s)\n", resp.User.Email, resp.Org.Slug, resp.Org.Role)
			return nil
		},
	}
	c.Flags().StringVar(&email, "email", "", "account email (required)")
	c.Flags().StringVar(&password, "password", "", "account password (required)")
	_ = c.MarkFlagRequired("email")
	_ = c.MarkFlagRequired("password")
	return c
}

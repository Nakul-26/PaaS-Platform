package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newSignupCmd() *cobra.Command {
	var email, password string
	c := &cobra.Command{
		Use:   "signup",
		Short: "Create a new account and its default organization",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, client, err := session()
			if err != nil {
				return err
			}
			resp, err := client.Signup(cmd.Context(), email, password)
			if err != nil {
				return err
			}
			cfg.SetSession(resp.Org.ID, resp.Org.Slug, resp.AccessToken, resp.RefreshToken)
			if err := cfg.Save(); err != nil {
				return fmt.Errorf("saving session: %w", err)
			}
			fmt.Printf("Signed up as %s, organization %s (%s)\n", resp.User.Email, resp.Org.Slug, resp.Org.Role)
			return nil
		},
	}
	c.Flags().StringVar(&email, "email", "", "account email (required)")
	c.Flags().StringVar(&password, "password", "", "account password (required)")
	_ = c.MarkFlagRequired("email")
	_ = c.MarkFlagRequired("password")
	return c
}

// Package config persists the CLI's local session state (auth tokens,
// current org/project, and resolved application name -> ID mappings) to a
// per-user file. This is purely CLI convenience state, not a source of
// truth — the server never trusts anything read back from it, and losing
// this file only costs the user a re-login/re-lookup, never data.
package config

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// Config is the CLI's on-disk session state.
type Config struct {
	APIURL       string `json:"api_url,omitempty"`
	AccessToken  string `json:"access_token,omitempty"`
	RefreshToken string `json:"refresh_token,omitempty"`
	OrgID        string `json:"org_id,omitempty"`
	OrgSlug      string `json:"org_slug,omitempty"`
	ProjectID    string `json:"project_id,omitempty"`
	ProjectName  string `json:"project_name,omitempty"`
	// Applications maps an application name to the ID the server assigned
	// it, scoped to ProjectID — cleared whenever ProjectID changes.
	Applications map[string]string `json:"applications,omitempty"`

	path string
}

// Path returns the default config file location: ~/.platform/config.json.
func Path() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".platform", "config.json"), nil
}

// Load reads the config file, returning an empty Config (not an error) if
// it doesn't exist yet — the first `platform login`/`signup` creates it.
func Load() (*Config, error) {
	path, err := Path()
	if err != nil {
		return nil, err
	}
	return loadFrom(path)
}

func loadFrom(path string) (*Config, error) {
	cfg := &Config{path: path, Applications: map[string]string{}}

	// #nosec G304 -- path is always either Path()'s own computed
	// ~/.platform/config.json or a test's t.TempDir() path, never
	// user/network-controlled input.
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return cfg, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, err
	}
	cfg.path = path
	if cfg.Applications == nil {
		cfg.Applications = map[string]string{}
	}
	return cfg, nil
}

// Save writes the config file, restricted to the owning user since it
// holds a live refresh token.
func (c *Config) Save() error {
	if err := os.MkdirAll(filepath.Dir(c.path), 0o700); err != nil {
		return err
	}
	// #nosec G117 -- this is the whole point of the struct: persisting the
	// CLI's own session tokens to its local config file, not a hardcoded
	// credential.
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(c.path, data, 0o600)
}

// SetSession stores a fresh auth response and clears any project/app state
// left over from a previous session — a new login/signup may land in a
// different org, under which old project/app IDs are meaningless.
func (c *Config) SetSession(orgID, orgSlug, accessToken, refreshToken string) {
	c.OrgID = orgID
	c.OrgSlug = orgSlug
	c.AccessToken = accessToken
	c.RefreshToken = refreshToken
	c.ProjectID = ""
	c.ProjectName = ""
	c.Applications = map[string]string{}
}

// GetAccessToken, GetRefreshToken, and SetTokens implement
// apiclient.TokenStore.
func (c *Config) GetAccessToken() string  { return c.AccessToken }
func (c *Config) GetRefreshToken() string { return c.RefreshToken }

func (c *Config) SetTokens(accessToken, refreshToken string) {
	c.AccessToken = accessToken
	c.RefreshToken = refreshToken
}

// SetProject records the current project and clears cached application IDs
// from whatever project was current before (an app name is only unique
// within its project).
func (c *Config) SetProject(id, name string) {
	c.ProjectID = id
	c.ProjectName = name
	c.Applications = map[string]string{}
}

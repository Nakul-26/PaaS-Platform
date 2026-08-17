package config

import (
	"path/filepath"
	"testing"
)

func TestSaveLoad_RoundTrip(t *testing.T) {
	cfg := &Config{path: filepath.Join(t.TempDir(), "config.json"), Applications: map[string]string{}}
	cfg.SetSession("org-1", "org-slug", "access-tok", "refresh-tok")
	cfg.SetProject("proj-1", "proj-name")
	cfg.Applications["demo"] = "app-1"

	if err := cfg.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}

	reloaded, err := loadFrom(cfg.path)
	if err != nil {
		t.Fatalf("loadFrom: %v", err)
	}

	if reloaded.OrgID != "org-1" || reloaded.ProjectID != "proj-1" || reloaded.Applications["demo"] != "app-1" {
		t.Fatalf("round-tripped config mismatch: %+v", reloaded)
	}
	if reloaded.AccessToken != "access-tok" {
		t.Fatalf("access token not preserved: %+v", reloaded)
	}
}

func TestSetProject_ClearsApplications(t *testing.T) {
	cfg := &Config{Applications: map[string]string{"demo": "app-1"}}
	cfg.SetProject("proj-2", "other")
	if len(cfg.Applications) != 0 {
		t.Fatalf("expected Applications cleared on project switch, got %v", cfg.Applications)
	}
}

func TestSetSession_ClearsProjectAndApplications(t *testing.T) {
	cfg := &Config{ProjectID: "proj-1", ProjectName: "old", Applications: map[string]string{"demo": "app-1"}}
	cfg.SetSession("org-2", "org-2-slug", "a", "r")
	if cfg.ProjectID != "" || cfg.ProjectName != "" || len(cfg.Applications) != 0 {
		t.Fatalf("expected project/app state cleared on new session, got %+v", cfg)
	}
}

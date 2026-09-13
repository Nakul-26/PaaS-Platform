package imagebuilder

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

// cloneRepo shallow-clones gitURL at gitRef into a fresh temp directory and
// resolves the checked-out commit's full SHA. Public repos only
// (phase-7-deployment-platform.md Open Decision 4 — no credential storage
// or injection exists yet); bounded by timeout so one hung/malicious clone
// can't wedge the consumer indefinitely.
//
// Shells out to the system git binary rather than a Go git library or a
// new named port: no documented alternative driver exists yet, and
// docs/modularity-and-extensibility.md §6 is explicit that an interface
// needs a concrete reason, not just "to be safe."
func cloneRepo(ctx context.Context, gitURL, gitRef string, timeout time.Duration) (dir string, commitSHA string, err error) {
	dir, err = os.MkdirTemp("", "image-builder-clone-*")
	if err != nil {
		return "", "", fmt.Errorf("creating temp clone dir: %w", err)
	}

	cloneCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if out, err := runGit(cloneCtx, "", "clone", "--depth", "1", "--branch", gitRef, gitURL, dir); err != nil {
		_ = os.RemoveAll(dir)
		return "", "", fmt.Errorf("git clone: %w: %s", err, out)
	}

	sha, err := runGit(cloneCtx, dir, "rev-parse", "HEAD")
	if err != nil {
		_ = os.RemoveAll(dir)
		return "", "", fmt.Errorf("git rev-parse HEAD: %w: %s", err, sha)
	}

	return dir, strings.TrimSpace(sha), nil
}

// runGit runs `git <args...>`, optionally with -C dir, and returns
// combined stdout+stderr (git's own error text is on stderr, and callers
// here want it surfaced either way) trimmed of trailing whitespace.
func runGit(ctx context.Context, dir string, args ...string) (string, error) {
	if dir != "" {
		args = append([]string{"-C", dir}, args...)
	}
	cmd := exec.CommandContext(ctx, "git", args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	return strings.TrimSpace(out.String()), err
}

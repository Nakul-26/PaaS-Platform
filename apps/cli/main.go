// Command platform is the cobra-based CLI, a thin REST client over the API
// server (ADR-0007, phase-1-mvp.md Task 6).
package main

import (
	"os"

	"platform/apps/cli/internal/cmd"
)

func main() {
	os.Exit(cmd.Execute())
}

package nodeagent

import (
	"fmt"
	"os"
	"strings"

	"github.com/google/uuid"
)

// LoadOrCreateNodeID reads a UUID from path, or generates and persists a new
// one if the file doesn't exist yet. This is the node ID docs/nats-contract.md
// requires: "a UUID it generates and persists locally on first run, so
// restarts re-register as the same node rather than leaking a new row every
// restart."
func LoadOrCreateNodeID(path string) (uuid.UUID, error) {
	// #nosec G304 -- path is operator-set config (WORKER_NODE_ID_FILE), never
	// request/network-controlled input.
	data, err := os.ReadFile(path)
	if err == nil {
		id, parseErr := uuid.Parse(strings.TrimSpace(string(data)))
		if parseErr != nil {
			return uuid.UUID{}, fmt.Errorf("parsing node id from %s: %w", path, parseErr)
		}
		return id, nil
	}
	if !os.IsNotExist(err) {
		return uuid.UUID{}, fmt.Errorf("reading node id file %s: %w", path, err)
	}

	id := uuid.New()
	if err := os.WriteFile(path, []byte(id.String()), 0o600); err != nil {
		return uuid.UUID{}, fmt.Errorf("writing node id file %s: %w", path, err)
	}
	return id, nil
}

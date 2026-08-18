package nodeagent

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadOrCreateNodeID_GeneratesAndPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker-node-id")

	first, err := LoadOrCreateNodeID(path)
	if err != nil {
		t.Fatalf("first LoadOrCreateNodeID: %v", err)
	}
	if first.String() == "" {
		t.Fatal("expected a non-empty generated node id")
	}

	second, err := LoadOrCreateNodeID(path)
	if err != nil {
		t.Fatalf("second LoadOrCreateNodeID: %v", err)
	}
	if second != first {
		t.Fatalf("expected re-reading the same file to return the same id: first=%s second=%s", first, second)
	}
}

func TestLoadOrCreateNodeID_RejectsCorruptFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker-node-id")
	if err := os.WriteFile(path, []byte("not-a-uuid"), 0o600); err != nil {
		t.Fatalf("seeding corrupt file: %v", err)
	}

	if _, err := LoadOrCreateNodeID(path); err == nil {
		t.Fatal("expected an error reading a corrupt node id file, got nil")
	}
}

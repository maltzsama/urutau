package state_test

import (
	"path/filepath"
	"testing"

	"github.com/maltzsama/urutau/internal/state"
)

func TestOpenClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	store, err := state.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestPositionSetGet(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	position := []byte("gtid-set-123")
	if err := store.SetPosition("pipeline-1", position); err != nil {
		t.Fatalf("SetPosition: %v", err)
	}

	got, err := store.Position("pipeline-1")
	if err != nil {
		t.Fatalf("Position: %v", err)
	}
	if string(got) != string(position) {
		t.Errorf("Position = %q, want %q", got, position)
	}
}

func TestPositionNotFound(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	got, err := store.Position("nonexistent")
	if err != nil {
		t.Fatalf("Position: %v", err)
	}
	if got != nil {
		t.Errorf("Position = %v, want nil", got)
	}
}

func TestPositionDelete(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	if err := store.SetPosition("pipeline-1", []byte("pos")); err != nil {
		t.Fatalf("SetPosition: %v", err)
	}
	if err := store.DeletePosition("pipeline-1"); err != nil {
		t.Fatalf("DeletePosition: %v", err)
	}

	got, err := store.Position("pipeline-1")
	if err != nil {
		t.Fatalf("Position: %v", err)
	}
	if got != nil {
		t.Errorf("Position = %v, want nil", got)
	}
}

func TestSnapshotSetGet(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	meta := &state.SnapshotMetadata{
		EndOffset: "base64-encoded-offset",
		Schema:    map[string]string{"id": "int64", "name": "utf8"},
	}
	if err := store.SetSnapshot("orders", meta); err != nil {
		t.Fatalf("SetSnapshot: %v", err)
	}

	got, err := store.GetSnapshot("orders")
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if got.EndOffset != meta.EndOffset {
		t.Errorf("EndOffset = %q, want %q", got.EndOffset, meta.EndOffset)
	}
	if len(got.Schema) != len(meta.Schema) {
		t.Errorf("Schema len = %d, want %d", len(got.Schema), len(meta.Schema))
	}
}

func TestSnapshotNotFound(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	got, err := store.GetSnapshot("nonexistent")
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if got != nil {
		t.Errorf("GetSnapshot = %v, want nil", got)
	}
}

func TestSnapshotDelete(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	if err := store.SetSnapshot("orders", &state.SnapshotMetadata{EndOffset: "test"}); err != nil {
		t.Fatalf("SetSnapshot: %v", err)
	}
	if err := store.DeleteSnapshot("orders"); err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}

	got, err := store.GetSnapshot("orders")
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if got != nil {
		t.Errorf("GetSnapshot = %v, want nil", got)
	}
}

func TestMultiplePipelines(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	if err := store.SetPosition("pipeline-1", []byte("pos1")); err != nil {
		t.Fatalf("SetPosition: %v", err)
	}
	if err := store.SetPosition("pipeline-2", []byte("pos2")); err != nil {
		t.Fatalf("SetPosition: %v", err)
	}

	pos1, _ := store.Position("pipeline-1")
	pos2, _ := store.Position("pipeline-2")

	if string(pos1) != "pos1" {
		t.Errorf("pipeline-1 = %q, want pos1", pos1)
	}
	if string(pos2) != "pos2" {
		t.Errorf("pipeline-2 = %q, want pos2", pos2)
	}
}

func TestPositionOverwrite(t *testing.T) {
	store := openTestStore(t)
	defer store.Close()

	if err := store.SetPosition("pipeline-1", []byte("old")); err != nil {
		t.Fatalf("SetPosition: %v", err)
	}
	if err := store.SetPosition("pipeline-1", []byte("new")); err != nil {
		t.Fatalf("SetPosition: %v", err)
	}

	got, _ := store.Position("pipeline-1")
	if string(got) != "new" {
		t.Errorf("Position = %q, want new", got)
	}
}

func openTestStore(t *testing.T) *state.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	store, err := state.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

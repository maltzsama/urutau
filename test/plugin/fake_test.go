package plugin

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/maltzsama/urutau/change"
	"github.com/maltzsama/urutau/internal/runner"
	"github.com/maltzsama/urutau/spec"
)

// TestExternalPluginDrivesPipeline proves the plugin seam end-to-end: the
// fake source and sink above live outside internal/ and import only the
// public contracts, yet the collapsed runner boots and drives them, and the
// seeded changes reach the sink's committer.
func TestExternalPluginDrivesPipeline(t *testing.T) {
	seedRows = []change.Change{
		{Op: change.OpInsert, Table: "raw.t", Key: []any{int64(1)}, After: map[string]any{"id": int64(1), "v": "a"}},
		{Op: change.OpInsert, Table: "raw.t", Key: []any{int64(2)}, After: map[string]any{"id": int64(2), "v": "b"}},
	}
	*committed = *newRecords()

	s := &spec.Spec{
		Pipeline: "plugin-proof",
		Source:   spec.Source{Kind: "fake", URI: "fake://"},
		Sink:     spec.Sink{Type: "fake", URI: "fake://", Namespace: "raw"},
		Tables: []spec.Table{{
			Source: "src.t", Target: "raw.t", PrimaryKey: []string{"id"},
		}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.Run(ctx, s, runner.Config{MaxRows: 10, MaxInterval: 20 * time.Millisecond})
	}()

	deadline := time.Now().Add(10 * time.Second)
	for len(committed.rows("raw.t")) < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for committed rows; got %d", len(committed.rows("raw.t")))
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	if err := <-errCh; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("runner returned: %v", err)
	}

	got := committed.rows("raw.t")
	if len(got) != 2 {
		t.Fatalf("committed %d rows, want 2", len(got))
	}
	seen := map[int64]string{}
	for _, c := range got {
		seen[c.Key[0].(int64)] = c.After["v"].(string)
	}
	if seen[1] != "a" || seen[2] != "b" {
		t.Fatalf("committed values = %v, want {1:a 2:b}", seen)
	}
}

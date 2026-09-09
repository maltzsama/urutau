package plugin

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/runner"
	"github.com/maltzsama/urutau/spec"
)

// TestExternalPluginDrivesPipeline proves the plugin seam end-to-end: the
// fake source and sink above live outside internal/ and import only the
// public contracts, yet the collapsed runner boots and drives them, and the
// seeded changes reach the sink's committer.
func TestExternalPluginDrivesPipeline(t *testing.T) {
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

// TestRunnerWithAdaptersDrivesPipeline is the regression for the audit
// finding: runExternalPlugins built the plugin adapters but never wired
// them into a runner, so the external-plugin path started subprocesses and
// replicated no data. NewRunnerWithAdapters must run the pipeline over
// concrete source.Source / sink.Sink instances and reach the sink.
func TestRunnerWithAdaptersDrivesPipeline(t *testing.T) {
	*committed = *newRecords()

	s := &spec.Spec{
		Pipeline: "plugin-adapters",
		Source:   spec.Source{Kind: "fake", URI: "fake://"},
		Sink:     spec.Sink{Type: "fake", URI: "fake://", Namespace: "raw"},
		Tables: []spec.Table{{
			Source: "src.t", Target: "raw.t", PrimaryKey: []string{"id"},
		}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r, err := runner.NewRunnerWithAdapters(ctx, s, runner.Config{MaxRows: 10, MaxInterval: 20 * time.Millisecond},
		Source{}, &Sink{rec: committed})
	if err != nil {
		t.Fatalf("NewRunnerWithAdapters: %v", err)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- r.Run(ctx) }()

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
		t.Fatalf("committed %d rows, want 2 (the adapters must flow data)", len(got))
	}
	seen := map[int64]string{}
	for _, c := range got {
		seen[c.Key[0].(int64)] = c.After["v"].(string)
	}
	if seen[1] != "a" || seen[2] != "b" {
		t.Fatalf("committed values = %v, want {1:a 2:b}", seen)
	}
}

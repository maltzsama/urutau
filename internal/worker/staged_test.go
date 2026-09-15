package worker

import (
	"context"
	"testing"
	"time"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
)

// stagingCommitter is a TableWriter that also stages (sink.StagingWriter).
type stagingCommitter struct {
	commits int
	stages  int
	desc    []byte
}

func (s *stagingCommitter) Close() error { return nil }

func (s *stagingCommitter) Commit(context.Context, *dataplane.Batch) error {
	s.commits++
	return nil
}

func (s *stagingCommitter) WriteStaged(context.Context, *dataplane.Batch) ([]byte, error) {
	s.stages++
	return s.desc, nil
}

// A staged table must ship the descriptor and never call Commit: the
// coordinator owns the commit, and a local Commit would double-write.
func TestStagedPipelineShipsDescriptorInsteadOfCommitting(t *testing.T) {
	sc := &stagingCommitter{desc: []byte("payload")}
	w := New(Config{MaxRows: 1, MaxInterval: time.Hour})
	w.Register("t", sc, dataplane.UpsertMode)
	w.SetKnownSchema("t", testSchema())
	if err := w.SetStaged("t"); err != nil {
		t.Fatalf("SetStaged: %v", err)
	}
	var gotDesc []byte
	var gotTable string
	w.OnStaged(func(table string, _ uint64, desc []byte, _, _ string, _ []uint32) {
		gotTable = table
		gotDesc = desc
	})

	ing := make(chan Ingest, 2)
	for _, in := range ingestFromChanges(t, []rowchange.Change{chg("t", rowchange.OpInsert, 1, "v", "p1")}) {
		ing <- in
	}
	close(ing)
	if err := w.Run(context.Background(), ing); err != nil {
		t.Fatalf("run: %v", err)
	}
	if sc.commits != 0 {
		t.Fatalf("staged table committed %d times, want 0", sc.commits)
	}
	if sc.stages != 1 {
		t.Fatalf("staged %d times, want 1", sc.stages)
	}
	if gotTable != "t" || string(gotDesc) != "payload" {
		t.Fatalf("descriptor not shipped: table=%q desc=%q", gotTable, gotDesc)
	}
}

// A staged assignment on a writer that cannot stage is a programming error:
// committing locally would hide the data from the coordinator's cycle.
func TestSetStagedRefusesNonStagingWriter(t *testing.T) {
	w := New(Config{MaxRows: 1, MaxInterval: time.Hour})
	w.Register("t", CommitterFunc(func(context.Context, *dataplane.Batch) error { return nil }), dataplane.UpsertMode)
	if err := w.SetStaged("t"); err == nil {
		t.Fatal("SetStaged accepted a writer that cannot stage")
	}
}

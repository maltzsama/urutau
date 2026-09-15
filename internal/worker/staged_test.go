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
	w.OnStaged(func(table string, _ uint64, desc []byte, _, _ string, _ []uint32) error {
		gotTable = table
		gotDesc = desc
		return nil
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

// TestStagedFlushesPerCycleSeq: a worker ships one descriptor per
// coordinator cycle. Batches carrying different Batch.Seq are different
// binlog batches (different cycles) and must not be merged into one flush —
// the coordinator expects exactly one delivery per (table, seq) — and Seq
// must survive the collapse/merge reconstruction (WK-001 C5.4). A dropped
// Seq sent live batches back as seq-0 snapshot cycles, bypassing the whole
// cycle model.
func TestStagedFlushesPerCycleSeq(t *testing.T) {
	sc := &stagingCommitter{desc: []byte("d")}
	// No MaxRows/ticker flush: only the cycle boundary may flush, so the
	// assertion isolates the seq logic.
	w := New(Config{MaxRows: 10000, MaxInterval: time.Hour})
	w.Register("t", sc, dataplane.UpsertMode)
	w.SetKnownSchema("t", testSchema())
	if err := w.SetStaged("t"); err != nil {
		t.Fatalf("SetStaged: %v", err)
	}
	var seqs []uint64
	w.OnStaged(func(_ string, seq uint64, _ []byte, _, _ string, _ []uint32) error {
		seqs = append(seqs, seq)
		return nil
	})

	ing := make(chan Ingest, 4)
	ing <- seqIngest(t, "t", 5, chg("t", rowchange.OpInsert, 1, "a", "p1"))
	ing <- seqIngest(t, "t", 6, chg("t", rowchange.OpInsert, 2, "b", "p2"))
	close(ing)
	if err := w.Run(context.Background(), ing); err != nil {
		t.Fatalf("run: %v", err)
	}
	if sc.stages != 2 {
		t.Fatalf("staged %d times, want 2 (one per cycle seq)", sc.stages)
	}
	if len(seqs) != 2 || seqs[0] != 5 || seqs[1] != 6 {
		t.Fatalf("staged seqs = %v, want [5 6]", seqs)
	}
}

// seqIngest wraps one change into an Ingest whose batch carries the given
// coordinator cycle sequence.
func seqIngest(t *testing.T, table string, seq uint64, c rowchange.Change) Ingest {
	t.Helper()
	b := wireBatch(t, table, dataplane.UpsertMode, []rowchange.Change{c})
	b.Seq = seq
	return Ingest{Table: table, Batch: b}
}

// A staged cycle whose sub-batch produced no data (an append-mode delete with
// no before image) must still deliver a descriptor: the coordinator expects
// one delivery per (table, seq), and a missing one leaves the cycle open,
// blocking every cycle behind it in the table's send order (WK-001 C5.4).
func TestStagedDeliversEmptyDescriptorWhenRowsDropped(t *testing.T) {
	sc := &stagingCommitter{desc: []byte("d")}
	w := New(Config{MaxRows: 1, MaxInterval: time.Hour})
	w.Register("t", sc, dataplane.AppendMode)
	w.SetKnownSchema("t", testSchema())
	w.SetDropDeletes("t", true)
	if err := w.SetStaged("t"); err != nil {
		t.Fatalf("SetStaged: %v", err)
	}
	var seqs []uint64
	w.OnStaged(func(_ string, seq uint64, _ []byte, _, _ string, _ []uint32) error {
		seqs = append(seqs, seq)
		return nil
	})

	ing := make(chan Ingest, 2)
	ing <- seqIngest(t, "t", 7, chg("t", rowchange.OpDelete, 1, "", "p1"))
	close(ing)
	if err := w.Run(context.Background(), ing); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(seqs) != 1 || seqs[0] != 7 {
		t.Fatalf("staged seqs = %v, want [7] (an empty delivery for the dropped cycle)", seqs)
	}
}

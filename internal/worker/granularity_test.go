package worker

// G0 granularity-insensitivity: the worker must produce IDENTICAL sink
// writes regardless of how the coordinator splits a logical stream into
// batches. The batch-native coordinator pump (G0) changes batch
// boundaries only — this test proves the worker does not care, which is
// what makes that pump change safe.

import (
	"context"
	"testing"
	"time"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
)

// toIngestBatch bridges one logical stream into Ingest batches of at most
// perBatch rows each.
func toIngestBatch(t *testing.T, changes []rowchange.Change, perBatch int) []Ingest {
	t.Helper()
	if perBatch <= 0 {
		perBatch = len(changes)
	}
	var out []Ingest
	for start := 0; start < len(changes); start += perBatch {
		end := start + perBatch
		if end > len(changes) {
			end = len(changes)
		}
		chunk := changes[start:end]
		if len(chunk) == 0 {
			continue
		}
		dpb := wireBatch(t, chunk[0].Table, dataplane.UpsertMode, chunk)
		out = append(out, Ingest{Table: chunk[0].Table, Batch: dpb})
	}
	return out
}

// collectWrites runs a worker over the given ingests and returns the final
// committed rows as (id -> value) per committed batch order flattened.
func collectWrites(t *testing.T, ingests []Ingest) []rowchange.Change {
	t.Helper()
	fc := &fakeCommitter{}
	w := New(Config{MaxRows: 100, MaxInterval: time.Hour})
	regTable(t, w, "t", fc, dataplane.UpsertMode)

	ing := make(chan Ingest, len(ingests))
	for _, in := range ingests {
		ing <- in
	}
	close(ing)
	if err := w.Run(context.Background(), ing); err != nil {
		t.Fatalf("run: %v", err)
	}

	var all []rowchange.Change
	for _, b := range fc.batches {
		all = append(all, b.Changes...)
	}
	return all
}

func TestGranularityInsensitiveUpsert(t *testing.T) {
	// The same logical stream: insert 1..6, then a delete + re-insert of 3
	// (last-write-wins must resolve identically whatever the batch size).
	changes := []rowchange.Change{
		chg("t", rowchange.OpInsert, 1, "a", "p1"),
		chg("t", rowchange.OpInsert, 2, "b", "p2"),
		chg("t", rowchange.OpInsert, 3, "c", "p3"),
		chg("t", rowchange.OpDelete, 3, "", "p4"),
		chg("t", rowchange.OpInsert, 3, "c2", "p5"),
		chg("t", rowchange.OpInsert, 4, "d", "p6"),
		chg("t", rowchange.OpInsert, 5, "e", "p7"),
		chg("t", rowchange.OpInsert, 6, "f", "p8"),
	}

	// One big batch (source batch == whole stream).
	one := collectWrites(t, toIngestBatch(t, changes, 8))
	// Per-change batches (the OLD coordinator pump granularity).
	perChange := collectWrites(t, toIngestBatch(t, changes, 1))
	// Two mid-size batches.
	two := collectWrites(t, toIngestBatch(t, changes, 3))

	same := func(a, b []rowchange.Change, name string) {
		t.Helper()
		if len(a) != len(b) {
			t.Fatalf("%s: row counts differ: %d vs %d", name, len(a), len(b))
		}
		for i := range a {
			if a[i].Op != b[i].Op || a[i].Key[0] != b[i].Key[0] || a[i].After["v"] != b[i].After["v"] {
				t.Fatalf("%s: row %d differs: %+v vs %+v", name, i, a[i], b[i])
			}
		}
	}
	same(one, perChange, "big vs per-change")
	same(one, two, "big vs mid")
}

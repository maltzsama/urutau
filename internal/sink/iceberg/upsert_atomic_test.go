package iceberg

// Issue #550: the direct-path upsert must commit its equality deletes and its
// data rows in ONE snapshot, so a reader never sees an updated key absent and
// metadata growth is one snapshot per batch, not two.

import (
	"context"
	"slices"
	"testing"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/rowchange"
)

// An upsert batch (an update and a delete) adds exactly one snapshot, carries
// both kinds of file, and leaves the sink showing the surviving row.
func TestUpsertCommitsDeletesAndRowsInOneSnapshot(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	ref := createOrders(t, s)
	w, err := s.Writer(ctx, ref, core.CastPolicy{}, nil)
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}

	// Seed two rows in one snapshot.
	seed := wireBatch(t,
		[3]any{int64(1), "a", rowchange.OpInsert},
		[3]any{int64(2), "b", rowchange.OpInsert},
	)
	if err := w.Commit(ctx, seed); err != nil {
		t.Fatalf("Commit(seed): %v", err)
	}
	seed.Release()

	seedTbl, err := s.cat.LoadTable(ctx, s.ident("orders"))
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	before := len(seedTbl.Metadata().Snapshots())

	// One upsert batch: update id=1, delete id=2. Old code committed this in
	// two snapshots (delete, then append); #550 makes it one.
	b := wireBatch(t,
		[3]any{int64(1), "a2", rowchange.OpInsert},
		[3]any{int64(2), "b", rowchange.OpDelete},
	)
	if err := w.Commit(ctx, b); err != nil {
		t.Fatalf("Commit(upsert): %v", err)
	}
	b.Release()

	tbl, err := s.cat.LoadTable(ctx, s.ident("orders"))
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	if got := len(tbl.Metadata().Snapshots()) - before; got != 1 {
		t.Fatalf("upsert added %d snapshots, want 1 (one atomic RowDelta)", got)
	}
	if got, want := scanIDs(t, tbl), []int64{1}; !slices.Equal(got, want) {
		t.Fatalf("rows after upsert = %v, want %v", got, want)
	}
	if pos, err := s.Position(ctx, ref); err != nil || pos != "p" {
		t.Fatalf("position after upsert = %q, %v; want p", pos, err)
	}
}

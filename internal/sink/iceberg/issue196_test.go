package iceberg

import (
	"context"
	"testing"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/snapshot"
)

// #196: an empty batch with a watermark must still advance cdc.position, so a
// restart does not replay the interval forever.
func TestCommitEmptyBatchAdvancesPosition(t *testing.T) {
	s := hadoopSink(t)
	ref := createOrders(t, s)
	ctx := context.Background()

	w, err := NewTableWriter(ctx, s.cat, s.ident(ref.Target), ref.PrimaryKey, core.CastPolicy{}, nil, "shop.orders", 0)
	if err != nil {
		t.Fatalf("NewTableWriter: %v", err)
	}

	empty := &dataplane.Batch{Table: ref.Target, Watermark: []byte("gtid:1-5"), Mode: dataplane.UpsertMode}
	if err := w.Commit(ctx, empty); err != nil {
		t.Fatalf("Commit(empty): %v", err)
	}

	pos, err := s.Position(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if pos != "gtid:1-5" {
		t.Fatalf("cdc.position = %q, want gtid:1-5", pos)
	}
}

// #196 (review): a snapshot-only empty batch (no position) must still persist
// its snapshot state, not just the position.
func TestCommitEmptyBatchPersistsSnapshotState(t *testing.T) {
	s := hadoopSink(t)
	ref := createOrders(t, s)
	ctx := context.Background()

	w, err := NewTableWriter(ctx, s.cat, s.ident(ref.Target), ref.PrimaryKey, core.CastPolicy{}, nil, "shop.orders", 0)
	if err != nil {
		t.Fatalf("NewTableWriter: %v", err)
	}

	empty := &dataplane.Batch{Table: ref.Target, SnapshotState: "in_progress", SnapshotPending: []uint32{1, 2}}
	if err := w.Commit(ctx, empty); err != nil {
		t.Fatalf("Commit(empty snapshot state): %v", err)
	}

	props, err := s.Properties(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if props[snapshot.PropSnapshotState] != "in_progress" {
		t.Fatalf("cdc.snapshot.state = %q, want in_progress", props[snapshot.PropSnapshotState])
	}
}

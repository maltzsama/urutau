package iceberg

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/snapshot"
)

// dyingCatalog lets commits through until it is armed, then lets armed of
// them through and fails the next: the process dying between an upsert's
// equality-delete commit and its append commit.
type dyingCatalog struct {
	catalog.Catalog
	armed int // commits still let through once armed; -1 = not armed
}

var errDied = errors.New("process died")

func (c *dyingCatalog) LoadTable(ctx context.Context, id table.Identifier) (*table.Table, error) {
	tbl, err := c.Catalog.LoadTable(ctx, id)
	if err != nil {
		return nil, err
	}
	// Bind the table to this catalog, so its commits come through here.
	return table.New(tbl.Identifier(), tbl.Metadata(), tbl.MetadataLocation(), tbl.FS, c), nil
}

func (c *dyingCatalog) CommitTable(ctx context.Context, id table.Identifier, reqs []table.Requirement, ups []table.Update) (table.Metadata, string, error) {
	if c.armed == 0 {
		return nil, "", errDied
	}
	if c.armed > 0 {
		c.armed--
	}
	return c.Catalog.CommitTable(ctx, id, reqs, ups)
}

// Issue #461 regression, seen in chaos-1M-5a01915: an upsert commits its
// equality deletes and its appends as two catalog commits. A snapshot window's
// progress (cdc.snapshot.pending) rode on the first, so a worker that died
// between them left its chunk recorded as done with none of its rows, and a
// restarted coordinator resumed past it: 7,991 rows of pr_events never
// reached Iceberg. The progress, like the position, belongs on the last
// commit only.
func TestAWindowsSnapshotProgressCommitsOnlyWithItsRows(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	dying := &dyingCatalog{Catalog: s.cat, armed: -1}
	s.cat = dying
	ref := createOrders(t, s)
	w, err := s.Writer(ctx, ref, core.CastPolicy{}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Chunks 5, 6 and 7 still to do.
	first := wireBatch(t, [3]any{int64(1), "a", rowchange.OpInsert})
	first.SnapshotState, first.SnapshotPending = string(snapshot.StateInProgress), []uint32{5, 6, 7}
	if err := w.Commit(ctx, first); err != nil {
		t.Fatal(err)
	}
	first.Release()

	// Chunk 5's window closes; the process dies after its deletes commit.
	window := wireBatch(t, [3]any{int64(50), "chunk5", rowchange.OpUpdate}, [3]any{int64(51), "chunk5", rowchange.OpUpdate})
	window.SnapshotState, window.SnapshotPending = string(snapshot.StateInProgress), []uint32{6, 7}
	dying.armed = 1
	if err := w.Commit(ctx, window); !errors.Is(err, errDied) {
		t.Fatalf("commit = %v, want the append to die", err)
	}
	window.Release()
	dying.armed = -1

	props, err := s.Properties(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if got := props[snapshot.PropSnapshotPending]; got != snapshot.EncodePending([]uint32{5, 6, 7}) {
		t.Fatalf("pending = %s after the window's rows failed to commit, want chunk 5 still pending (%s)",
			got, snapshot.EncodePending([]uint32{5, 6, 7}))
	}
}

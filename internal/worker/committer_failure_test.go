package worker

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
)

// A failed commit must end Run with that error while the stream is still
// live. The committer used to exit on the error while the batcher kept
// blocking on the ready channel nobody read any more, so the worker went
// silent: no error logged, no ack, until the coordinator declared it stalled
// and the commit error was never seen.
func TestRunReturnsCommitErrorWhileStreamIsLive(t *testing.T) {
	boom := errors.New("boom")
	w := New(Config{MaxRows: 1, MaxInterval: time.Hour})
	regTable(t, w, "raw.orders", CommitterFunc(func(context.Context, *dataplane.Batch) error {
		return boom
	}), dataplane.UpsertMode)

	ing := make(chan Ingest, 64)
	var changes []rowchange.Change
	for i := range 40 {
		changes = append(changes, chg("raw.orders", rowchange.OpInsert, int64(i), "a", "0/1"))
	}
	for _, in := range ingestFromChanges(t, changes) {
		ing <- in
	}
	// ing stays open: the source keeps streaming.

	done := make(chan error, 1)
	go func() { done <- w.Run(context.Background(), ing) }()
	select {
	case err := <-done:
		if err == nil || !errors.Is(err, boom) && !strings.Contains(err.Error(), "boom") {
			t.Fatalf("Run = %v, want the commit error", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run hung after a commit failure: the committer error was swallowed")
	}
}

// The mirror case: the batcher fails (schema drift) while the committer is
// inside a commit that only returns once its context is cancelled. Run must
// cancel it and return the batcher's error, not hang on the commit, and not
// report the commit's "context canceled" echo instead of the cause.
func TestRunReturnsBatcherErrorWhileCommitBlocks(t *testing.T) {
	w := New(Config{MaxRows: 1, MaxInterval: time.Hour})
	regTable(t, w, "t", CommitterFunc(func(ctx context.Context, _ *dataplane.Batch) error {
		<-ctx.Done()
		return ctx.Err()
	}), dataplane.UpsertMode)
	w.SetKnownSchema("t", core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
	}, PrimaryKey: []string{"id"}})

	ing := make(chan Ingest, 8)
	for _, in := range ingestFromChanges(t, []rowchange.Change{
		{Op: rowchange.OpInsert, Table: "t", Key: []any{1}, After: map[string]any{"id": int64(1)}, Position: "p1"},
		{Op: rowchange.OpInsert, Table: "t", Key: []any{2}, After: map[string]any{"id": int64(2), "extra": "x"}, Position: "p2"},
	}) {
		ing <- in
	}

	done := make(chan error, 1)
	go func() { done <- w.Run(context.Background(), ing) }()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "schema drift") {
			t.Fatalf("Run = %v, want the batcher's schema-drift error", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run hung on a blocked commit after the batcher failed")
	}
}

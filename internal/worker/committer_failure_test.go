package worker

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

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

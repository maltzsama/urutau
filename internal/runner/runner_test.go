package runner

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/maltzsama/urutau/change"
	"github.com/maltzsama/urutau/internal/worker"
	"github.com/maltzsama/urutau/position"
)

const runnerTestUUID = "3e11fa47-71ca-11e1-9e33-c80aa9429562"

// gateCommitter records committed batches.
type gateCommitter struct {
	mu      sync.Mutex
	batches []change.Batch
}

func (c *gateCommitter) Close() error { return nil }
func (c *gateCommitter) Commit(_ context.Context, b change.Batch) error {
	c.mu.Lock()
	c.batches = append(c.batches, b)
	c.mu.Unlock()
	return nil
}

// upsert returns the final committed value for a key, if any.
func (c *gateCommitter) upsert(id int64) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, b := range c.batches {
		for _, u := range b.Upserts {
			if len(u.Key) == 1 && u.Key[0] == id {
				return u.After["v"].(string), true
			}
		}
	}
	return "", false
}

// TestRelayGateLiveEventsAfterWindowRows proves the gate ordering (design
// §3.1): a live event decoded while a chunk's SELECT is in flight is buffered
// by the relay and released InWindow-tagged only after AddWindowRows has
// populated the window — so the live value deterministically wins over the
// stale snapshot row instead of racing ahead of an empty window.
func TestRelayGateLiveEventsAfterWindowRows(t *testing.T) {
	at := position.MustGTID(runnerTestUUID + ":1-9")
	committer := &gateCommitter{}
	w := worker.New(worker.Config{MaxRows: 100, MaxInterval: time.Hour})
	w.RegisterCommitter("raw.orders", committer, change.UpsertMode)

	ingest := make(chan change.Change, 64)
	done := make(chan error, 1)
	go func() { done <- w.Run(context.Background(), ingest) }()

	r := newRelay(ingest, w)
	out := make(chan change.Change, 64)
	relayDone := make(chan struct{})
	go func() {
		_ = r.run(context.Background(), out)
		close(relayDone)
	}()

	// Chunk 0 SELECT in flight: the table's live events are gated.
	r.GateOn("raw.orders", 0)

	// A live UPDATE of id=1 decoded during the SELECT: the reader tags it
	// InWindow, but the relay must hold it until the window is populated.
	out <- change.Change{
		Op:       change.OpUpdate,
		Table:    "raw.orders",
		Key:      []any{int64(1)},
		After:    map[string]any{"id": int64(1), "v": "live"},
		Position: at.String(),
		Window:   &change.Window{ChunkID: 0, InWindow: true},
	}

	// The chunk SELECT lands: id=1 is stale (v=a), id=2 stable (v=x).
	if err := r.AddWindowRows("raw.orders", 0, []change.Change{
		{Op: change.OpInsert, Table: "raw.orders", Key: []any{int64(1)}, After: map[string]any{"id": int64(1), "v": "a"}, Position: at.String()},
		{Op: change.OpInsert, Table: "raw.orders", Key: []any{int64(2)}, After: map[string]any{"id": int64(2), "v": "x"}, Position: at.String()},
	}); err != nil {
		t.Fatalf("AddWindowRows: %v", err)
	}

	// Release the gated live event, then close the chunk.
	r.GateFlush()
	r.Release("raw.orders", 0, at)

	// Wait for the pump to fully exit (it drains the gate buffer first), so
	// closing ingest can never race an in-flight write.
	close(out)
	<-relayDone
	close(ingest)
	if err := <-done; err != nil {
		t.Fatalf("worker run: %v", err)
	}

	if v, ok := committer.upsert(1); !ok || v != "live" {
		t.Fatalf("id=1 must carry the live value, got %q (present=%v)", v, ok)
	}
	if v, ok := committer.upsert(2); !ok || v != "x" {
		t.Fatalf("id=2 must carry the snapshot value, got %q (present=%v)", v, ok)
	}
	// The synchronous GateFlush guarantees the gated live event was
	// deduplicated against the populated window before Closes flushed it —
	// so the droppedByWindow evidence is deterministic, not a race.
	if n := w.DroppedByWindow("raw.orders"); n != 1 {
		t.Fatalf("droppedByWindow = %d, want 1 (deterministic dedup)", n)
	}
}

// The confirmed position must use the position's own ordering, not string
// comparison: "0/10" sorts before "0/2" lexicographically while 16 follows 2
// numerically — a string min would advance the Postgres slot past data still
// in flight.
func TestConfirmedPositionUsesPositionOrdering(t *testing.T) {
	r := &Runner{committedPositions: make(map[string]position.Position)}

	r.updateCommitted("raw.a", position.MustLSN("0/10"))
	r.updateCommitted("raw.b", position.MustLSN("0/2"))

	got := r.confirmedPosition()
	want := position.MustLSN("0/2")
	if got == nil || got.String() != want.String() {
		t.Fatalf("confirmed = %v, want %v — the true minimum", got, want)
	}

	// A later, larger commit moves the floor to the other table's position.
	r.updateCommitted("raw.b", position.MustLSN("0/40"))
	if got := r.confirmedPosition().String(); got != "0/10" {
		t.Fatalf("confirmed = %q after raw.b advanced, want 0/10 (raw.a's position)", got)
	}
}

// Nothing durably committed means nil: the reader must not advance the slot.
func TestConfirmedPositionEmptyIsNil(t *testing.T) {
	r := &Runner{committedPositions: make(map[string]position.Position)}
	if r.confirmedPosition() != nil {
		t.Fatal("confirmed = non-nil with no commits, want nil")
	}
}

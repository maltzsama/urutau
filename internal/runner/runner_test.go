package runner

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/sourcepull"
	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/internal/worker"
	"github.com/maltzsama/urutau/position"
)

const runnerTestUUID = "3e11fa47-71ca-11e1-9e33-c80aa9429562"

// gateCommitter records committed batches.
type gateCommitter struct {
	mu      sync.Mutex
	batches []rowchange.Batch
}

func (c *gateCommitter) Close() error { return nil }
func (c *gateCommitter) Commit(_ context.Context, b *dataplane.Batch) error {
	// Decode to rowchange for row-shaped assertions — a test read path, not
	// a production bridge.
	if b.Record == nil || b.Record.NumRows() == 0 {
		c.mu.Lock()
		c.batches = append(c.batches, rowchange.Batch{Table: b.Table, Position: string(b.Watermark), Mode: rowchange.ToRowMode(b.Mode)})
		c.mu.Unlock()
		return nil
	}
	rows, _ := transport.DecodeBatch(b.Record, b.Table, []string{"id"})
	var upserts []rowchange.Change
	for _, r := range rows {
		if r.Op != rowchange.OpDelete {
			upserts = append(upserts, r)
		}
	}
	c.mu.Lock()
	c.batches = append(c.batches, rowchange.Batch{
		Table: b.Table, Changes: upserts, Position: string(b.Watermark), Mode: rowchange.ToRowMode(b.Mode),
	})
	c.mu.Unlock()
	return nil
}

// upsert returns the final committed value for a key, if any.
func (c *gateCommitter) upsert(id int64) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, b := range c.batches {
		for _, u := range b.Changes {
			if u.Op == rowchange.OpDelete {
				continue
			}
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
	w.RegisterCommitter("raw.orders", committer, dataplane.UpsertMode)
	w.SetKnownSchema("raw.orders", core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "v", Type: core.ColumnType{Kind: core.KindString}},
		},
		PrimaryKey: []string{"id"},
	})

	ingest := make(chan worker.Ingest, 64)
	done := make(chan error, 1)
	go func() { done <- w.Run(context.Background(), ingest) }()

	r := newRelay(ingest, w)
	out := make(chan rowchange.Change, 64)
	pr := &pullTestReader{Puller: sourcepull.New(out)}
	relayDone := make(chan struct{})
	go func() {
		_ = r.run(context.Background(), pr)
		close(relayDone)
	}()

	// Chunk 0 SELECT in flight: the table's live events are gated.
	r.GateOn("raw.orders", 0)

	// A live UPDATE of id=1 decoded during the SELECT: the reader tags it
	// InWindow, but the relay must hold it until the window is populated.
	out <- rowchange.Change{
		Op:       rowchange.OpUpdate,
		Table:    "raw.orders",
		Key:      []any{int64(1)},
		After:    map[string]any{"id": int64(1), "v": "live"},
		Position: at.String(),
		Window:   &rowchange.Window{ChunkID: 0, InWindow: true},
	}

	// The chunk SELECT lands: id=1 is stale (v=a), id=2 stable (v=x).
	if err := r.AddWindowRows("raw.orders", 0, []rowchange.Change{
		{Op: rowchange.OpInsert, Table: "raw.orders", Key: []any{int64(1)}, After: map[string]any{"id": int64(1), "v": "a"}, Position: at.String()},
		{Op: rowchange.OpInsert, Table: "raw.orders", Key: []any{int64(2)}, After: map[string]any{"id": int64(2), "v": "x"}, Position: at.String()},
	}); err != nil {
		t.Fatalf("AddWindowRows: %v", err)
	}

	// Release the gated live event, then close the chunk. The pull-based
	// relay bridges asynchronously; wait until the event is gated.
	deadline := time.Now().Add(2 * time.Second)
	for r.gatedCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if r.gatedCount() == 0 {
		t.Fatal("live event never reached the gate")
	}
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

// pullTestReader adapts a change channel to the pull-based Reader contract
// for relay tests.
type pullTestReader struct {
	*sourcepull.Puller
}

func (p *pullTestReader) Start(context.Context, position.Position) error    { return nil }
func (p *pullTestReader) Synced() position.Position                         { return nil }
func (p *pullTestReader) Master(context.Context) (position.Position, error) { return nil, nil }
func (p *pullTestReader) OpenWindow(context.Context, uint32)                {}
func (p *pullTestReader) ClearWindow()                                      {}
func (p *pullTestReader) Close()                                            {}
func (p *pullTestReader) SetConfirmed(func() position.Position)             {}

// P3 / retention: updateCommitted recomputes the confirmed point the Postgres
// slot advances to. An incomparable pair must set it to nil (hold the slot
// back), never an arbitrary minimum.
func TestUpdateCommittedIncomparableHolds(t *testing.T) {
	r := &Runner{
		log:                slog.New(slog.DiscardHandler),
		committedPositions: map[string]position.Position{},
	}
	r.updateCommitted("a", opaqueTestPos("x"))
	r.updateCommitted("b", opaqueTestPos("y"))
	if r.minConfirmed != nil {
		t.Fatalf("incomparable positions must hold the confirmed point (nil), got %s", r.minConfirmed)
	}

	lo := position.MustGTID("3e11fa47-71ca-11e1-9e33-c80aa9429562:1")
	hi := position.MustGTID("3e11fa47-71ca-11e1-9e33-c80aa9429562:5")
	r2 := &Runner{log: slog.New(slog.DiscardHandler), committedPositions: map[string]position.Position{}}
	r2.updateCommitted("a", lo)
	r2.updateCommitted("b", hi)
	if r2.minConfirmed == nil || r2.minConfirmed.String() != lo.String() {
		t.Fatalf("comparable fold = %v, want %s", r2.minConfirmed, lo)
	}
}

// opaqueTestPos is identity-only: different values are Incomparable.
type opaqueTestPos string

func (o opaqueTestPos) String() string { return string(o) }
func (o opaqueTestPos) Compare(other position.Position) int {
	p, ok := other.(opaqueTestPos)
	if ok && o == p {
		return 0
	}
	return position.Incomparable
}
func (o opaqueTestPos) Contains(other position.Position) bool {
	p, ok := other.(opaqueTestPos)
	return ok && o == p
}

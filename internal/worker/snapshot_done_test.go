package worker

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/snapshot"
	"github.com/maltzsama/urutau/internal/transport"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/position"
)

// commitLog records each commit's row count, watermark and snapshot state.
type commitLog struct {
	mu      sync.Mutex
	commits []committed
}

type committed struct {
	rows      int64
	watermark string
	state     string
}

func (c *commitLog) Close() error { return nil }

func (c *commitLog) Commit(_ context.Context, b *dataplane.Batch) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var n int64
	if b.Record != nil {
		n = b.Record.NumRows()
	}
	c.commits = append(c.commits, committed{rows: n, watermark: string(b.Watermark), state: b.SnapshotState})
	return nil
}

// The snapshot-done marker commits the table's completion after everything
// sent ahead of it, the last window's rows among them (#428): committed
// first, a crash in between would take the snapshot as finished without
// those rows. The completion carries no position (the stream may already
// have committed past the marker's), and the ack reports the marker's.
func TestSnapshotDoneCommitsCompletionAfterTheWindow(t *testing.T) {
	cl := &commitLog{}
	w := New(Config{MaxRows: 100, MaxInterval: time.Hour})
	w.Register("raw.orders", cl, dataplane.UpsertMode)
	w.SetKnownSchema("raw.orders", testSchema())
	var mu sync.Mutex
	var acks []string
	w.OnCommit(func(b *dataplane.Batch, _ int) {
		mu.Lock()
		acks = append(acks, string(b.Watermark))
		mu.Unlock()
	})
	if err := w.AddWindowRows("raw.orders", 7, toWindow(t, "raw.orders", []rowchange.Change{windowRow(1, "a"), windowRow(2, "b")})); err != nil {
		t.Fatalf("AddWindowRows: %v", err)
	}
	ingest := make(chan Ingest, 3)
	ingest <- toIngest(t, rowchange.Change{Table: "raw.orders", Position: "p9", Window: &rowchange.Window{ChunkID: 7, Closes: true}})
	ingest <- Ingest{Table: "raw.orders", Position: "p9", SnapshotDone: true}
	close(ingest)
	if err := w.Run(context.Background(), ingest); err != nil {
		t.Fatalf("run: %v", err)
	}

	cl.mu.Lock()
	defer cl.mu.Unlock()
	if len(cl.commits) != 2 {
		t.Fatalf("commits %+v, want the window's rows then the completion", cl.commits)
	}
	window, done := cl.commits[0], cl.commits[1]
	if window.rows != 2 || window.state != "" {
		t.Fatalf("first commit %+v, want the window's 2 rows without a snapshot state", window)
	}
	if done.rows != 0 || done.state != string(snapshot.StateComplete) || done.watermark != "" {
		t.Fatalf("second commit %+v, want 0 rows, state complete, no position", done)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(acks) != 2 || acks[1] != "p9" {
		t.Fatalf("acks %v, want the completion acked at the marker's position p9", acks)
	}
}

// On a staged table the completion is the marker's cycle: it is delivered
// under the marker's seq, carrying the state and no position, so the
// coordinator commits it in send order after every window.
func TestSnapshotDoneOnAStagedTableIsDeliveredAsItsCycle(t *testing.T) {
	sc := &stagingCommitter{desc: []byte("d")}
	w := New(Config{MaxRows: 100, MaxInterval: time.Hour})
	w.Register("raw.orders", sc, dataplane.UpsertMode)
	w.SetKnownSchema("raw.orders", testSchema())
	type delivery struct {
		seq        uint64
		pos, state string
	}
	var mu sync.Mutex
	var got []delivery
	w.OnStaged(func(_ string, s uint64, _ []byte, pos, state string, _ []uint32) error {
		mu.Lock()
		got = append(got, delivery{s, pos, state})
		mu.Unlock()
		return nil
	})
	if err := w.SetStaged("raw.orders"); err != nil {
		t.Fatalf("SetStaged: %v", err)
	}
	ingest := make(chan Ingest, 1)
	ingest <- Ingest{Table: "raw.orders", Position: "p9", SnapshotDone: true, Seq: 43, Staged: true}
	close(ingest)
	if err := w.Run(context.Background(), ingest); err != nil {
		t.Fatalf("run: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 || got[0].seq != 43 || got[0].state != string(snapshot.StateComplete) || got[0].pos != "" {
		t.Fatalf("staged deliveries %+v, want one under seq 43, state complete, no position", got)
	}
}

// The receiver turns a snapshot-done marker into a SnapshotDone ingest,
// carrying the marker's position and, on a staged table, its cycle. It is
// never skipped as covered: it carries no high position.
func TestReceiverRoutesTheSnapshotDoneMarker(t *testing.T) {
	ingest := make(chan Ingest, 1)
	recv := &batchReceiver{
		ctx:       context.Background(),
		ingest:    ingest,
		committed: map[string]position.Position{"raw.orders": position.MustLSN("0/99")},
		parsePos:  parsePosition("postgres"),
		log:       slog.New(slog.DiscardHandler),
	}
	meta := &pb.BatchMeta{Table: "raw.orders", LowPos: "0/10", BatchId: 43, Staged: true, Window: &pb.WindowTag{SnapshotDone: true}}
	body, metaBytes, err := transport.EncodeBatch(nil, testSchema(), meta, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := recv.apply(&flight.FlightData{DataBody: body, AppMetadata: metaBytes}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	select {
	case ing := <-ingest:
		if !ing.SnapshotDone || ing.Position != "0/10" || ing.Seq != 43 || !ing.Staged || ing.Batch != nil {
			t.Fatalf("ingest %+v, want the done marker at 0/10 as cycle 43", ing)
		}
	default:
		t.Fatal("the done marker was not ingested")
	}
}

package coordinator

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/eventlog"
	"github.com/maltzsama/urutau/internal/logging"
)

// recordingTrail captures every batch the trail writes.
type recordingTrail struct {
	mu      sync.Mutex
	kinds   []string
	batches [][]map[string]any
}

func (t *recordingTrail) EmitBatch(_ context.Context, kind string, batch []map[string]any) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.kinds = append(t.kinds, kind)
	t.batches = append(t.batches, append([]map[string]any{}, batch...))
	return nil
}

func (t *recordingTrail) records() (kinds []string, recs []map[string]any) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, b := range t.batches {
		recs = append(recs, b...)
	}
	return append([]string{}, t.kinds...), recs
}

// blockingTrail signals when a flush starts and blocks until released, so a
// test can hold the flusher still while it fills the queue.
type blockingTrail struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingTrail() *blockingTrail {
	return &blockingTrail{entered: make(chan struct{}, 1), release: make(chan struct{})}
}

func (t *blockingTrail) EmitBatch(context.Context, string, []map[string]any) error {
	t.once.Do(func() {
		select {
		case t.entered <- struct{}{}:
		default:
		}
	})
	<-t.release
	return nil
}

// The trail is the run's history: every structured record the coordinator
// logs must reach it, stamped as a kind="log" event with its own level,
// message, timestamp and attrs — while the buffer stays the live tail.
func TestLogTrailPersistsRecords(t *testing.T) {
	logger, buf, err := logging.NewBuffered("debug", "text", 100)
	if err != nil {
		t.Fatal(err)
	}
	tr := &recordingTrail{}
	lt := newLogTrail(tr, buf, slog.Default())

	logger.Info("worker attached", "worker", "w1", "table", "shop.orders")
	logger.Warn("commit slow", "table", "shop.orders", "err", "boom")
	lt.stop() // detach + drain: everything logged so far must be flushed

	kinds, recs := tr.records()
	if len(recs) != 2 {
		t.Fatalf("records = %d, want 2 (stop must drain the queue)", len(recs))
	}
	for i, k := range kinds {
		if k != eventlog.KindLog {
			t.Errorf("batch %d kind = %q, want %q", i, k, eventlog.KindLog)
		}
	}
	first, second := recs[0], recs[1]
	if first["level"] != "INFO" || first["msg"] != "worker attached" {
		t.Errorf("first = %+v", first)
	}
	if _, err := time.Parse(time.RFC3339Nano, first["ts"].(string)); err != nil {
		t.Errorf("ts %v is not RFC3339: %v", first["ts"], err)
	}
	attrs, ok := first["attrs"].(map[string]any)
	if !ok || attrs["worker"] != "w1" || attrs["table"] != "shop.orders" {
		t.Errorf("attrs = %#v", first["attrs"])
	}
	if second["level"] != "WARN" || second["msg"] != "commit slow" {
		t.Errorf("second = %+v", second)
	}
	if second["attrs"].(map[string]any)["err"] != "boom" {
		t.Errorf("err attr = %#v", second["attrs"])
	}

	// stop() detaches: nothing logged afterwards reaches a sealed trail.
	logger.Info("after stop")
	if _, recs := tr.records(); len(recs) != 2 {
		t.Fatalf("records after stop = %d, want 2", len(recs))
	}
	lt.stop() // idempotent
}

// A flusher stalled on a slow S3 must not back up into the logging path:
// the queue is bounded and the overflow is counted instead of blocking.
func TestLogTrailQueueNeverBlocks(t *testing.T) {
	_, buf, err := logging.NewBuffered("debug", "text", 8)
	if err != nil {
		t.Fatal(err)
	}
	tr := newBlockingTrail()
	lt := newLogTrail(tr, buf, slog.Default())
	rec := logging.Record{Time: time.Now(), Level: slog.LevelInfo, Message: "line"}

	// Fill one batch so the flusher enters EmitBatch and stalls there.
	for i := 0; i < logTrailBatch; i++ {
		lt.sink(rec)
	}
	select {
	case <-tr.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the flusher never started a flush")
	}

	// With the flusher stalled, the queue takes logTrailQueue more and then
	// drops; neither call may wait on the stalled flush.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < logTrailQueue+64; i++ {
			lt.sink(rec)
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("sink blocked — a stalled trail would stall logging")
	}
	if n := lt.drop.Load(); n == 0 {
		t.Fatal("no record was dropped: the bounded queue did not overflow")
	}

	close(tr.release)
	lt.stop()
}

// Records enter in order and leave in order, batched at most to the batch
// cap, so the trail reads chronologically.
func TestLogTrailBatches(t *testing.T) {
	_, buf, err := logging.NewBuffered("debug", "text", 100)
	if err != nil {
		t.Fatal(err)
	}
	tr := &recordingTrail{}
	lt := newLogTrail(tr, buf, slog.Default())
	const n = logTrailBatch*2 + 7
	for i := 0; i < n; i++ {
		lt.sink(logging.Record{Time: time.Now(), Level: slog.LevelInfo, Message: "line"})
	}
	lt.stop()

	_, recs := tr.records()
	if len(recs) != n {
		t.Fatalf("records = %d, want %d", len(recs), n)
	}
	for i, r := range recs {
		if want := "line"; r["msg"] != want {
			t.Fatalf("record %d = %#v", i, r)
		}
	}
	// The batch cap bounds one upload; n records cannot fit in fewer.
	if got := len(tr.batches); got < 3 {
		t.Fatalf("batches = %d, want >= 3 (cap %d over %d records)", got, logTrailBatch, n)
	}
}

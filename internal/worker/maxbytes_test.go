package worker

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
)

// Rows carry payloads: a flush bounded by rows alone let one commit hold
// 10,000 rows of the full profile's payloads, and the worker neared its
// memory limit copying them (#437). A batch past MaxBytes flushes on its own,
// whatever MaxRows allows.
func TestBatcherFlushesOnBytes(t *testing.T) {
	cl := &commitLog{}
	w := New(Config{MaxRows: 1_000_000, MaxBytes: 64 << 10, MaxInterval: time.Hour})
	w.Register("raw.orders", cl, dataplane.UpsertMode)
	w.SetKnownSchema("raw.orders", testSchema())
	const n, payload = 10, 20 << 10 // 20 KiB rows: about three fit 64 KiB
	ingest := make(chan Ingest, n)
	for i := int64(1); i <= n; i++ {
		ingest <- toIngest(t, rowchange.Change{
			Op: rowchange.OpInsert, Table: "raw.orders", Key: []any{i},
			After: map[string]any{"id": i, "v": strings.Repeat("x", payload)}, Position: "p1",
		})
	}
	close(ingest)
	if err := w.Run(context.Background(), ingest); err != nil {
		t.Fatalf("run: %v", err)
	}
	cl.mu.Lock()
	defer cl.mu.Unlock()
	var total int64
	for i, c := range cl.commits {
		if c.rows > 4 {
			t.Fatalf("commit %d holds %d rows of %d bytes: past the 64 KiB bound", i, c.rows, payload)
		}
		total += c.rows
	}
	if total != n || len(cl.commits) < 3 {
		t.Fatalf("%d rows in %d commits; want all %d, in several bounded commits", total, len(cl.commits), n)
	}
}

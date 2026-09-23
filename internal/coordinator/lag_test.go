package coordinator

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/maltzsama/urutau/internal/observability"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// lagHarness builds a coordinator owning raw.orders through one worker whose
// queue holds one outstanding batch (pending > 0), the state in which lag is
// meaningful.
func lagHarness() *Coordinator {
	w := &workerState{name: "w-0", queue: make(chan queuedBatch, 8)}
	w.queue <- queuedBatch{}
	c := &Coordinator{
		cfg:     Config{Spec: &spec.Spec{Tables: []spec.Table{{Source: "s", Target: "raw.orders"}}}},
		metrics: observability.New(),
		staged:  newStagedCycles(),
	}
	c.publishRouting(&routing{
		owners: map[string][]*workerState{"raw.orders": {w}},
		ranges: map[string][]source.Chunk{},
	})
	return c
}

// While a table owes work, the lag gauge must GROW between commits, not read
// ~0 right after one: it is recomputed on a timer from the last commit time,
// mirroring Tables().
func TestPublishLagGrows(t *testing.T) {
	c := lagHarness()
	c.recordTableStats("w-0", "raw.orders", 1, 0, 0, time.Now().Add(-30*time.Second))

	c.publishLag()

	got := testutil.ToFloat64(c.metrics.LagSeconds.WithLabelValues("raw.orders"))
	if got < 29 || got > 32 {
		t.Fatalf("lag = %v, want ~30 (time since last commit, while work is pending)", got)
	}
}

// A caught-up table reports zero lag. Time-since-last-commit alone would make
// an idle pipeline look infinitely behind and scale it to its ceiling (#298).
func TestPublishLagZeroWhenCaughtUp(t *testing.T) {
	c := lagHarness()
	// Drain the queue: the table no longer owes anything.
	w := &workerState{name: "w-0", queue: make(chan queuedBatch, 8)}
	c.publishRouting(&routing{
		owners: map[string][]*workerState{"raw.orders": {w}},
		ranges: map[string][]source.Chunk{},
	})
	c.recordTableStats("w-0", "raw.orders", 1, 0, 0, time.Now().Add(-30*time.Second))

	c.publishLag()

	if got := testutil.ToFloat64(c.metrics.LagSeconds.WithLabelValues("raw.orders")); got != 0 {
		t.Fatalf("lag = %v, want 0 for a caught-up table", got)
	}
}

// The backlog gauge is the direct load signal a scaler uses; it is set for
// every table, so it is visible even before the first commit.
func TestPublishLagSetsPendingBatches(t *testing.T) {
	c := lagHarness()
	c.publishLag()
	if got := testutil.ToFloat64(c.metrics.PendingBatches.WithLabelValues("raw.orders")); got != 1 {
		t.Fatalf("pending = %v, want 1 (one queued batch)", got)
	}
}

// A table that has never committed has no series: a permanent 0 would alert on
// a pipeline that simply has not started.
func TestPublishLagSkipsTablesWithoutCommit(t *testing.T) {
	c := &Coordinator{
		metrics:    observability.New(),
		tableStats: map[string]*tableStats{"raw.orders": {}},
	}
	c.publishLag()
	if n := testutil.CollectAndCount(c.metrics.LagSeconds); n != 0 {
		t.Fatalf("lag series = %d, want 0", n)
	}
}

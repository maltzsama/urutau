package coordinator

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/maltzsama/urutau/internal/observability"
	"github.com/maltzsama/urutau/spec"
)

// The lag gauge must GROW between commits, not read ~0 right after one: it is
// recomputed on a timer from the last commit time, mirroring Tables().
func TestPublishLagGrows(t *testing.T) {
	c := &Coordinator{
		cfg:     Config{Spec: &spec.Spec{Tables: []spec.Table{{Source: "s", Target: "raw.orders"}}}},
		metrics: observability.New(),
	}
	c.recordTableStats("w-0", "raw.orders", 1, 0, 0, time.Now().Add(-30*time.Second))

	c.publishLag()

	got := testutil.ToFloat64(c.metrics.LagSeconds.WithLabelValues("raw.orders"))
	if got < 29 || got > 32 {
		t.Fatalf("lag = %v, want ~30 (time since last commit)", got)
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

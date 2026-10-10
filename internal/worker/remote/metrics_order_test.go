package remote

import (
	"testing"

	"github.com/maltzsama/urutau/internal/worker"
)

// #267: metricsSnapshot must emit tables in a deterministic (sorted) order,
// not the random map-iteration order.
func TestMetricsSnapshotTableOrderStable(t *testing.T) {
	w := worker.New(worker.Config{})
	m := w.Metrics()
	for _, tbl := range []string{"c", "a", "b"} {
		m.CommitFailures.WithLabelValues(tbl).Inc()
	}

	rep := metricsSnapshot(w)
	if len(rep.Tables) != 3 {
		t.Fatalf("tables = %d, want 3", len(rep.Tables))
	}
	for i, want := range []string{"a", "b", "c"} {
		if rep.Tables[i].Table != want {
			t.Fatalf("table[%d] = %q, want %q (sorted)", i, rep.Tables[i].Table, want)
		}
	}
}

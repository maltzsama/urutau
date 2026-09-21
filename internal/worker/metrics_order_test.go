package worker

import (
	"testing"

	"github.com/maltzsama/urutau/internal/observability"
)

// #267: MetricsSnapshot must emit tables in a deterministic (sorted) order,
// not the random map-iteration order.
func TestMetricsSnapshotTableOrderStable(t *testing.T) {
	m := observability.New()
	for _, tbl := range []string{"c", "a", "b"} {
		m.CommitFailures.WithLabelValues(tbl).Inc()
	}
	w := New(Config{})
	w.metrics = m

	rep := w.MetricsSnapshot()
	if len(rep.Tables) != 3 {
		t.Fatalf("tables = %d, want 3", len(rep.Tables))
	}
	for i, want := range []string{"a", "b", "c"} {
		if rep.Tables[i].Table != want {
			t.Fatalf("table[%d] = %q, want %q (sorted)", i, rep.Tables[i].Table, want)
		}
	}
}

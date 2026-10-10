package pods

import (
	"testing"

	"github.com/maltzsama/urutau/internal/faultinject"
)

// The randomized matrix draws from the complete boundary set: every fault
// point the engine defines has an end-to-end case. A new point without one
// fails here, not silently outside the matrix.
func TestBoundarySetCoversEveryFaultPoint(t *testing.T) {
	covered := map[faultinject.Point]bool{}
	for _, d := range boundarySet() {
		covered[d.bc.point] = true
	}
	// The ClickHouse and Couchbase boundaries are exercised by their own
	// deterministic tests (MySQL→ClickHouse, MySQL→Couchbase), not the
	// randomized Iceberg matrix — the randomized runner is Trino/Iceberg-
	// shaped — so fold them in here: every fault point must still have an
	// end-to-end case.
	for _, bc := range clickhouseBoundaries {
		covered[bc.point] = true
	}
	for _, bc := range couchbaseBoundaries {
		covered[bc.point] = true
	}
	for _, bc := range couchbaseFastBoundaries {
		covered[bc.point] = true
	}
	for _, p := range faultinject.Points {
		if !covered[p] {
			t.Errorf("fault point %s has no end-to-end case in the boundary set", p)
		}
	}
}

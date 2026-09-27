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
	for _, p := range faultinject.Points {
		if !covered[p] {
			t.Errorf("fault point %s has no end-to-end case in the boundary set", p)
		}
	}
}

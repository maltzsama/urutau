package coordinator

import (
	"github.com/maltzsama/urutau/position"

	"github.com/maltzsama/urutau/internal/inflight"
)

// commitEverything is a fake worker committing everything it holds: its
// stream batches through pos, and every window, whose marker it acks by id.
func commitEverything(idx *inflight.PositionIndex, table string, pos position.Position) {
	idx.Truncate(table, pos)
	var markers []uint64
	for _, h := range idx.Held() {
		if h.Marker {
			markers = append(markers, h.ID)
		}
	}
	for _, id := range markers {
		idx.ReleaseMarker(id)
	}
}

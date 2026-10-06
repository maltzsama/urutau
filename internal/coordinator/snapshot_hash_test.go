package coordinator

import (
	"testing"

	"github.com/maltzsama/urutau/internal/snapshot"
	"github.com/maltzsama/urutau/source"
)

// resumableProgress must resume only progress the fan-out path wrote. The
// range path stamps propSnapshotPartitions and packs its pending ids, which
// the fan-out would misread and skip every chunk.
func TestResumableProgress(t *testing.T) {
	bounds := [][]any{{int64(1)}, {int64(2)}}

	if sp := resumableProgress(map[string]string{}); sp != nil {
		t.Fatalf("empty props resumed: %+v", sp)
	}

	own := snapshot.EncodeSnapshotProgress(&snapshot.SnapshotProgress{
		State: snapshot.StateInProgress, Bounds: bounds, Pending: []uint32{1},
	})
	if sp := resumableProgress(own); sp == nil || sp.State != snapshot.StateInProgress {
		t.Fatalf("the fan-out's own progress was not resumed: %+v", sp)
	}

	stale := snapshot.EncodeSnapshotProgress(&snapshot.SnapshotProgress{
		State: snapshot.StateInProgress, Bounds: bounds, Pending: []uint32{chunkRef(1, 0)},
	})
	stale[propSnapshotPartitions] = partitionLayout([]source.Chunk{{}, {}})
	if sp := resumableProgress(stale); sp != nil {
		t.Fatalf("range-path progress was resumed: %+v", sp)
	}

	done := snapshot.EncodeSnapshotProgress(&snapshot.SnapshotProgress{State: snapshot.StateComplete})
	if sp := resumableProgress(done); sp != nil {
		t.Fatalf("completed snapshot resumed: %+v", sp)
	}
}

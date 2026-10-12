package snapshot

import (
	"testing"

	"github.com/maltzsama/urutau/source"
)

func TestRunningTracksTablesAndTheirTodo(t *testing.T) {
	var r Running
	if r.Is("raw.a") || r.Todo("raw.a") != nil {
		t.Fatal("a table is snapshotting before any began")
	}
	r.SetTodo("raw.a", nil) // clearing nothing is not an error

	r.Begin("raw.a")
	r.SetTodo("raw.a", map[uint32]bool{3: true})
	if !r.Is("raw.a") || r.Is("raw.b") {
		t.Fatal("Begin did not mark raw.a alone")
	}
	if todo := r.Todo("raw.a"); len(todo) != 1 || !todo[3] {
		t.Fatalf("todo = %v, want chunk 3", todo)
	}
	r.SetTodo("raw.a", nil)
	r.End("raw.a")
	if r.Is("raw.a") || r.Todo("raw.a") != nil {
		t.Fatal("the table is still snapshotting after End")
	}
}

// resumableProgress must resume only progress the fan-out path wrote. The
// range path stamps PropSnapshotPartitions and packs its pending ids, which
// the fan-out would misread and skip every chunk.
func TestFanoutProgress(t *testing.T) {
	bounds := [][]any{{int64(1)}, {int64(2)}}
	mustEncode := func(sp *SnapshotProgress) map[string]string {
		t.Helper()
		props, err := EncodeSnapshotProgress(sp)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		return props
	}

	if sp := FanoutProgress(map[string]string{}); sp != nil {
		t.Fatalf("empty props resumed: %+v", sp)
	}

	own := mustEncode(&SnapshotProgress{
		State: StateInProgress, Bounds: bounds, Pending: []uint32{1},
	})
	if sp := FanoutProgress(own); sp == nil || sp.State != StateInProgress {
		t.Fatalf("the fan-out's own progress was not resumed: %+v", sp)
	}

	stale := mustEncode(&SnapshotProgress{
		State: StateInProgress, Bounds: bounds, Pending: []uint32{ChunkRef(1, 0)},
	})
	stale[PropSnapshotPartitions] = PartitionLayout([]source.Chunk{{}, {}})
	if sp := FanoutProgress(stale); sp != nil {
		t.Fatalf("range-path progress was resumed: %+v", sp)
	}

	done := mustEncode(&SnapshotProgress{State: StateComplete})
	if sp := FanoutProgress(done); sp != nil {
		t.Fatalf("completed snapshot resumed: %+v", sp)
	}
}

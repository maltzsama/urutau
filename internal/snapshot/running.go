package snapshot

import "sync"

// PropSnapshotPartitions records the partition ranges a table's in-progress
// snapshot was chunked for: its pending chunk ids index chunks clipped to
// those ranges, so progress recorded under other ranges cannot be resumed
// (issue #461).
const PropSnapshotPartitions = "cdc.snapshot.partitions"

// FanoutProgress returns the snapshot progress a fan-out snapshot may resume
// from, or nil.
//
// The range path persists pending chunk ids packed as partition<<20 | index
// (ChunkRef) and stamps PropSnapshotPartitions; the fan-out uses plain chunk
// indices, so resuming a pack-format list would make every id out of range,
// skip every chunk, and complete a snapshot that copied nothing. Presence of
// PropSnapshotPartitions is the discriminator: it is only ever written by the
// range path, so its absence means the progress is the fan-out's own.
func FanoutProgress(props map[string]string) *SnapshotProgress {
	if props[PropSnapshotPartitions] != "" {
		return nil
	}
	sp, err := ReadSnapshotProgress(props)
	if err != nil || !Resumable(sp) {
		return nil
	}
	return sp
}

// Running is the tables whose snapshot is in progress, and the chunks each
// has still to do. The zero value is ready to use.
type Running struct {
	mu     sync.Mutex
	tables map[string]bool
	todo   map[string]map[uint32]bool
}

// Begin marks target's snapshot running; End clears it. A re-slice must not
// flip that table's owner layout mid-snapshot: the fan-out captured the
// owners at start and the live stream routes by the current layout, so a
// flip would send a key's snapshot row and its live row to different workers.
func (r *Running) Begin(target string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.tables == nil {
		r.tables = map[string]bool{}
	}
	r.tables[target] = true
}

// End marks target's snapshot over.
func (r *Running) End(target string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.tables, target)
}

// Is reports whether target's snapshot is in progress.
func (r *Running) Is(target string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.tables[target]
}

// SetTodo records the chunks still to do of the table being snapshotted; nil
// clears it. The map is read-only once set.
func (r *Running) SetTodo(target string, todo map[uint32]bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if todo == nil {
		delete(r.todo, target)
		return
	}
	if r.todo == nil {
		r.todo = map[string]map[uint32]bool{}
	}
	r.todo[target] = todo
}

// Todo returns the table's chunks still to do, or nil (every chunk).
func (r *Running) Todo(target string) map[uint32]bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.todo[target]
}

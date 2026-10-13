package snapshot

import "sync"

// Marker is a queued Closes marker: its in-flight batch id, and the table and
// window it closes.
type Marker struct {
	ID       uint64
	Target   string
	Window   uint64
	ChunkRef uint32
	HighKey  []any // the window's last row's PK tuple (the redo cursor, #646)
}

// cursorKey names one chunk of one table. chunkRef alone is partition and
// chunk index, which every table shares: a cursor keyed only by it could be
// taken by another table's redo of the same partition and index, and that
// table would resume its chunk from the wrong key, skipping rows.
type cursorKey struct {
	target   string
	chunkRef uint32
}

// Markers tracks each worker's queued Closes markers: the ones still
// uncommitted pace the chunks, the ones a lost worker held say where its redo
// starts, and the last committed one of a chunk is the cursor the redo
// resumes from. The zero value is ready to use.
type Markers struct {
	mu     sync.Mutex
	queued map[string][]Marker
	// lost are the chunk windows each lost worker had not committed, taken
	// at the loss.
	lost map[string][]Marker
	// cursor is, per table and chunkRef, the last acked window's high key
	// (#646).
	cursor map[cursorKey][]any
}

// Note records a queued Closes marker of worker's snapshot.
func (m *Markers) Note(worker string, mk Marker) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.queued == nil {
		m.queued = map[string][]Marker{}
	}
	m.queued[worker] = append(m.queued[worker], mk)
}

// Held drops worker's committed markers and returns the rest, oldest first.
// holds reports whether a marker id is still in the worker's in-flight index:
// a marker leaves it once the worker has committed it.
func (m *Markers) Held(worker string, holds func(id uint64) bool) []Marker {
	m.mu.Lock()
	defer m.mu.Unlock()
	kept := m.queued[worker][:0]
	for _, mk := range m.queued[worker] {
		if holds(mk.ID) {
			kept = append(kept, mk)
		}
	}
	if len(kept) == 0 {
		delete(m.queued, worker)
		return nil
	}
	clear(m.queued[worker][len(kept):])
	m.queued[worker] = kept
	return append([]Marker(nil), kept...)
}

// RecordCursor stores a chunk's resume cursor — the high key of the last
// window whose Closes marker this ack committed — so a mid-chunk redo resumes
// the chunk from it instead of re-emitting already-committed windows (#646).
// A window whose Closes marker was never sent has no marker here, so it is
// never mistaken for committed.
func (m *Markers) RecordCursor(worker string, id uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, mk := range m.queued[worker] {
		if mk.ID == id && mk.HighKey != nil {
			if m.cursor == nil {
				m.cursor = map[cursorKey][]any{}
			}
			m.cursor[cursorKey{mk.Target, mk.ChunkRef}] = mk.HighKey
			return
		}
	}
}

// TakeCursor returns and forgets the resume cursor of one of target's chunks,
// or nil.
func (m *Markers) TakeCursor(target string, chunkRef uint32) []any {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := cursorKey{target, chunkRef}
	cur := m.cursor[k]
	delete(m.cursor, k)
	return cur
}

// ForgetCursors drops every cursor of target. Called when its snapshot
// finishes: no chunk of it will be redone, and a cursor nothing takes would
// otherwise stay for the whole run.
func (m *Markers) ForgetCursors(target string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k := range m.cursor {
		if k.target == target {
			delete(m.cursor, k)
		}
	}
}

// Lose records the chunk windows a lost worker had not committed.
func (m *Markers) Lose(worker string, held []Marker) {
	if len(held) == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.lost == nil {
		m.lost = map[string][]Marker{}
	}
	m.lost[worker] = append(m.lost[worker], held...)
}

// Lost returns the chunk windows worker lost and no redo has taken yet.
func (m *Markers) Lost(worker string) []Marker {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]Marker(nil), m.lost[worker]...)
}

// TakeLost returns and forgets the chunk windows worker lost on target. A
// marker of another table stays for that table's own redo: today a worker
// serves one table, so there is none, but taking them all here and skipping
// the others would drop them for good the day that changes.
func (m *Markers) TakeLost(worker, target string) []Marker {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out, kept []Marker
	for _, mk := range m.lost[worker] {
		if mk.Target == target {
			out = append(out, mk)
		} else {
			kept = append(kept, mk)
		}
	}
	if len(kept) == 0 {
		delete(m.lost, worker)
	} else {
		m.lost[worker] = kept
	}
	return out
}

// RedoFrom is where a partition's snapshot resumes after its worker was lost
// at chunk next: the first chunk of this partition whose Closes marker the
// worker had not committed, whose rows died with its window. windows maps a
// window to its chunk's index. It also returns that chunk's resume cursor —
// the high key of the last window whose Closes marker the worker DID commit —
// so the redo resumes the chunk from there instead of re-emitting committed
// windows (issue #646). A chunk with no committed window returns nil, and is
// re-read from its start.
func (m *Markers) RedoFrom(worker, target string, windows map[uint64]int, partition, next int) (int, []any) {
	from := next
	for _, mk := range m.TakeLost(worker, target) {
		if i, ok := windows[mk.Window]; ok && i < from {
			from = i
		}
	}
	return from, m.TakeCursor(target, ChunkRef(partition, from))
}

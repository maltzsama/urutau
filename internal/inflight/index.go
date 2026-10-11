package inflight

import (
	"sync"

	"github.com/maltzsama/urutau/position"
)

// Batch is one batch's flight-window metadata — position only; the
// data itself is never retained (design §5.4.2).
type Batch struct {
	ID    uint64
	Table string
	High  position.Position // nil for position-less snapshot rows
	Bytes int64
	// oversized marks a batch larger than the whole flow budget, so its ack
	// can release the budget's single oversized slot.
	Oversized bool
	// marker is a snapshot window's Closes Marker: it leaves only on the ack
	// of its own id, once the window's rows are committed, never on a
	// position ack a batch of the same or a later position sent (#468).
	Marker bool
}

// PositionIndex tracks each worker's unacked batches so an Ack can release
// the budget by table position (design §6). Removal is by the head only:
// batches leave in order, and a batch of an unconfirmed table blocks the
// removal of the ones behind it — conservative and correct.
type PositionIndex struct {
	mu    sync.Mutex
	head  []Batch
	acked map[string]position.Position
	runID string
	dirty bool   // set on add/truncate; cleared by MarkClean
	gen   uint64 // bumped on every mutation; MarkClean only clears its own gen
}

func NewPositionIndex(runID string) *PositionIndex {
	return &PositionIndex{acked: map[string]position.Position{}, runID: runID}
}

// InFlight reports the number of unacked batches (delivered, not yet acked).
func (p *PositionIndex) InFlight() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.head)
}

func (p *PositionIndex) Add(b Batch) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.head = append(p.head, b)
	p.dirty = true
	p.gen++
}

// remove drops the entry for id if present and reports whether it was found:
// it undoes an add for a batch whose send was cancelled before the worker
// ever saw it (issue #559).
func (p *PositionIndex) Remove(id uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.head {
		if p.head[i].ID == id {
			p.head = append(p.head[:i], p.head[i+1:]...)
			p.dirty = true
			p.gen++
			return true
		}
	}
	return false
}

// Dirty reports whether the manifest changed since the last MarkClean.
func (p *PositionIndex) Dirty() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.dirty
}

// MarkClean clears the dirty flag after a successful checkpoint write, but
// only if the index has not changed since gen was taken by Manifest. Otherwise
// an add/truncate racing the write would have its dirty flag wrongly cleared
// and its change skipped until the next unrelated mutation (issue #215).
func (p *PositionIndex) MarkClean(gen uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.gen == gen {
		p.dirty = false
	}
}

// Manifest snapshots the acked positions and the in-flight batch-id range
// for the async S3 Checkpoint (design §6) — a small file; the data is never
// persisted.
func (p *PositionIndex) Manifest() (PositionManifest, uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	m := PositionManifest{
		Acked: make(map[string]string, len(p.acked)),
		RunID: p.runID,
	}
	for t, pos := range p.acked {
		m.Acked[t] = pos.String()
	}
	if len(p.head) > 0 {
		m.FirstBatchID = p.head[0].ID
		m.LastBatchID = p.head[len(p.head)-1].ID
	}
	return m, p.gen
}

// PositionManifest is the on-disk checkpoint: per-table acked positions and
// the in-flight batch id range. Written asynchronously; losing it never
// blocks recovery (the Iceberg table property is the source of truth).
type PositionManifest struct {
	Acked        map[string]string `json:"acked"`
	RunID        string            `json:"run_id"`
	FirstBatchID uint64            `json:"first_batch_id"`
	LastBatchID  uint64            `json:"last_batch_id"`
}

// truncate records an Ack and pops every head batch the commit covers: a
// positioned batch pops once its table acked at or beyond its high
// position; a position-less batch (snapshot window rows) pops once its
// table has any ack. It returns the bytes released, whether an oversized
// batch was among them, and the ids of the popped batches (so the worker's
// sent list can be pruned — issue #235).
func (p *PositionIndex) Truncate(table string, pos position.Position) (freed int64, freedOversized bool, popped []uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if cur, ok := p.acked[table]; !ok || advances(pos, cur) {
		p.acked[table] = pos
		p.dirty = true
		p.gen++
	}
	return p.popCoveredLocked()
}

// popCoveredLocked pops the head while its batch's table is acked through
// it, stopping at a Closes marker, which only releaseMarker removes. Caller
// holds p.mu.
func (p *PositionIndex) popCoveredLocked() (freed int64, freedOversized bool, popped []uint64) {
	for len(p.head) > 0 {
		h := p.head[0]
		if h.Marker {
			break
		}
		if h.High == nil {
			if _, ok := p.acked[h.Table]; !ok {
				break
			}
		} else if cur, ok := p.acked[h.Table]; !ok || !covered(h.High, cur) {
			break
		}
		freed += h.Bytes
		if h.Oversized {
			freedOversized = true
		}
		popped = append(popped, h.ID)
		p.head = p.head[1:]
		p.dirty = true
		p.gen++
	}
	return freed, freedOversized, popped
}

// releaseMarker removes a Closes marker on the ack of its id — the worker
// committed the window's rows — and pops the batches behind it that their
// table's acks already cover.
func (p *PositionIndex) ReleaseMarker(id uint64) (freed int64, freedOversized bool, popped []uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, h := range p.head {
		if h.ID == id && h.Marker {
			freed, freedOversized = h.Bytes, h.Oversized
			popped = []uint64{h.ID}
			p.head = append(p.head[:i:i], p.head[i+1:]...)
			p.dirty = true
			p.gen++
			break
		}
	}
	f, o, pp := p.popCoveredLocked()
	return freed + f, freedOversized || o, append(popped, pp...)
}

// advances reports whether pos is provably strictly greater than cur. An
// Incomparable comparison (opaque plugin offsets, contract §8.2) does not
// advance the confirmed point — the conservative choice.
func advances(pos, cur position.Position) bool {
	c := pos.Compare(cur)
	return c != position.Incomparable && c > 0
}

// covered reports whether pos is provably at or before cur. Incomparable
// means not covered — a head batch stays queued until coverage is certain.
func covered(pos, cur position.Position) bool {
	c := pos.Compare(cur)
	return c != position.Incomparable && c <= 0
}

// holds reports whether batch id is still unacked.
func (p *PositionIndex) Holds(id uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, h := range p.head {
		if h.ID == id {
			return true
		}
	}
	return false
}

// minHeldID returns the oldest unacked batch id, or 0.
func (p *PositionIndex) MinHeldID() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	var min uint64
	for _, h := range p.head {
		if min == 0 || h.ID < min {
			min = h.ID
		}
	}
	return min
}

// maxHeldID returns the newest unacked batch id, or 0.
func (p *PositionIndex) MaxHeldID() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	var max uint64
	for _, h := range p.head {
		if h.ID > max {
			max = h.ID
		}
	}
	return max
}

// Held returns a copy of the unacked batches, oldest first.
func (p *PositionIndex) Held() []Batch {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]Batch(nil), p.head...)
}

// Clear drops every unacked batch, as an ack of each would. Tests use it to
// play a worker that committed everything it held.
func (p *PositionIndex) Clear() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.head = nil
}

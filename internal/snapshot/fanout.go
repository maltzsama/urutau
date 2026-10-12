package snapshot

import "sync"

// A fan-out snapshot's window rows are never redelivered: a worker that
// restarts gets the Closes marker of a window it never held and says so on
// the cycle's delivery (StagedBatch.window_lost). The other owners held their
// share, and their delivery of the same cycle carries the snapshot progress
// with the chunk removed — committed, the chunk is done with the lost owner's
// rows missing, and so is every chunk queued behind it, whose progress was
// written before the loss and omits it too (#829).
//
// So a window cycle an owner lost does not commit, nor does any later window
// cycle of the same pass over the table's chunks. The table's durable
// progress stays at the first lost chunk, and the fan-out reads again from
// there once the pass has drained.

// fanoutPass is one pass of a table's fan-out snapshot over its chunks.
type fanoutPass struct {
	// chunks are the pass's window cycles not yet committed or dropped, by
	// cycle seq.
	chunks map[uint64]uint32
	// lost is set by the first window cycle an owner lost; from is its chunk.
	lost bool
	from uint32
}

// FanoutPasses tracks the fan-out snapshot pass each table is in.
type FanoutPasses struct {
	mu     sync.Mutex
	tables map[string]*fanoutPass
}

// Begin starts tracking target's pass.
func (f *FanoutPasses) Begin(target string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.tables == nil {
		f.tables = map[string]*fanoutPass{}
	}
	f.tables[target] = &fanoutPass{chunks: map[uint64]uint32{}}
}

// End forgets target's pass.
func (f *FanoutPasses) End(target string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.tables, target)
}

// Note records the window cycle queued for one of target's chunks.
func (f *FanoutPasses) Note(target string, seq uint64, chunk uint32) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if p := f.tables[target]; p != nil {
		p.chunks[seq] = chunk
	}
}

// Resolve takes a complete window cycle out of target's pass and reports
// whether it must be dropped instead of committed: an owner lost it, or it
// follows one an owner lost. A cycle of no fan-out pass commits.
func (f *FanoutPasses) Resolve(target string, seq uint64, lost bool) (drop bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p := f.tables[target]
	if p == nil {
		return false
	}
	chunk, ok := p.chunks[seq]
	if !ok {
		return false
	}
	delete(p.chunks, seq)
	if lost && !p.lost {
		p.lost, p.from = true, chunk
	}
	return p.lost
}

// Drained reports whether every window cycle of target's pass is resolved
// and, when so, the first chunk an owner lost. It leaves the pass clean for
// the next one.
func (f *FanoutPasses) Drained(target string) (done bool, from uint32, lost bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p := f.tables[target]
	if p == nil {
		return true, 0, false
	}
	if len(p.chunks) > 0 {
		return false, 0, false
	}
	from, lost = p.from, p.lost
	p.from, p.lost = 0, false
	return true, from, lost
}

// fanoutDrainPoll is how often a finished pass re-checks its window cycles.

// PendingFrom is the part of a pass's chunks, in the order the pass takes
// them, from chunk on: what is still to do when chunk was lost.
func PendingFrom(plan []uint32, chunk uint32) []uint32 {
	for i, id := range plan {
		if id == chunk {
			return append([]uint32(nil), plan[i:]...)
		}
	}
	return nil
}

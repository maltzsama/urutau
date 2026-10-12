package coordinator

import (
	"context"
	"sync"
	"time"
)

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

// fanoutPasses tracks the fan-out snapshot pass each table is in.
type fanoutPasses struct {
	mu     sync.Mutex
	tables map[string]*fanoutPass
}

// begin starts tracking target's pass.
func (f *fanoutPasses) begin(target string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.tables == nil {
		f.tables = map[string]*fanoutPass{}
	}
	f.tables[target] = &fanoutPass{chunks: map[uint64]uint32{}}
}

// end forgets target's pass.
func (f *fanoutPasses) end(target string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.tables, target)
}

// note records the window cycle queued for one of target's chunks.
func (f *fanoutPasses) note(target string, seq uint64, chunk uint32) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if p := f.tables[target]; p != nil {
		p.chunks[seq] = chunk
	}
}

// resolve takes a complete window cycle out of target's pass and reports
// whether it must be dropped instead of committed: an owner lost it, or it
// follows one an owner lost. A cycle of no fan-out pass commits.
func (f *fanoutPasses) resolve(target string, seq uint64, lost bool) (drop bool) {
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

// drained reports whether every window cycle of target's pass is resolved
// and, when so, the first chunk an owner lost. It leaves the pass clean for
// the next one.
func (f *fanoutPasses) drained(target string) (done bool, from uint32, lost bool) {
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
const fanoutDrainPoll = 20 * time.Millisecond

// awaitFanoutPass returns once every window cycle of target's pass is
// committed or dropped, with the first chunk an owner lost, if any. A cycle
// waits for every owner's delivery; an owner that never comes back ends the
// run through the supervisor, which cancels ctx.
func (c *Coordinator) awaitFanoutPass(ctx context.Context, target string) (from uint32, lost bool, err error) {
	poll := time.NewTicker(fanoutDrainPoll)
	defer poll.Stop()
	for {
		if done, from, lost := c.fanout.drained(target); done {
			return from, lost, nil
		}
		select {
		case <-ctx.Done():
			return 0, false, ctx.Err()
		case <-poll.C:
		}
	}
}

// pendingFrom is the part of a pass's chunks, in the order the pass takes
// them, from chunk on: what is still to do when chunk was lost.
func pendingFrom(plan []uint32, chunk uint32) []uint32 {
	for i, id := range plan {
		if id == chunk {
			return append([]uint32(nil), plan[i:]...)
		}
	}
	return nil
}

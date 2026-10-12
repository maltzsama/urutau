package inflight

import "sync"

// Indexes holds each worker's position index. A reader takes the lock alone;
// a scale-out or its rollback writes the map at runtime, so every lookup goes
// through it (issue #312). The zero value is ready to use.
type Indexes struct {
	mu       sync.RWMutex
	byWorker map[string]*PositionIndex
}

// Of returns a worker's position index, or nil.
func (x *Indexes) Of(worker string) *PositionIndex {
	x.mu.RLock()
	defer x.mu.RUnlock()
	return x.byWorker[worker]
}

// Set installs a worker's position index.
func (x *Indexes) Set(worker string, idx *PositionIndex) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.byWorker == nil {
		x.byWorker = map[string]*PositionIndex{}
	}
	x.byWorker[worker] = idx
}

// Delete removes a worker's position index.
func (x *Indexes) Delete(worker string) {
	x.mu.Lock()
	defer x.mu.Unlock()
	delete(x.byWorker, worker)
}

// Snapshot copies the indexes, for a reader that iterates them (the
// checkpoint runner) while a scale-out may be writing.
func (x *Indexes) Snapshot() map[string]*PositionIndex {
	x.mu.RLock()
	defer x.mu.RUnlock()
	out := make(map[string]*PositionIndex, len(x.byWorker))
	for k, v := range x.byWorker {
		out[k] = v
	}
	return out
}

// InFlight reports how many batches the worker has been delivered but has not
// acked; 0 for a worker with no index.
func (x *Indexes) InFlight(worker string) int {
	if idx := x.Of(worker); idx != nil {
		return idx.InFlight()
	}
	return 0
}

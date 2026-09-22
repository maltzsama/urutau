package coordinator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/eventlog"
	"github.com/maltzsama/urutau/source"
)

// routing is the immutable snapshot of one table's partition layout: the
// owners in partition order and the key ranges they own, where
// owners[i] owns ranges[i]. It is never mutated after publication — a
// re-slice builds a new value and swaps the pointer, so a reader that
// loaded it keeps routing a whole batch by one consistent layout.
type routing struct {
	owners map[string][]*workerState
	ranges map[string][]source.Chunk
}

// ownersOf returns the owners of target and whether the table is routed.
func (r *routing) ownersOf(target string) ([]*workerState, bool) {
	if r == nil {
		return nil, false
	}
	o, ok := r.owners[target]
	return o, ok
}

// rangesOf returns the key ranges of target, owners[i] owning ranges[i].
func (r *routing) rangesOf(target string) []source.Chunk {
	if r == nil {
		return nil
	}
	return r.ranges[target]
}

// clone copies the two maps one level deep. The slices are shared, never
// appended to in place: a re-slice replaces a table's slice wholesale, so a
// reader holding the previous snapshot still sees the previous slice.
func (r *routing) clone() *routing {
	out := &routing{
		owners: make(map[string][]*workerState, len(r.owners)),
		ranges: make(map[string][]source.Chunk, len(r.ranges)),
	}
	for k, v := range r.owners {
		out.owners[k] = v
	}
	for k, v := range r.ranges {
		out.ranges[k] = v
	}
	return out
}

// loadRouting returns the current routing snapshot, never nil.
func (c *Coordinator) loadRouting() *routing {
	if r := c.routing.Load(); r != nil {
		return r
	}
	return &routing{owners: map[string][]*workerState{}, ranges: map[string][]source.Chunk{}}
}

// publishRouting swaps in a new routing snapshot.
func (c *Coordinator) publishRouting(r *routing) {
	c.routing.Store(r)
}

// repartitioner serializes re-slices: two concurrent scale events on the
// same table would each read-modify-write the routing snapshot and one
// would be lost.
type repartitioner struct {
	mu sync.Mutex
}

// ScaleTable re-slices target across n partitions, adding or removing
// owners so the table ends up with exactly n of them. It is the entry
// point a scaler (KEDA today, an operator by hand) drives.
//
// The sequence is ordered so no key is ever routed to an owner that cannot
// commit it, and no cycle is split across two layouts:
//
//  1. Resolve the new ranges from the source's chunker.
//  2. Register any new owner (queue, ticket, index) so its Hello is
//     accepted the moment its pod starts.
//  3. Seed the new owner's confirmed position, so confirmedPosition does
//     not stall the whole pipeline on an owner that has committed nothing.
//  4. Drain the affected table's open staged cycles — only that table.
//  5. Swap the routing snapshot atomically.
//  6. Drain and drop any owner the new layout removed.
func (c *Coordinator) ScaleTable(ctx context.Context, target string, n int) error {
	if n < 1 {
		return fmt.Errorf("coordinator: scale %s: worker count must be >= 1, got %d", target, n)
	}
	c.repart.mu.Lock()
	defer c.repart.mu.Unlock()

	cur := c.loadRouting()
	owners, ok := cur.ownersOf(target)
	if !ok {
		return fmt.Errorf("coordinator: scale %s: no such routed table", target)
	}
	if len(owners) == n {
		return nil
	}
	if max := c.maxWorkersFor(target); n > max {
		return fmt.Errorf("coordinator: scale %s: %d exceeds maxWorkers %d", target, n, max)
	}

	ref, ok := c.tableRef(target)
	if !ok {
		return fmt.Errorf("coordinator: scale %s: no table ref", target)
	}
	if n > 1 && len(ref.PrimaryKey) == 0 {
		return fmt.Errorf("coordinator: scale %s: partitioning requires a primary key", target)
	}

	ranges, err := c.rangesFor(ctx, target, n)
	if err != nil {
		return fmt.Errorf("coordinator: scale %s: %w", target, err)
	}
	if len(ranges) != n {
		return fmt.Errorf("coordinator: scale %s: resolved %d ranges for %d partitions", target, len(ranges), n)
	}

	next, removed, err := c.reslicedOwners(target, owners, n, ref)
	if err != nil {
		return fmt.Errorf("coordinator: scale %s: %w", target, err)
	}

	// The new owners must be able to resume: an owner with no confirmed
	// position pins confirmedPosition to nil and stalls source retention
	// for the WHOLE pipeline, not just this table. A scale-in adds none.
	if len(next) > len(owners) {
		c.seedConfirmed(next[len(owners):])
	}

	// Per-table barrier: a staged cycle groups one binlog batch's
	// sub-batches and must commit atomically. Flipping mid-cycle would
	// leave a cycle expecting an owner the new layout no longer routes to.
	if err := c.drainStagedCycles(ctx, target); err != nil {
		return fmt.Errorf("coordinator: scale %s: drain cycles: %w", target, err)
	}

	snap := cur.clone()
	snap.owners[target] = next
	snap.ranges[target] = ranges
	c.publishRouting(snap)

	c.emitScale(target, len(owners), n)

	// Only after the flip: a removed owner still holds in-flight batches
	// for keys the new layout has reassigned. Draining before the flip
	// would race the reader still routing to it.
	for _, w := range removed {
		c.retireOwner(ctx, w)
	}
	c.pushDashState()
	return nil
}

// reslicedOwners returns the owner slice for n partitions, keeping the
// existing owners in place (minimal remap: only the boundary moves) and
// creating the ones a scale-out adds. It also returns the owners a
// scale-in drops.
func (c *Coordinator) reslicedOwners(target string, cur []*workerState, n int, ref source.TableRef) (next, removed []*workerState, err error) {
	if n <= len(cur) {
		return cur[:n:n], cur[n:], nil
	}
	next = make([]*workerState, len(cur), n)
	copy(next, cur)
	for p := len(cur); p < n; p++ {
		w, err := c.registerOwner(partitionName(c.cfg.Spec.Pipeline, target, p), ref)
		if err != nil {
			return nil, nil, err
		}
		next = append(next, w)
	}
	return next, nil, nil
}

// registerOwner creates a worker group and publishes it in the registry, so
// Session accepts the pod's Hello the moment it connects. A name that is
// already registered is reused: a pod restart must not orphan its queue.
func (c *Coordinator) registerOwner(name string, ref source.TableRef) (*workerState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if w, ok := c.workers[name]; ok {
		return w, nil
	}
	w := &workerState{
		name:  name,
		refs:  []source.TableRef{ref},
		queue: make(chan queuedBatch, workerQueueCap),
	}
	// A 128-bit ticket colliding is ~0, but the queue lookup is keyed by
	// it: a collision would silently orphan a worker's stream.
	for {
		ticket := randTicket()
		if _, taken := c.byTicket[string(ticket)]; taken {
			continue
		}
		w.ticket = ticket
		c.byTicket[string(ticket)] = w
		break
	}
	c.workers[name] = w
	c.index[name] = newPositionIndex(c.runID)
	return w, nil
}

// seedConfirmed gives each owner the pipeline's current confirmed position
// as its baseline. Without it confirmedPosition returns nil — it holds
// retention back for ANY registered owner with no position — and the source
// slot stops advancing for every table, not only the scaled one.
func (c *Coordinator) seedConfirmed(added []*workerState) {
	if len(added) == 0 {
		return
	}
	base := c.confirmedPosition()
	if base == nil {
		return // nothing durably committed yet; boot baseline already holds
	}
	c.confirmedMu.Lock()
	defer c.confirmedMu.Unlock()
	for _, w := range added {
		if _, ok := c.confirmed[w.name]; !ok {
			c.confirmed[w.name] = base
		}
	}
}

// drainStagedCycles blocks until target has no open staged cycle, so the
// routing flip never splits one cycle across two layouts. Only this table
// is held: the shared reader keeps streaming every other table.
func (c *Coordinator) drainStagedCycles(ctx context.Context, target string) error {
	if !c.isStagedTable(target) {
		return nil
	}
	deadline := time.NewTimer(c.drainTimeout())
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if c.staged.openFor(core.TableRef{Target: target}) == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("staged cycles still open after %s", c.drainTimeout())
		case <-tick.C:
		}
	}
}

// retireOwner drains a removed owner's in-flight batches, then detaches it.
// The drain is bounded: an owner that never drains is forced out and its
// range replayed by the inheriting owner, which is safe because the writes
// are idempotent upserts.
func (c *Coordinator) retireOwner(ctx context.Context, w *workerState) {
	deadline := time.NewTimer(c.drainTimeout())
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	drained := false
	for !drained {
		if c.inFlight(w.name) == 0 && len(w.queue) == 0 {
			drained = true
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-deadline.C:
			c.log.Warn("coordinator: owner drain timed out; forcing removal",
				"worker", w.name, "inflight", c.inFlight(w.name), "timeout", c.drainTimeout())
		case <-tick.C:
			continue
		}
		break
	}
	c.mu.Lock()
	if cancel := w.cancel; cancel != nil {
		cancel()
	}
	delete(c.workers, w.name)
	delete(c.byTicket, string(w.ticket))
	delete(c.index, w.name)
	c.mu.Unlock()

	c.confirmedMu.Lock()
	delete(c.confirmed, w.name)
	c.confirmedMu.Unlock()

	if err := c.emit(eventlog.KindWorkerRetired, map[string]any{
		"worker": w.name,
		"reason": "scale-in",
	}); err != nil {
		c.log.Warn("coordinator: eventlog emit", "err", err)
	}
}

// rangesFor resolves n contiguous key ranges for target from the table's
// chunker. n == 1 is the unpartitioned single unbounded range, matching
// boot behavior byte for byte.
func (c *Coordinator) rangesFor(ctx context.Context, target string, n int) ([]source.Chunk, error) {
	if n <= 1 {
		return []source.Chunk{{}}, nil
	}
	chunker, ok := c.chunkers[target]
	if !ok || chunker == nil {
		return nil, fmt.Errorf("no chunker: table was booted unpartitioned")
	}
	ps, ok := chunker.(source.PartitionSource)
	if !ok {
		return nil, fmt.Errorf("this source does not support range partitioning")
	}
	return ps.Partitions(ctx, n)
}

// tableRef returns the resolved TableRef for target.
func (c *Coordinator) tableRef(target string) (source.TableRef, bool) {
	for _, w := range c.loadRouting().owners[target] {
		for _, r := range w.refs {
			if r.Target == target {
				return r, true
			}
		}
	}
	return source.TableRef{}, false
}

// partitionName derives partition p's worker group name, the same shape
// spec.Table.WorkerGroupNames produces at boot.
func partitionName(pipeline, target string, p int) string {
	return fmt.Sprintf("%s-%s-%d", pipeline, target, p)
}

// emitScale records the scale event on the durable trail.
func (c *Coordinator) emitScale(target string, from, to int) {
	if err := c.emit(eventlog.KindTableRepartitioned, map[string]any{
		"table": target,
		"from":  from,
		"to":    to,
	}); err != nil {
		c.log.Warn("coordinator: eventlog emit", "err", err)
	}
	c.log.Info("coordinator: table repartitioned", "table", target, "from", from, "to", to)
}

const (
	defaultScaleDrainTimeout = 60 * time.Second
	defaultMaxWorkers        = 32
)

// drainTimeout bounds the staged-cycle and in-flight drains of a re-slice.
func (c *Coordinator) drainTimeout() time.Duration {
	if c.cfg.ScaleDrainTimeout > 0 {
		return c.cfg.ScaleDrainTimeout
	}
	return defaultScaleDrainTimeout
}

// maxWorkersFor returns target's partition cap: the table's own
// spec.Table.Workers.Max when set, else the pipeline-wide Config.MaxWorkers,
// else the default. A table that declares its own cap ignores the global one.
func (c *Coordinator) maxWorkersFor(target string) int {
	if c.cfg.Spec != nil {
		for _, t := range c.cfg.Spec.Tables {
			if t.Target != target {
				continue
			}
			if t.Workers != nil && t.Workers.Max > 0 {
				return t.Workers.Max
			}
			break
		}
	}
	if c.cfg.MaxWorkers > 0 {
		return c.cfg.MaxWorkers
	}
	return defaultMaxWorkers
}

// setRouteForTest publishes one table's owners into the routing snapshot.
// Tests only: production routing is published by boot and by ScaleTable.
func (c *Coordinator) setRouteForTest(target string, owners []*workerState) {
	snap := c.loadRouting().clone()
	snap.owners[target] = owners
	c.publishRouting(snap)
}

// setRangesForTest replaces the whole ranges map in the routing snapshot,
// matching the old c.partitionRanges = map[...] assignment. Tests only.
func (c *Coordinator) setRangesForTest(ranges map[string][]source.Chunk) {
	snap := c.loadRouting().clone()
	snap.ranges = ranges
	c.publishRouting(snap)
}

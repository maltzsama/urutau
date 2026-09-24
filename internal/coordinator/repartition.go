package coordinator

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/eventlog"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
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

// pauseTable marks a table whose re-slice is draining, so a re-slice's drain
// can converge: a continuously loaded table never owes nothing on its own.
// The pump buffers the table's next batches (pauseHold) instead of parking on
// them, so the queue drains, the flip happens, and resumeTable flushes the
// held batches — routed by the new layout. Idempotent: pausing an
// already-paused table keeps the same resume channel.
func (c *Coordinator) pauseTable(target string) {
	c.pausedMu.Lock()
	defer c.pausedMu.Unlock()
	if c.paused == nil {
		c.paused = map[string]chan struct{}{}
	}
	if _, ok := c.paused[target]; !ok {
		c.paused[target] = make(chan struct{})
	}
}

// resumeTable releases a table paused by pauseTable and wakes the pump to
// flush the batches held during the pause.
func (c *Coordinator) resumeTable(target string) {
	c.pausedMu.Lock()
	if ch, ok := c.paused[target]; ok {
		close(ch)
		delete(c.paused, target)
	}
	c.pausedMu.Unlock()
	select {
	case c.pauseWake <- struct{}{}:
	default: // a wake is already pending; flushPaused drains every resumed table
	}
}

// pauseHold reports whether b was buffered for a paused table rather than left
// for the pump to route. A table that is paused, OR that still has held
// batches awaiting their post-flip flush, buffers: the second condition keeps
// ordering when a batch for the resumed table arrives before the pump
// processes the resume wake. Callers must be the pump goroutine — it is the
// only router, so nothing can be routed between a held batch and the batches
// that follow it.
func (c *Coordinator) pauseHold(b *dataplane.Batch) bool {
	c.pausedMu.Lock()
	defer c.pausedMu.Unlock()
	if c.pauseBuf == nil {
		c.pauseBuf = map[string][]*dataplane.Batch{}
	}
	if _, paused := c.paused[b.Table]; !paused && len(c.pauseBuf[b.Table]) == 0 {
		return false
	}
	c.pauseBuf[b.Table] = append(c.pauseBuf[b.Table], b)
	return true
}

// flushPaused routes the batches held for every resumed table, in order, and
// clears their buffers. A table still paused keeps buffering. It runs on the
// pump goroutine, so the held batches are routed before any later batch.
func (c *Coordinator) flushPaused(ctx context.Context) error {
	c.pausedMu.Lock()
	held := make(map[string][]*dataplane.Batch)
	for table, buf := range c.pauseBuf {
		if len(buf) == 0 {
			continue
		}
		if _, paused := c.paused[table]; paused {
			continue // still paused: keep buffering
		}
		held[table] = buf
		delete(c.pauseBuf, table)
	}
	c.pausedMu.Unlock()
	for table, batches := range held {
		for _, b := range batches {
			if err := c.enqueueBatch(ctx, b, nil); err != nil {
				return fmt.Errorf("flush held batches for %s: %w", table, err)
			}
		}
	}
	return nil
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
// The sequence is prepare → commit, and every wait is bounded: a step that
// cannot complete fails the scale and leaves the OLD layout intact, so a
// scale is a retryable no-op rather than an incident (never an unbounded
// pause, never data loss):
//
//  1. Resolve the new ranges from the source's chunker.
//  2. Register any new owner (queue, ticket, index) so its Hello is
//     accepted the moment its pod starts.
//  3. Seed the new owner's confirmed position, so confirmedPosition does
//     not stall the whole pipeline on an owner that has committed nothing.
//  4. PREPARE: pause the table's input so its drain can converge — a
//     continuously loaded table never reaches zero in-flight on its own.
//  5. Drain the table's in-flight batches and open staged cycles. With the
//     input paused, no new batch arrives and this converges.
//  6. COMMIT: swap the routing snapshot atomically, then resume the input.
//     The pump's held batch is routed by the new layout, so no batch spans
//     the flip.
//  7. Drain and drop any owner the new layout removed.
//
// A timeout in the prepare phase (4-5) resumes the input and returns,
// leaving the old layout in place: the scaler retries, and worker recovery
// stays the supervisor's job.
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
	// A partitioned table needs a sink that can order concurrent writers —
	// the same capability boot-time validation requires. Re-checked here
	// because the table may have booted unpartitioned (workers: 1) and is
	// only now becoming partitioned.
	if err := requireConcurrentSink(spec.Table{Target: target, Workers: &spec.WorkerSpec{Number: n}}, c.snk); err != nil {
		return fmt.Errorf("coordinator: scale %s: %w", target, err)
	}

	ranges, err := c.rangesFor(ctx, target, n, ref)
	if err != nil {
		return fmt.Errorf("coordinator: scale %s: %w", target, err)
	}
	if len(ranges) != n {
		return fmt.Errorf("coordinator: scale %s: resolved %d ranges for %d partitions", target, len(ranges), n)
	}

	next, removed, created, err := c.reslicedOwners(target, owners, n, ref)
	if err != nil {
		return fmt.Errorf("coordinator: scale %s: %w", target, err)
	}

	// A scale-in removes owners whose Pods the StatefulSet has already
	// deleted. One that is not attached yet owes work can never drain it:
	// the drain below would time out on every retry and the table would
	// wedge. The case the supervisor cannot see is an owner that never
	// attached at all — its Pod died while still connecting — so it is never
	// "detached". Its work is uncommitted, so a clean replay recovers it.
	for _, w := range removed {
		if c.strandedOwner(w) {
			err := fmt.Errorf("coordinator: scale %s: removed owner %s is not attached and owes work; terminating for a clean replay", target, w.name)
			c.fail(err)
			return err
		}
	}

	// A pre-commit failure leaves the old layout active, so the owners this
	// call CREATED must be rolled back: otherwise they linger as ghosts —
	// accepting Hellos, supervised, and holding a retention position they
	// will never advance.
	committed := false
	defer func() {
		if !committed {
			for _, w := range created {
				c.unregisterOwner(w)
			}
		}
	}()

	// The new owners must be able to resume: an owner with no confirmed
	// position pins confirmedPosition to nil and stalls source retention
	// for the WHOLE pipeline, not just this table. A scale-in creates none.
	c.seedConfirmed(created)

	// PREPARE: pause the table's input. The drain below waits until the
	// table owes nothing — every in-flight batch acked, every staged cycle
	// committed — and a continuously loaded table never reaches that on its
	// own. Pausing makes it converge; on any failure the input resumes and
	// the old layout stands.
	c.pauseTable(target)
	resumed := false
	defer func() {
		if !resumed {
			c.resumeTable(target)
		}
	}()
	if err := c.drainForFlip(ctx, target, owners); err != nil {
		return fmt.Errorf("coordinator: scale %s: drain: %w", target, err)
	}

	// COMMIT: the flip is atomic and instant, and happens only now that the
	// table owes nothing. Resuming routes the pump's held batch — and every
	// batch after it — by the new layout.
	snap := cur.clone()
	snap.owners[target] = next
	snap.ranges[target] = ranges
	c.publishRouting(snap)
	c.resumeTable(target)
	resumed = true
	committed = true

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

// strandedOwner reports whether w has no attached session yet holds queued or
// in-flight batches — work nothing will deliver once its Pod is gone.
func (c *Coordinator) strandedOwner(w *workerState) bool {
	c.mu.Lock()
	attached := w.attached
	queued := len(w.queue)
	c.mu.Unlock()
	return !attached && (queued > 0 || c.inFlight(w.name) > 0)
}

// reslicedOwners returns the owner slice for n partitions, keeping the
// existing owners in place (minimal remap: only the boundary moves) and
// creating the ones a scale-out adds. It also returns the owners a scale-in
// drops and the ones this call CREATED (not reused), so a pre-commit failure
// can roll exactly those back.
func (c *Coordinator) reslicedOwners(target string, cur []*workerState, n int, ref source.TableRef) (next, removed, created []*workerState, err error) {
	if n <= len(cur) {
		return cur[:n:n], cur[n:], nil, nil
	}
	next = make([]*workerState, len(cur), n)
	copy(next, cur)
	for p := len(cur); p < n; p++ {
		w, isNew, err := c.registerOwner(partitionName(c.cfg.Spec.Pipeline, target, p), ref)
		if err != nil {
			return nil, nil, nil, err
		}
		next = append(next, w)
		if isNew {
			created = append(created, w)
		}
	}
	return next, nil, created, nil
}

// registerOwner creates a worker group and publishes it in the registry, so
// Session accepts the pod's Hello the moment it connects. A name that is
// already registered is reused (created=false): a pod restart must not
// orphan its queue.
func (c *Coordinator) registerOwner(name string, ref source.TableRef) (*workerState, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if w, ok := c.workers[name]; ok {
		return w, false, nil
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
	c.setIndex(name, newPositionIndex(c.runID))
	// Seed the ack clock before the pod can attach, or the supervisor's
	// next tick sees an attached worker with no lastAck and resets it.
	c.supervisor.noteRegistered(name, time.Now())
	return w, true, nil
}

// unregisterOwner drops a worker the coordinator created for a scale that
// never committed, so no ghost owner is left registered under the old
// layout — accepting Hellos, supervised, and holding a retention position it
// will never advance.
func (c *Coordinator) unregisterOwner(w *workerState) {
	c.mu.Lock()
	delete(c.workers, w.name)
	delete(c.byTicket, string(w.ticket))
	c.deleteIndex(w.name)
	c.mu.Unlock()
	c.confirmedMu.Lock()
	delete(c.confirmed, w.name)
	c.confirmedMu.Unlock()
	c.supervisor.forget(w.name)
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

// drainForFlip blocks until target owes nothing: no in-flight batch on any
// current owner and no open staged cycle. Only this table is held; the
// shared reader keeps streaming every other table.
func (c *Coordinator) drainForFlip(ctx context.Context, target string, owners []*workerState) error {
	deadline := time.NewTimer(c.drainTimeout())
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		inflight, queued := 0, 0
		// In-flight batches first. positionIndex frees a worker's queue
		// strictly from the head, in send order, and a batch leaves only
		// when the table's acked position covers it. After a flip the owner
		// receives a DIFFERENT key range, whose acks need not cover the
		// batch still at its head, so the queue can wedge: the owner stops
		// acking, the supervisor calls it stale, and the reset discards its
		// open staged cycles — silently losing the rows they carried.
		for _, w := range owners {
			inflight += c.inFlight(w.name)
			queued += len(w.queue)
		}
		// Then the staged cycles, which must commit as one unit and so must
		// not span two layouts.
		cycles := 0
		cyclesOpen, cyclesDone := 0, 0
		if c.stagesCycles() {
			cyclesOpen, cyclesDone = c.staged.openForBreakdown(core.TableRef{Target: target})
			cycles = cyclesOpen + cyclesDone
		}
		pending := inflight + queued + cycles
		if pending == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("%d batch(es)/cycle(s) still pending after %s (in-flight=%d queued=%d staged-cycles=%d open=%d done=%d open-seqs=%v)",
				pending, c.drainTimeout(), inflight, queued, cycles, cyclesOpen, cyclesDone, c.staged.openSeqs(core.TableRef{Target: target}))
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
	for c.inFlight(w.name) != 0 || len(w.queue) != 0 {
		select {
		case <-ctx.Done():
			// The flip has already committed: the retire must finish, or the
			// owner stays registered with a live session under the new
			// layout. Force the detach rather than abort mid-way.
			c.log.Warn("coordinator: owner drain context done; forcing removal",
				"worker", w.name, "inflight", c.inFlight(w.name))
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
	// c.index is deliberately NOT deleted: enqueueTo and onAck index it
	// without holding c.mu (coordinator.go:1797, :2024) and dereference the
	// result directly, so removing the entry under a live in-flight batch
	// would race them into a nil-map-value panic. The entry is drained by
	// the loop above and costs one empty index per retired owner, bounded
	// by the run's lifetime.
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
func (c *Coordinator) rangesFor(ctx context.Context, target string, n int, ref source.TableRef) ([]source.Chunk, error) {
	if n <= 1 {
		return []source.Chunk{{}}, nil
	}
	// A table booted with one worker has no chunker: resolvePartitionRanges
	// short-circuits before building one. Scaling such a table out is the
	// main case this feature exists for, so build it on demand here the same
	// way boot does, and keep it for the next re-slice.
	chunker := c.lookupChunker(target)
	if chunker == nil {
		built, err := c.qsrc.NewChunker(ref.Source, strings.Join(ref.PrimaryKey, ","), c.cfg.ChunkSize)
		if err != nil {
			return nil, fmt.Errorf("chunker: %w", err)
		}
		c.storeChunker(target, built)
		chunker = built
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
	return fmt.Sprintf("%s-%d", spec.WorkerGroupPrefix(pipeline, target), p)
}

// OwnerNames returns target's partition owner names in partition order — the
// live routing layout. It is the read-only companion to ScaleTable: a scaler
// (or a test) uses it to observe a re-slice's effect, including DURING a
// scale-in/out, without reaching into the coordinator's internals.
func (c *Coordinator) OwnerNames(target string) []string {
	owners, ok := c.loadRouting().ownersOf(target)
	if !ok {
		return nil
	}
	out := make([]string, len(owners))
	for i, w := range owners {
		out[i] = w.name
	}
	return out
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

// lookupChunker returns target's chunker, or nil when none is built yet.
func (c *Coordinator) lookupChunker(target string) source.ChunkSource {
	c.chunkersMu.Lock()
	defer c.chunkersMu.Unlock()
	return c.chunkers[target]
}

// storeChunker memoizes a chunker built on demand by a re-slice.
func (c *Coordinator) storeChunker(target string, ch source.ChunkSource) {
	c.chunkersMu.Lock()
	defer c.chunkersMu.Unlock()
	if c.chunkers == nil {
		c.chunkers = map[string]source.ChunkSource{}
	}
	c.chunkers[target] = ch
}

package coordinator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/eventlog"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"

	"github.com/maltzsama/urutau/internal/inflight"
	route "github.com/maltzsama/urutau/internal/routing"
)

// routing is the coordinator's partition layout: the routing package's
// immutable snapshot, with the worker groups as its owners.
type routing = route.Layout[*workerState]

// newRouting returns a layout over the given owners and ranges.
func newRouting(owners map[string][]*workerState, ranges map[string][]source.Chunk) *routing {
	return route.NewLayout(owners, ranges)
}

// loadRouting returns the current routing snapshot, never nil.
func (c *Coordinator) loadRouting() *routing {
	if r := c.routing.Load(); r != nil {
		return r
	}
	return newRouting(nil, nil)
}

// publishRouting swaps in a new routing snapshot.
func (c *Coordinator) publishRouting(r *routing) {
	c.routing.Store(r)
}

// pauseTable pauses the input of a table whose re-slice is draining (see
// route.Pause).
func (c *Coordinator) pauseTable(target string) { c.pause.Pause(target) }

// resumeTable releases a table paused by pauseTable and wakes the pump to
// flush the batches held during the pause.
func (c *Coordinator) resumeTable(target string) { c.pause.Resume(target) }

// pauseHold reports whether b was buffered for a paused table rather than left
// for the pump to route. Callers must be the pump goroutine — it is the only
// router, so nothing can be routed between a held batch and the batches that
// follow it.
func (c *Coordinator) pauseHold(b *dataplane.Batch) bool { return c.pause.Hold(b.Table, b) }

// flushPaused routes the batches held for every resumed table, in order, and
// clears their buffers. A table still paused keeps buffering. It runs on the
// pump goroutine, so the held batches are routed before any later batch.
func (c *Coordinator) flushPaused(ctx context.Context) error {
	held := c.pause.TakeResumed()
	// On an error, release every held batch not sent yet: they left the pause,
	// so nothing else ever would.
	releaseHeld := func() {
		for _, batches := range held {
			for _, b := range batches {
				b.Release()
			}
		}
	}
	for table, batches := range held {
		delete(held, table)
		// Batches accumulated before the pause are older than those it held.
		if err := c.flushAccum(ctx, table); err != nil {
			for _, b := range batches {
				b.Release()
			}
			releaseHeld()
			return fmt.Errorf("flush accumulated batches for %s: %w", table, err)
		}
		// Sent the way a gate's held batches are: concatenated up to the
		// cycle bounds. One by one, every held source batch became a cycle,
		// and a commit, of its own (#828). enqueueHeld releases them all.
		if err := c.enqueueHeld(ctx, &pb.BatchMeta{Table: table}, batches); err != nil {
			releaseHeld()
			return fmt.Errorf("flush held batches for %s: %w", table, err)
		}
	}
	return nil
}

// ScaleTable re-slices target across n partitions, adding or removing
// owners so the table ends up with exactly n of them. It is the entry
// point a scaler (KEDA today, an operator by hand) drives. The sequence —
// prepare, drain, flip, retire — is route.Scaler's; the coordinator does
// each step through routingHost.
func (c *Coordinator) ScaleTable(ctx context.Context, target string, n int) error {
	changed, err := c.repart.Scale(ctx, routingHost{c}, target, n)
	if err != nil {
		return fmt.Errorf("coordinator: %w", err)
	}
	if changed {
		c.pushDashState()
	}
	return nil
}

// routingHost is the coordinator as a re-slice sees it.
type routingHost struct{ c *Coordinator }

func (h routingHost) Layout() *routing                   { return h.c.loadRouting() }
func (h routingHost) Publish(l *routing)                 { h.c.publishRouting(l) }
func (h routingHost) Name(w *workerState) string         { return w.name }
func (h routingHost) Stranded(w *workerState) bool       { return h.c.strandedOwner(w) }
func (h routingHost) Forget(w *workerState)              { h.c.unregisterOwner(w) }
func (h routingHost) Seed(created []*workerState)        { h.c.seedConfirmed(created) }
func (h routingHost) PauseInput(target string)           { h.c.pauseTable(target) }
func (h routingHost) ResumeInput(target string)          { h.c.resumeTable(target) }
func (h routingHost) Fail(err error)                     { h.c.fail(fmt.Errorf("coordinator: %w", err)) }
func (h routingHost) Scaled(target string, from, to int) { h.c.emitScale(target, from, to) }

func (h routingHost) Drain(ctx context.Context, target string, owners []*workerState) error {
	return h.c.drainForFlip(ctx, target, owners)
}

func (h routingHost) Retire(ctx context.Context, w *workerState) { h.c.retireOwner(ctx, w) }

// Admit refuses a scale that cannot happen now: during the table's
// snapshot, past its worker cap, without a primary key to partition by, or
// onto a sink that cannot order concurrent writers.
func (h routingHost) Admit(target string, n int) error {
	c := h.c
	if c.isSnapshotting(target) {
		return errors.New("snapshot in progress; retry once it completes")
	}
	if max := c.maxWorkersFor(target); n > max {
		return fmt.Errorf("%d exceeds maxWorkers %d", n, max)
	}
	ref, ok := c.tableRef(target)
	if !ok {
		return errors.New("no table ref")
	}
	if n > 1 && len(ref.PrimaryKey) == 0 {
		return errors.New("partitioning requires a primary key")
	}
	// The same capability boot-time validation requires. Re-checked here
	// because the table may have booted unpartitioned (workers: 1) and is
	// only now becoming partitioned.
	return requireConcurrentSink(spec.Table{Target: target, Workers: &spec.WorkerSpec{Number: n}}, c.snk)
}

func (h routingHost) Ranges(ctx context.Context, target string, n int) ([]source.Chunk, error) {
	ref, ok := h.c.tableRef(target)
	if !ok {
		return nil, errors.New("no table ref")
	}
	return h.c.rangesFor(ctx, target, n, ref)
}

// Owner registers the worker group of target's partition p, so Session
// accepts the pod's Hello the moment it connects.
func (h routingHost) Owner(target string, p int) (*workerState, bool, error) {
	ref, ok := h.c.tableRef(target)
	if !ok {
		return nil, false, errors.New("no table ref")
	}
	return h.c.registerOwner(route.PartitionName(h.c.cfg.Spec.Pipeline, target, p), ref)
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
	c.setIndex(name, inflight.NewPositionIndex(c.runID))
	// Seed the ack clock before the pod can attach, or the supervisor's
	// next tick sees an attached worker with no lastAck and resets it.
	c.supervisor.noteRegistered(name, time.Now())
	return w, true, nil
}

// unregisterOwner drops a worker the coordinator created for a scale that
// never committed, so no ghost owner is left registered under the old
// layout — accepting Hellos, supervised, and holding a retention position it
// will never advance.
//
// A worker may have attached to it while the scale was draining. Its session
// ends here: left attached it stays bound to this forgotten group, and when
// the scale is retried and registers the name again nothing reads the new
// group's queue (#842). The worker attaches again and is refused as unknown
// until the retry registers it.
func (c *Coordinator) unregisterOwner(w *workerState) {
	c.mu.Lock()
	if cancel := w.cancel; cancel != nil {
		cancel()
	}
	delete(c.workers, w.name)
	delete(c.byTicket, string(w.ticket))
	c.deleteIndex(w.name)
	c.mu.Unlock()
	c.confirmed.Forget(w.name)
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
	for _, w := range added {
		c.confirmed.Seed(w.name, base)
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
			cyclesOpen, cyclesDone = c.staged.OpenForBreakdown(core.TableRef{Target: target})
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
				pending, c.drainTimeout(), inflight, queued, cycles, cyclesOpen, cyclesDone, c.staged.OpenSeqs(core.TableRef{Target: target}))
		case <-c.ackNotify: // an ack may have drained in-flight (issue #587)
		case <-tick.C:
		}
	}
}

// retireOwner drains a removed owner's in-flight batches, then detaches it.
// The drain is bounded: an owner that never drains is forced out and its
// range replayed by the inheriting owner, which is safe because the writes
// are idempotent upserts. It also forgets the owner's supervisor state,
// symmetric with unregisterOwner: worker names are deterministic, so a later
// scale-out that reuses the name must not inherit the retired owner's crash
// and delivery clocks (issue #491).
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
		case <-c.ackNotify: // an ack may have drained in-flight (issue #587)
			continue
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

	c.confirmed.Forget(w.name)

	c.supervisor.forget(w.name)

	if err := c.emit(eventlog.KindWorkerRetired, map[string]any{
		"worker": w.name,
		"reason": "scale-in",
	}); err != nil {
		c.log.Warn("coordinator: eventlog emit", "err", err)
	}
}

// rangesFor ensures the table's chunker exists and returns the single
// unbounded range. Ownership is the rendezvous hash, so a re-slice no longer
// samples key ranges — the chunker only powers the index range scan the
// snapshot fan-out reads by. A table booted with one worker has no chunker
// yet, so it is built on demand here and kept for the next re-slice.
func (c *Coordinator) rangesFor(_ context.Context, target string, n int, ref source.TableRef) ([]source.Chunk, error) {
	if n <= 1 {
		return []source.Chunk{{}}, nil
	}
	if c.lookupChunker(target) == nil {
		if c.qsrc == nil {
			// A source with no SQL query surface (Kafka: coordinator.go's
			// boot makes QuerySource optional, issue #394) has no chunker at
			// all — scaling it out to n>1 is a config error, not a nil-qsrc
			// panic on the line below.
			return nil, fmt.Errorf("coordinator: scale %s: source %q has no SQL query surface to read chunks by", target, c.cfg.Spec.Source.Kind)
		}
		built, err := c.qsrc.NewChunker(ref.Source, strings.Join(ref.PrimaryKey, ","), c.cfg.ChunkSize)
		if err != nil {
			return nil, fmt.Errorf("chunker: %w", err)
		}
		c.storeChunker(target, built)
	}
	return []source.Chunk{{}}, nil
}

// tableRef returns the resolved TableRef for target.
func (c *Coordinator) tableRef(target string) (source.TableRef, bool) {
	owners, _ := c.loadRouting().OwnersOf(target)
	for _, w := range owners {
		for _, r := range w.refs {
			if r.Target == target {
				return r, true
			}
		}
	}
	return source.TableRef{}, false
}

// ownerNames returns the live owner names, the input to the rendezvous owner
// function. The order does not matter: the winner is chosen by hash, with the
// name as an order-independent tie-break.
func ownerNames(owners []*workerState) []string {
	names := make([]string, len(owners))
	for i, w := range owners {
		names[i] = w.name
	}
	return names
}

// OwnerNames returns target's partition owner names in partition order — the
// live routing layout. It is the read-only companion to ScaleTable: a scaler
// (or a test) uses it to observe a re-slice's effect, including DURING a
// scale-in/out, without reaching into the coordinator's internals.
func (c *Coordinator) OwnerNames(target string) []string {
	owners, ok := c.loadRouting().OwnersOf(target)
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
	if c.specByTarget != nil {
		if t, ok := c.specByTarget[target]; ok && t.Workers != nil && t.Workers.Max > 0 {
			return t.Workers.Max
		}
	} else if c.cfg.Spec != nil {
		for _, t := range c.tablesOrSpec() {
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

// ownerCount returns how many owners target has in the live layout.
func (c *Coordinator) ownerCount(target string) int {
	owners, _ := c.loadRouting().OwnersOf(target)
	return len(owners)
}

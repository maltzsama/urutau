// Package gate is the shared DBLog window gate used by both the collapsed
// runner and the distributed coordinator (design: one orchestration for
// collapsed and distributed mode, issue #404).
//
// While a chunk's SELECT is in flight on a consumer, live events of the
// gated table are held instead of being shipped: a live event racing ahead
// of the chunk's rows would miss the window delete and duplicate the row. A
// gate is keyed by (target, partition) — a partitioned table's N workers each
// run their own independent DBLog pass over their own PK range concurrently,
// so N windows can be open on the same table at once — bounded by a batch
// count and byte size, and releases everything it holds when it is torn down.
//
// The package holds only its windows and buffers. Delivery is delegated to a
// Sink supplied at construction, so the same gate serves the coordinator's
// per-partition enqueue path and the runner's single-key relay.
package gate

import (
	"context"
	"fmt"
	"sync"
)

// Window identifies one open DBLog window: a table's Nth partition. An
// unpartitioned table's sole window is always {Target, 0}.
type Window struct {
	Target    string
	Partition int
}

// Key renders a Window into the gate's map key.
func (w Window) Key() string { return fmt.Sprintf("%s#%d", w.Target, w.Partition) }

// Sink receives the items a gate releases. Flush and Close run under the
// gate's flush lock, so their sends reach the consumer in source order and
// are serialized with the gate's own early drains.
type Sink[T any] interface {
	// Flush delivers items held for one open window, InWindow-tagged for
	// windowID. The window stays open.
	Flush(ctx context.Context, w Window, windowID uint64, items []T) error
	// Close delivers items held when a window is sealed: ordinary trailing
	// changes, with no window tag.
	Close(ctx context.Context, w Window, items []T) error
	// Release discards items when the gate is torn down (release-on-cancel,
	// issue #212).
	Release(items []T)
}

// Gate is a set of bounded, keyed DBLog windows. It is safe for concurrent
// use: a producer Holds, a consumer Flushes/Closes, and a teardown path
// Releases.
type Gate[T any] struct {
	// maxCount and maxBytes bound one window; maxBytes is measured by size.
	// A bound of zero is unlimited. The item that crosses a bound is admitted,
	// so one oversized item never deadlocks the gate.
	maxCount int
	maxBytes int64
	size     func(T) int64

	sink Sink[T]

	// mu guards on/win/buf/bytes/ready/drain. flushMu serializes every drain
	// (Flush, Close, the early drain) and any caller-side send that must not
	// overtake a drain (WithFlush).
	mu      sync.Mutex
	flushMu sync.Mutex

	on    map[string]bool
	win   map[string]Window
	buf   map[string][]T
	bytes map[string]int64
	// ready is, per open window, the window id whose rows are in the
	// consumer's window and whose catch-up is still under way: its held items
	// may go out tagged before the catch-up ends. A full gate then drains
	// instead of blocking the producer — blocking it stalls the reader, and
	// with it the very catch-up the window waits for (#435).
	ready map[string]uint64
	// drain is closed and replaced to wake a producer blocked on a full gate
	// when any drain happens; the waiter re-checks its own window.
	drain chan struct{}
}

// New builds a gate. size (required when maxBytes > 0) measures one item's
// byte weight; sink receives every drain.
func New[T any](maxCount int, maxBytes int64, size func(T) int64, sink Sink[T]) *Gate[T] {
	return &Gate[T]{
		maxCount: maxCount,
		maxBytes: maxBytes,
		size:     size,
		sink:     sink,
		on:       map[string]bool{},
		win:      map[string]Window{},
		buf:      map[string][]T{},
		bytes:    map[string]int64{},
		ready:    map[string]uint64{},
		drain:    make(chan struct{}),
	}
}

// Open opens (or refreshes) the window for (target, partition). The window
// stays open for the WHOLE snapshot of that partition — the consumer pauses
// relaying while it works the table. Flush drains per chunk without closing
// it; Close seals it. A gate that opened and closed per chunk would let gap
// events (positioned after the gate's backlog) flow straight through, then
// release older backlog after them — a reordering that resurrects old values.
func (g *Gate[T]) Open(target string, partition int) {
	w := Window{Target: target, Partition: partition}
	g.mu.Lock()
	g.on[w.Key()] = true
	g.win[w.Key()] = w
	g.mu.Unlock()
}

// MarkReady records that windowID's rows are in the consumer's window, so a
// full window drains early instead of blocking the producer, and wakes a
// producer blocked on a full gate. Ignored when no window is open.
func (g *Gate[T]) MarkReady(target string, partition int, windowID uint64) {
	key := Window{Target: target, Partition: partition}.Key()
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.on[key] {
		return
	}
	g.ready[key] = windowID
	g.wakeLocked()
}

// ClearReady forgets that a window's current chunk is in its consumer's
// window: that consumer died, and the gate must hold live items again until
// the redone chunk's readiness.
func (g *Gate[T]) ClearReady(target string, partition int) {
	g.mu.Lock()
	delete(g.ready, Window{Target: target, Partition: partition}.Key())
	g.mu.Unlock()
}

// Hold buffers item in an open window for target, blocking while that window
// is full. It returns:
//
//   - (true, nil): item is held, and Flush/Close/ReleaseAll owns it.
//   - (false, nil): no window is open for target, or ctx ended while blocked —
//     the item is the caller's to route live. (ctx ending is only reachable
//     during shutdown.)
//   - (true, err): an early drain failed; the item was not taken and the
//     caller must release it and fail the run.
func (g *Gate[T]) Hold(ctx context.Context, target string, item T) (bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	key, held := g.openKeyLocked(target)
	if !held {
		return false, nil
	}
	for g.fullLocked(key) {
		_, ready := g.ready[key]
		drain := g.drain
		g.mu.Unlock()
		if ready {
			// The chunk is in the consumer's window: drain the held items
			// tagged now, so the producer keeps pulling the reader.
			if err := g.drainReady(ctx, key); err != nil {
				g.mu.Lock()
				return true, err
			}
		} else {
			// Before readiness: wait for the consumer's chunk SELECT, which
			// does not depend on the producer.
			select {
			case <-drain:
			case <-ctx.Done():
				g.mu.Lock()
				return false, nil
			}
		}
		g.mu.Lock()
		// Re-check after the wait or the drain: the gate may have closed, or
		// the table's window changed, while the producer was away.
		if key, held = g.openKeyLocked(target); !held {
			return false, nil
		}
	}
	g.appendLocked(key, item)
	return true, nil
}

// Flush drains a window's held items and delivers them InWindow-tagged for
// windowID; the window stays open. A no-op when the window holds nothing.
func (g *Gate[T]) Flush(ctx context.Context, target string, partition int, windowID uint64) error {
	g.flushMu.Lock()
	defer g.flushMu.Unlock()
	key := Window{Target: target, Partition: partition}.Key()
	g.mu.Lock()
	items := g.takeLocked(key)
	delete(g.ready, key)
	g.wakeLocked()
	g.mu.Unlock()
	if len(items) == 0 {
		return nil
	}
	return g.sink.Flush(ctx, Window{Target: target, Partition: partition}, windowID, items)
}

// Close drains a window's held items as ordinary trailing changes (no window
// tag) and seals just that window — other partitions of the same table keep
// their own windows open. A no-op when the window holds nothing.
func (g *Gate[T]) Close(ctx context.Context, target string, partition int) error {
	g.flushMu.Lock()
	defer g.flushMu.Unlock()
	key := Window{Target: target, Partition: partition}.Key()
	g.mu.Lock()
	items := g.takeLocked(key)
	delete(g.on, key)
	delete(g.win, key)
	delete(g.ready, key)
	g.wakeLocked()
	g.mu.Unlock()
	if len(items) == 0 {
		return nil
	}
	return g.sink.Close(ctx, Window{Target: target, Partition: partition}, items)
}

// ReleaseAll closes every open window and releases the items held in them.
// Called when the snapshot phase ends, so an aborted snapshot (ctx cancelled
// before Close) does not leak held items (issue #212). A producer blocked on
// a full gate wakes and finds the window gone.
func (g *Gate[T]) ReleaseAll() {
	g.mu.Lock()
	var held []T
	for k := range g.on {
		held = append(held, g.takeLocked(k)...)
		delete(g.on, k)
		delete(g.win, k)
		delete(g.ready, k)
	}
	g.wakeLocked()
	g.mu.Unlock()
	if len(held) > 0 {
		g.sink.Release(held)
	}
}

// WithFlush runs fn while holding the gate's flush lock. A caller with its own
// per-table send path (the coordinator's staged accumulator) uses it to
// serialize that send with the gate's drains, preserving source order.
func (g *Gate[T]) WithFlush(fn func()) {
	g.flushMu.Lock()
	defer g.flushMu.Unlock()
	fn()
}

// Lock and Unlock guard the gate's open-window state. A caller with a
// fallback buffer that must be chosen atomically with the gate's open state
// (the coordinator's staged accumulator) holds the lock across both its check
// and its fallback update. Do not call other Gate methods while holding it.
func (g *Gate[T]) Lock()   { g.mu.Lock() }
func (g *Gate[T]) Unlock() { g.mu.Unlock() }

// OpenKeyLocked returns the first open window key for target, if any. Caller
// holds the lock. A table has at most as many simultaneously-open keys as it
// has partitions actively snapshotting.
func (g *Gate[T]) OpenKeyLocked(target string) (string, bool) { return g.openKeyLocked(target) }

// AppendLocked buffers item under key without the size bound. Caller holds the
// lock. This is the staged-accumulator path, which holds a batch until the
// window's own drain rather than applying the gate's backpressure.
func (g *Gate[T]) AppendLocked(key string, item T) { g.appendLocked(key, item) }

// BufferedTablesLocked returns the targets of open windows holding items.
// Caller holds the lock.
func (g *Gate[T]) BufferedTablesLocked() []string {
	var out []string
	seen := map[string]bool{}
	for k, buf := range g.buf {
		if len(buf) == 0 {
			continue
		}
		if w, ok := g.win[k]; ok && !seen[w.Target] {
			seen[w.Target] = true
			out = append(out, w.Target)
		}
	}
	return out
}

// Len reports how many items window (target, partition) holds.
func (g *Gate[T]) Len(target string, partition int) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.buf[Window{Target: target, Partition: partition}.Key()])
}

// IsOpen reports whether window (target, partition) is open.
func (g *Gate[T]) IsOpen(target string, partition int) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.on[Window{Target: target, Partition: partition}.Key()]
}

// openKeyLocked returns the first open window key for target. Caller holds mu.
func (g *Gate[T]) openKeyLocked(target string) (string, bool) {
	for k, on := range g.on {
		if !on {
			continue
		}
		if w, ok := g.win[k]; ok && w.Target == target {
			return k, true
		}
	}
	return "", false
}

// fullLocked reports whether key's window has reached a bound. Caller holds mu.
func (g *Gate[T]) fullLocked(key string) bool {
	if g.maxCount > 0 && len(g.buf[key]) >= g.maxCount {
		return true
	}
	if g.maxBytes > 0 && g.size != nil && g.bytes[key] >= g.maxBytes {
		return true
	}
	return false
}

// appendLocked holds item in key's window. Caller holds mu.
func (g *Gate[T]) appendLocked(key string, item T) {
	g.buf[key] = append(g.buf[key], item)
	if g.size != nil {
		g.bytes[key] += g.size(item)
	}
}

// takeLocked empties key's window and returns what it held, oldest first.
// Caller holds mu.
func (g *Gate[T]) takeLocked(key string) []T {
	items := g.buf[key]
	delete(g.buf, key)
	delete(g.bytes, key)
	return items
}

// wakeLocked wakes every producer blocked on a full gate. Caller holds mu.
func (g *Gate[T]) wakeLocked() {
	close(g.drain)
	g.drain = make(chan struct{})
}

// drainReady is the early flush of a full window whose chunk is ready: the
// held items go out tagged for that window, as Flush would at the end of the
// catch-up. The window stays open.
func (g *Gate[T]) drainReady(ctx context.Context, key string) error {
	g.flushMu.Lock()
	defer g.flushMu.Unlock()
	g.mu.Lock()
	windowID, ready := g.ready[key]
	w, open := g.win[key]
	if !ready || !open {
		g.mu.Unlock()
		return nil
	}
	items := g.takeLocked(key)
	g.wakeLocked()
	g.mu.Unlock()
	if len(items) == 0 {
		return nil
	}
	return g.sink.Flush(ctx, w, windowID, items)
}

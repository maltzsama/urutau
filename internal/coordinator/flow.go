package coordinator

import (
	"context"
	"sync"

	"github.com/maltzsama/urutau/position"
)

// flowBudget is the coordinator's global in-flight budget (design §5.4.1):
// a ceiling on serialized batch bytes queued or sent but not yet acked, per
// PROCESS — not per worker. A slow worker may consume more than its share
// but never starves the others below perWorkerMin; a budget that is full
// blocks the producer, which is the structural backpressure down to the
// reader loop.
type flowBudget struct {
	totalBytes   int64
	perWorkerMin int64

	mu   sync.Mutex
	cond *sync.Cond
	used map[string]int64
	// oversizedOwner is the worker holding the single oversized batch the
	// budget admits at once. A batch larger than the whole ceiling can never
	// fit it, so waiting for room would deadlock (nothing can free the budget
	// when nothing is in flight). It is admitted regardless of the sum as
	// long as no OTHER oversized batch is in flight — that bounds the
	// over-ceiling memory to one batch while still making progress. Cleared
	// when that worker's charge returns to zero.
	oversizedOwner string
}

func newFlowBudget(totalBytes, perWorkerMin int64) *flowBudget {
	b := &flowBudget{
		totalBytes:   totalBytes,
		perWorkerMin: perWorkerMin,
		used:         map[string]int64{},
	}
	b.cond = sync.NewCond(&b.mu)
	return b
}

func (b *flowBudget) sum() int64 {
	var s int64
	for _, v := range b.used {
		s += v
	}
	return s
}

// acquire reserves n bytes for one worker, blocking while the process is
// over budget AND the worker is beyond its minimum floor. The wait is
// ctx-aware: a cancelled pipeline stops waiting.
//
// The ctx wake-up uses context.AfterFunc, which registers a callback fired
// only on cancellation — no goroutine or channel is allocated on the hot
// (under-budget) path.
func (b *flowBudget) acquire(ctx context.Context, worker string, n int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	// Wake blocked waiters when ctx dies, so they re-check and exit.
	stop := context.AfterFunc(ctx, func() { b.cond.Broadcast() })
	defer stop()

	oversized := n > b.totalBytes
	for {
		if oversized {
			// An oversized batch can never fit the ceiling, so waiting for
			// room would deadlock; admit it when no other oversized batch is
			// in flight (issue #209).
			if b.oversizedOwner == "" {
				break
			}
		} else if b.sum()+n <= b.totalBytes || b.used[worker]+n <= b.perWorkerMin {
			break
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		b.cond.Wait()
	}
	b.used[worker] += n
	if oversized {
		b.oversizedOwner = worker
	}
	return nil
}

// release returns n bytes to the pool.
func (b *flowBudget) release(worker string, n int64) {
	b.mu.Lock()
	b.used[worker] -= n
	if b.used[worker] <= 0 {
		delete(b.used, worker)
		if b.oversizedOwner == worker {
			b.oversizedOwner = ""
		}
	}
	b.mu.Unlock()
	b.cond.Broadcast()
}

// isOversized reports whether n is larger than the whole budget — a batch that
// can never fit the ceiling and so takes the single oversized slot.
func (b *flowBudget) isOversized(n int64) bool { return n > b.totalBytes }

// clearOversized frees the oversized slot when its batch is acked, so a later
// oversized batch is not blocked while the owner keeps normal traffic in
// flight (issue #209 review).
func (b *flowBudget) clearOversized(worker string) {
	b.mu.Lock()
	if b.oversizedOwner == worker {
		b.oversizedOwner = ""
	}
	b.mu.Unlock()
	b.cond.Broadcast()
}

// inFlight reports the bytes reserved for one worker.
func (b *flowBudget) inFlight(worker string) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used[worker]
}

// inflightBatch is one batch's flight-window metadata — position only; the
// data itself is never retained (design §5.4.2).
type inflightBatch struct {
	id    uint64
	table string
	high  position.Position // nil for position-less snapshot rows
	bytes int64
	// oversized marks a batch larger than the whole flow budget, so its ack
	// can release the budget's single oversized slot.
	oversized bool
}

// positionIndex tracks each worker's unacked batches so an Ack can release
// the budget by table position (design §6). Removal is by the head only:
// batches leave in order, and a batch of an unconfirmed table blocks the
// removal of the ones behind it — conservative and correct.
type positionIndex struct {
	mu    sync.Mutex
	head  []inflightBatch
	acked map[string]position.Position
	runID string
	dirty bool // set on add/truncate; cleared by MarkClean
}

func newPositionIndex(runID string) *positionIndex {
	return &positionIndex{acked: map[string]position.Position{}, runID: runID}
}

// InFlight reports the number of unacked batches (delivered, not yet acked).
func (p *positionIndex) InFlight() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.head)
}

func (p *positionIndex) add(b inflightBatch) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.head = append(p.head, b)
	p.dirty = true
}

// Dirty reports whether the manifest changed since the last MarkClean.
func (p *positionIndex) Dirty() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.dirty
}

// MarkClean clears the dirty flag after a successful checkpoint write.
func (p *positionIndex) MarkClean() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dirty = false
}

// Manifest snapshots the acked positions and the in-flight batch-id range
// for the async S3 checkpoint (design §6) — a small file; the data is never
// persisted.
func (p *positionIndex) Manifest() PositionManifest {
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
		m.FirstBatchID = p.head[0].id
		m.LastBatchID = p.head[len(p.head)-1].id
	}
	return m
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
// table has any ack. It returns the bytes released and whether an oversized
// batch was among them (so its budget slot can be freed).
func (p *positionIndex) truncate(table string, pos position.Position) (freed int64, freedOversized bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if cur, ok := p.acked[table]; !ok || advances(pos, cur) {
		p.acked[table] = pos
		p.dirty = true
	}
	for len(p.head) > 0 {
		h := p.head[0]
		if h.high == nil {
			if _, ok := p.acked[h.table]; !ok {
				break
			}
		} else if cur, ok := p.acked[h.table]; !ok || !covered(h.high, cur) {
			break
		}
		freed += h.bytes
		if h.oversized {
			freedOversized = true
		}
		p.head = p.head[1:]
		p.dirty = true
	}
	return freed, freedOversized
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

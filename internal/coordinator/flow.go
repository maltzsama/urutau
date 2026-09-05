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
func (b *flowBudget) acquire(ctx context.Context, worker string, n int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	// Wake blocked waiters when ctx dies, so they re-check and exit.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			b.cond.Broadcast()
		case <-stop:
		}
	}()

	for b.sum()+n > b.totalBytes && b.used[worker]+n > b.perWorkerMin {
		if err := ctx.Err(); err != nil {
			return err
		}
		b.cond.Wait()
	}
	b.used[worker] += n
	return nil
}

// release returns n bytes to the pool.
func (b *flowBudget) release(worker string, n int64) {
	b.mu.Lock()
	b.used[worker] -= n
	if b.used[worker] <= 0 {
		delete(b.used, worker)
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
}

func newPositionIndex(runID string) *positionIndex {
	return &positionIndex{acked: map[string]position.Position{}, runID: runID}
}

func (p *positionIndex) add(b inflightBatch) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.head = append(p.head, b)
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
// table has any ack. It returns the bytes released.
func (p *positionIndex) truncate(table string, pos position.Position) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if cur, ok := p.acked[table]; !ok || pos.Compare(cur) > 0 {
		p.acked[table] = pos
	}
	var freed int64
	for len(p.head) > 0 {
		h := p.head[0]
		if h.high == nil {
			if _, ok := p.acked[h.table]; !ok {
				break
			}
		} else if cur, ok := p.acked[h.table]; !ok || h.high.Compare(cur) > 0 {
			break
		}
		freed += h.bytes
		p.head = p.head[1:]
	}
	return freed
}

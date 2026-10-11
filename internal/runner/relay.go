package runner

import (
	"context"
	"sync"

	"github.com/maltzsama/urutau/dataplane"
	dpint "github.com/maltzsama/urutau/internal/dataplane"
	"github.com/maltzsama/urutau/internal/gate"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/internal/worker"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
)

// relayGateMaxEvents and relayGateMaxBytes bound the relay's DBLog window,
// the same structural backpressure the coordinator's gate has (#438, audit
// #5): beyond it the relay blocks until the window's chunk is ready and
// drains. The runner's gate is a single window, keyed by (target, 0).
const (
	relayGateMaxEvents = 1024
	relayGateMaxBytes  = 64 << 20
)

// relay pumps reader events into the worker's ingest channel and releases
// the DBLog chunk markers. Window tagging happens in the reader at decode
// time; the marker's Release first drains the pump, so every event decoded
// inside the window is already enqueued ahead of it. The gate mirrors the
// coordinator's pump gate: while a chunk SELECT is in flight the table's
// live events are buffered and released InWindow-tagged only after
// AddWindowRows populates the worker window — the ordering the window proof
// needs (a live event must never deduplicate against an empty window).
type relay struct {
	ingest   chan<- worker.Ingest
	window   *worker.Worker
	flushReq chan chan struct{}
	// onDeliver notes a batch's position as it is read from the source, so
	// the runner can tell delivered (dispatched) from committed per table.
	onDeliver func(table, pos string)

	// gate is the shared DBLog window gate (design §3.1): while a chunk
	// SELECT is in flight the table's live events are buffered and released
	// InWindow-tagged only after AddWindowRows populates the worker window.
	gate *gate.Gate[*dataplane.Batch]

	gateMu       sync.Mutex
	gateTgt      string
	gateChk      uint32
	flushGate    bool
	gateFlushReq chan chan struct{}
}

// relaySink delivers the relay's gate drains to the worker's ingest channel.
type relaySink struct{ r *relay }

func (s relaySink) Flush(ctx context.Context, _ gate.Window, windowID uint64, items []*dataplane.Batch) error {
	return s.r.sendWindowed(ctx, uint32(windowID), items)
}

// Close releases the relay's window at seal. drainGate Flushes before it, so
// the buffer is empty here; anything left is released defensively.
func (s relaySink) Close(_ context.Context, _ gate.Window, items []*dataplane.Batch) error {
	for _, b := range items {
		b.Release()
	}
	return nil
}

func (s relaySink) Release(items []*dataplane.Batch) {
	for _, b := range items {
		b.Release()
	}
}

// sendWindowed enqueues held batches InWindow-tagged for chunkID, in order.
func (r *relay) sendWindowed(ctx context.Context, chunkID uint32, items []*dataplane.Batch) error {
	for _, b := range items {
		select {
		case r.ingest <- worker.Ingest{Table: b.Table, Batch: b, Win: &rowchange.Window{WindowID: uint64(chunkID), InWindow: true}}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func newRelay(ingest chan<- worker.Ingest, window *worker.Worker, onDeliver func(table, pos string)) *relay {
	r := &relay{
		ingest:       ingest,
		window:       window,
		onDeliver:    onDeliver,
		flushReq:     make(chan chan struct{}, 1),
		gateFlushReq: make(chan chan struct{}, 1),
	}
	r.gate = gate.New[*dataplane.Batch](relayGateMaxEvents, relayGateMaxBytes, dpint.BatchBytes, relaySink{r})
	return r
}

func (r *relay) Release(ctx context.Context, table string, chunkID uint32, at position.Position) error {
	req := make(chan struct{})
	// Every handshake observes ctx: if the relay goroutine has already
	// exited, nobody closes req and the send below would block forever,
	// wedging the boot snapshot with no error (issue #551).
	select {
	case r.flushReq <- req:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-req:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case r.ingest <- worker.Ingest{
		Table:    table,
		Position: at.String(),
		Win:      &rowchange.Window{WindowID: uint64(chunkID), Closes: true},
	}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *relay) AddWindowRows(target string, chunkID uint32, batch *dataplane.Batch) error {
	// The batch is already Arrow (the chunk SELECT was encoded straight into
	// builders, #584); the worker window takes ownership.
	if err := r.window.AddWindowRows(target, uint64(chunkID), batch); err != nil {
		return err
	}
	// The chunk's rows are in the window: a full gate may now drain early
	// rather than block the relay, the coordinator's ChunkReady early drain.
	r.gate.MarkReady(target, 0, uint64(chunkID))
	return nil
}

// GateOn starts buffering the table's live events for a chunk SELECT in
// flight. Called by the orchestrator before the SELECT.
func (r *relay) GateOn(table string, chunkID uint32) {
	// Open the window before publishing the chunk id: a batch the relay
	// pulls concurrently is then held rather than routed untagged.
	r.gate.Open(table, 0)
	r.gateMu.Lock()
	r.gateTgt = table
	r.gateChk = chunkID
	r.gateMu.Unlock()
}

// holdGate buffers a live batch in the relay's open window, if any, blocking
// while that bounded window is full. A drain failure surfaces to the relay.
func (r *relay) holdGate(ctx context.Context, b *dataplane.Batch) (bool, error) {
	return r.gate.Hold(ctx, b.Table, b)
}

// GateFlush releases the buffered events InWindow-tagged for the chunk. It
// is SYNCHRONOUS: it waits for the pump to drain the gate buffer into ingest
// before returning, so a gated event can never be overtaken by the Closes
// marker that Release sends afterwards. This makes the window deduplication
// (and the droppedByWindow evidence) deterministic.
func (r *relay) GateFlush(ctx context.Context) error {
	r.gateMu.Lock()
	r.flushGate = true
	r.gateMu.Unlock()
	req := make(chan struct{})
	select {
	case r.gateFlushReq <- req:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-req:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// deliverNote reports a batch's last position to the runner as it is read
// from the source, before it is routed or gated. It is the "delivered" half
// of the confirmed-point rule: a table with delivered past its committed has
// data still in flight, so the confirmed position must not advance past its
// committed.
func (r *relay) deliverNote(b *dataplane.Batch) {
	if r.onDeliver == nil || b == nil || b.Record == nil || b.Record.NumRows() == 0 {
		return
	}
	reader, err := transport.NewBatchReader(b.Record, nil)
	if err != nil {
		return
	}
	r.onDeliver(b.Table, reader.Position(reader.NumRows()-1))
}

// drainGate writes the pending gate buffer to ingest, InWindow-tagged, and
// turns the gate off. Returns true if a flush was performed. The reader's own
// window decision is preserved: an event it explicitly left untagged (at or
// before its source watermark) stays untagged; only events decoded in the
// GateOn↔OpenWindow gap (reader never saw them) are tagged here.
func (r *relay) drainGate(ctx context.Context) (bool, error) {
	r.gateMu.Lock()
	if !r.flushGate {
		r.gateMu.Unlock()
		return false, nil
	}
	table := r.gateTgt
	chunkID := r.gateChk
	r.flushGate = false
	r.gateMu.Unlock()

	// Flush tags the held events for the chunk; Close then seals the window
	// (its buffer is empty here — the relay goroutine is the only producer).
	if err := r.gate.Flush(ctx, table, 0, uint64(chunkID)); err != nil {
		return true, err
	}
	if err := r.gate.Close(ctx, table, 0); err != nil {
		return true, err
	}
	return true, nil
}

// run pulls columnar batches from the reader and routes them into the
// worker's ingest channel, gating them during a chunk SELECT (ordering the
// window proof needs: a live event must never deduplicate against an empty
// window). Release drains decoded events ahead of the Closes marker.
func (r *relay) run(ctx context.Context, rdr source.Reader) error {
	batchCh := make(chan *dataplane.Batch, 16)
	// readErr carries the puller's failure to the consumer below. A closed
	// batchCh alone cannot distinguish "the source ended" from "the source
	// broke": discarding the error here would let a mid-stream failure end
	// the pipeline as a clean, successful run with silently truncated data —
	// the same class of bug fixed in the plugin reader, one layer up. Written
	// once before close(batchCh), read only after the channel is drained, so
	// the close/read pair carries the happens-before edge.
	var readErr error
	drainer, _ := rdr.(source.Drainer)

	// readerMu serializes rdr.Next and rdr.Drain: both read the reader's own
	// buffer, so a Release flush must never interleave a pull and reorder the
	// InWindow events ahead of the Closes marker. drainReq asks the puller
	// goroutine — the sole reader — to flush the reader's buffer into batchCh.
	var readerMu sync.Mutex
	drainReq := make(chan *drainRequest, 1)

	// pushBatch routes one batch into batchCh, applying the delivered-note
	// bookkeeping the normal pull does.
	pushBatch := func(b *dataplane.Batch) error {
		r.deliverNote(b)
		select {
		case batchCh <- b:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	go func() {
		defer close(batchCh)
		for {
			// Service a pending drain before pulling: flush the reader's
			// buffered events into batchCh, in order.
			select {
			case req := <-drainReq:
				if err := r.serviceDrain(ctx, req, drainer, &readerMu, pushBatch); err != nil {
					readErr = err
					return
				}
				continue
			default:
			}

			readerMu.Lock()
			b, err := rdr.Next(ctx)
			if err != nil {
				readerMu.Unlock()
				readErr = err
				return
			}
			if b == nil {
				readerMu.Unlock()
				return
			}
			err = pushBatch(b)
			readerMu.Unlock()
			if err != nil {
				readErr = err
				return
			}
		}
	}()

	for {
		if flushed, err := r.drainGate(ctx); err != nil {
			return err
		} else if flushed {
			continue
		}
		select {
		case b, ok := <-batchCh:
			if !ok {
				if _, err := r.drainGate(ctx); err != nil {
					return err
				}

				return readErr // nil on a clean end of stream
			}
			held, err := r.holdGate(ctx, b)
			if err != nil {
				return err
			}
			if held {
				continue
			}
			select {
			case r.ingest <- worker.Ingest{Table: b.Table, Batch: b}:
			case <-ctx.Done():
				return ctx.Err()
			}
		case req := <-r.flushReq:
			// Drain the reader's own buffer ahead of the Closes marker, so no
			// InWindow event is overtaken (issue #488).
			if err := r.flushDecoded(ctx, drainer, drainReq, batchCh); err != nil {
				close(req)
				return err
			}
			close(req)
		case req := <-r.gateFlushReq:
			// Flush the gate buffer before acking: GateFlush blocks until
			// the gated events are in ingest, so Release's Closes marker
			// (sent after) can never overtake them.
			if _, err := r.drainGate(ctx); err != nil {
				close(req)
				return err
			}
			close(req)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

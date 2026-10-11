package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/plugin/client"
	"github.com/maltzsama/urutau/internal/plugin/contract"
	"github.com/maltzsama/urutau/position"
)

// sourceResult is one outcome of a table stream: a batch, or the error that
// ended it. Batches and errors share ONE channel so an error can never race
// the channel close and surface as a clean end (issue #568).
type sourceResult struct {
	batch *dataplane.Batch
	err   error
}

// sourceReader implements source.Reader over a Flight DoGet stream. The
// plugin ships an Arrow CDC record (contract §8.1, before/after struct
// shaped); the reader projects it straight into the flat urutau wire schema
// — no rowchange round-trip.
type sourceReader struct {
	clients   ClientFunc
	alloc     memory.Allocator
	refs      []core.TableRef
	ctx       context.Context
	cancel    context.CancelFunc
	logger    *slog.Logger
	out       chan sourceResult
	wg        sync.WaitGroup
	positions map[string]string // target table → its last opaque offset
	mu        sync.Mutex
	setConf   func() position.Position
	// closeAdapter releases the owning adapter (client + in-process server) on
	// Close; nil for a subprocess source (issue #496).
	closeAdapter func() error
}

func (r *sourceReader) Start(_ context.Context, from position.Position) error {
	// One goroutine per table: CDC streams are continuous, so streaming them
	// one after another meant the second table's stream never started until
	// the first ended — which, for a live source, is never (issue #568).
	// Streams run on the reader's own context (Open's), so Close stops them.
	//
	// Each table resumes from ITS OWN offset: the stored position is a
	// per-table envelope for a multi-table source, a bare offset for a
	// single-table one (which is also the legacy format) (issue #568).
	perTable := decodePositions(from)
	r.wg.Add(len(r.refs))
	for _, ref := range r.refs {
		go func(ref core.TableRef) {
			defer r.wg.Done()
			start := from
			if perTable != nil {
				start = StringPosition{Offset: perTable[ref.Target]}
			} else if from != nil {
				start = StringPosition{Offset: from.String()}
			}
			if err := r.streamTable(r.ctx, ref, start); err != nil {
				r.emit(sourceResult{err: fmt.Errorf("stream %s: %w", ref.Source, err)})
			}
		}(ref)
	}
	go func() {
		r.wg.Wait()
		close(r.out)
	}()
	return nil
}

// decodePositions returns the per-table envelope a stored position carries, or
// nil when it is a bare (single-table/legacy) offset. A base64 opaque offset
// never starts with '{', so it cannot be mistaken for the JSON envelope.
func decodePositions(p position.Position) map[string]string {
	if p == nil {
		return nil
	}
	var env map[string]string
	if err := json.Unmarshal([]byte(p.String()), &env); err != nil || env == nil {
		return nil
	}
	return env
}

// recordPosition remembers one table's last offset. A multi-table source keeps
// them separate instead of one table overwriting another's resume point
// (issue #568).
func (r *sourceReader) recordPosition(target, offset string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.positions == nil {
		r.positions = map[string]string{}
	}
	r.positions[target] = offset
}

// positionSnapshot returns the resume position: the bare offset for a single
// table (backward compatible), a deterministic per-table envelope for several.
func (r *sourceReader) positionSnapshot() StringPosition {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.positions) == 1 {
		for _, off := range r.positions {
			return StringPosition{Offset: off}
		}
	}
	if len(r.positions) == 0 {
		return StringPosition{}
	}
	b, err := json.Marshal(r.positions)
	if err != nil {
		return StringPosition{}
	}
	return StringPosition{Offset: string(b)}
}

// emit delivers a result unless the reader was closed: a full buffer is drained
// by Next, and a cancelled context means nobody is reading anymore.
func (r *sourceReader) emit(res sourceResult) {
	select {
	case r.out <- res:
	case <-r.ctx.Done():
	}
}

func (r *sourceReader) Next(ctx context.Context) (*dataplane.Batch, error) {
	select {
	case res, ok := <-r.out:
		if !ok {
			return nil, nil
		}
		return res.batch, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// use resolves the reader's current client (see SourceAdapter.use).
func (r *sourceReader) use() *client.Client {
	if r.clients == nil {
		return nil
	}
	return r.clients()
}

func (r *sourceReader) streamTable(ctx context.Context, ref core.TableRef, from position.Position) error {
	c := r.use()
	if c == nil {
		return errors.New("plugin source: not connected")
	}
	var fromOffset string
	if from != nil {
		fromOffset = from.String()
	}

	info, err := c.GetFlightInfo(ctx, contract.GetFlightInfoRequest{
		Table:      ref.Source,
		Mode:       "changes",
		FromOffset: fromOffset,
	})
	if err != nil {
		return err
	}

	if len(info.Endpoint) == 0 {
		return fmt.Errorf("no endpoint in FlightInfo")
	}

	// Consume all endpoints (contract allows partitioned data).
	for _, ep := range info.Endpoint {
		stream, err := c.DoGet(ctx, &flight.Ticket{Ticket: ep.Ticket.Ticket})
		if err != nil {
			return err
		}

		// Read the CDC schema from the stream, then read batches.
		var arrowSchema *arrow.Schema
		for {
			fd, err := stream.Recv()
			if err != nil {
				return fmt.Errorf("recv schema: %w", err)
			}
			if len(fd.DataHeader) > 0 {
				schema, err := flight.DeserializeSchema(fd.DataHeader, r.alloc)
				if err != nil {
					return fmt.Errorf("deserialize schema: %w", err)
				}
				// Contract §8.1 gate: validate the announced change-record
				// schema BEFORE consuming any record — the sink-side §10
				// rule (reject on the first batch, not the tenth) applied
				// symmetrically to the source side.
				if err := validateChangeSchema(schema); err != nil {
					return fmt.Errorf("contract violation: %w", err)
				}
				arrowSchema = schema
				break
			}
		}

		if err := r.readBatches(ctx, stream, arrowSchema, ref); err != nil {
			return err
		}
	}
	return nil
}

func (r *sourceReader) readBatches(ctx context.Context, stream flight.FlightService_DoGetClient, arrowSchema *arrow.Schema, ref core.TableRef) error {
	announcedChecked := false
	for {
		fd, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil // stream ended
			}
			return fmt.Errorf("plugin source: recv: %w", err)
		}
		if len(fd.DataBody) == 0 {
			continue // empty batch (liveness signal, CONTRACT §8.3)
		}

		// Deserialize the IPC record batch from DataBody.
		reader, err := ipc.NewReader(bytes.NewReader(fd.DataBody), ipc.WithAllocator(r.alloc))
		if err != nil {
			return fmt.Errorf("ipc reader: %w", err)
		}

		for reader.Next() {
			rec := reader.RecordBatch()
			if rec == nil {
				continue
			}
			// Contract §8 gate: the schema is fixed within the stream. On
			// the first record, the embedded record schema must match what
			// was announced — otherwise every column read below shifts.
			if !announcedChecked {
				if err := recordMatchesAnnouncedSchema(rec.Schema(), arrowSchema); err != nil {
					reader.Release()
					return fmt.Errorf("contract violation: %w", err)
				}
				announcedChecked = true
			}
			wire, lastOffset, err := cdcRecordToWire(rec, arrowSchema, ref, r.alloc)
			if err != nil {
				reader.Release()
				return err
			}
			if wire == nil {
				continue // every row had an unknown op
			}
			r.recordPosition(ref.Target, lastOffset)
			r.emit(sourceResult{batch: &dataplane.Batch{Table: ref.Target, Record: wire, Mode: dataplane.UpsertMode}})
			if ctx.Err() != nil {
				reader.Release()
				return ctx.Err()
			}
		}
		if err := reader.Err(); err != nil {
			reader.Release()
			return fmt.Errorf("read ipc: %w", err)
		}
		reader.Release()
	}
}

func (r *sourceReader) Close() {
	r.cancel()
	if r.closeAdapter != nil {
		_ = r.closeAdapter()
	}
}

func (r *sourceReader) SetConfirmed(f func() position.Position) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.setConf = f
}

func (r *sourceReader) Synced() position.Position {
	return r.positionSnapshot()
}

func (r *sourceReader) Master(_ context.Context) (position.Position, error) {
	return r.positionSnapshot(), nil
}

func (r *sourceReader) OpenWindow(_ context.Context, _ uint32) {}

func (r *sourceReader) ClearWindow() {}

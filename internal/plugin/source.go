// Package plugin implements adapters that wrap the Flight client into the
// standard source.Source and sink.Sink interfaces. This lets the runner
// drive external plugins identically to native drivers.
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
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/plugin/client"
	"github.com/maltzsama/urutau/internal/plugin/contract"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// ServerStopper is an in-process Flight server an adapter owns and must stop
// when it closes. flightwrap passes one; the subprocess path passes none — the
// stage supervisor owns that process (issue #496).
type ServerStopper interface{ Stop() }

// ClientFunc resolves the plugin's CURRENT Flight client, or nil when it is
// not connected. A subprocess plugin can be restarted by its supervisor, so an
// adapter must resolve the client per use instead of pinning the one that was
// live when it was built; an in-process plugin (flightwrap) passes a constant
// (issue #570).
type ClientFunc func() *client.Client

// StringPosition is an opaque position backed by a base64 offset string.
// External plugins use opaque offsets; the runner never inspects them.
type StringPosition struct {
	Offset string
}

func (p StringPosition) String() string { return p.Offset }

// Compare is identity-only. An opaque offset is a plugin cookie (contract
// §8.2), not an ordinal — the base64 alphabet does not preserve byte order,
// so lexicographic comparison is meaningless and can silently reorder
// resume points (skipping uncommitted data). The only defined relation is
// identity; anything else is position.Incomparable and callers MUST handle
// it conservatively.
func (p StringPosition) Compare(other position.Position) int {
	o, ok := other.(StringPosition)
	if !ok || p.Offset != o.Offset {
		return position.Incomparable
	}
	return 0
}

// Contains is identity-only for the same reason: an opaque offset does not
// contain another unless it is the same cookie.
func (p StringPosition) Contains(other position.Position) bool {
	o, ok := other.(StringPosition)
	return ok && p.Offset == o.Offset
}

// SourceAdapter wraps a Flight client as a source.Source. It speaks the
// external plugin contract (GetFlightInfo + DoGet) and projects the plugin's
// Arrow CDC record stream (contract §8.1) straight into the flat urutau wire
// schema.
type SourceAdapter struct {
	clients ClientFunc
	spec    spec.Source
	logger  *slog.Logger
	alloc   memory.Allocator
	// server is the in-process Flight server this adapter owns (flightwrap);
	// nil for a subprocess plugin, whose supervisor owns the process.
	server    ServerStopper
	closeOnce sync.Once
	closeErr  error
}

// NewSourceAdapter creates a source adapter over a connected Flight client.
// An optional ServerStopper is the in-process server flightwrap started, which
// Close stops (issue #496).
func NewSourceAdapter(clients ClientFunc, src spec.Source, logger *slog.Logger, servers ...ServerStopper) *SourceAdapter {
	if logger == nil {
		logger = slog.Default()
	}
	a := &SourceAdapter{
		clients: clients,
		spec:    src,
		logger:  logger,
		alloc:   memory.NewGoAllocator(),
	}
	if len(servers) > 0 {
		a.server = servers[0]
	}
	return a
}

// use resolves the adapter's current client, or nil when the plugin is down.
func (a *SourceAdapter) use() *client.Client {
	if a.clients == nil {
		return nil
	}
	return a.clients()
}

// Close releases the adapter: it closes the Flight client and stops the
// in-process server flightwrap started. Idempotent, so a reader's Close and a
// direct caller may both call it (issue #496).
func (a *SourceAdapter) Close() error {
	a.closeOnce.Do(func() {
		if c := a.use(); c != nil {
			a.closeErr = c.Close()
		}
		if a.server != nil {
			a.server.Stop()
		}
	})
	return a.closeErr
}

// Introspect resolves one spec table into its ref, schema, and warnings.
// For external plugins, schema comes from GetFlightInfo in snapshot mode.
// Validates that declared primary key columns exist in the returned schema.
func (a *SourceAdapter) Introspect(ctx context.Context, t spec.Table) (core.TableRef, core.Schema, []core.Warning, error) {
	c := a.use()
	if c == nil {
		return core.TableRef{}, core.Schema{}, nil, errors.New("plugin source: not connected")
	}
	info, err := c.GetFlightInfo(ctx, contract.GetFlightInfoRequest{
		Table: t.Source,
		Mode:  "snapshot",
	})
	if err != nil {
		return core.TableRef{}, core.Schema{}, nil, fmt.Errorf("introspect %s: %w", t.Source, err)
	}
	arrowSchema, err := flight.DeserializeSchema(info.Schema, a.alloc)
	if err != nil {
		return core.TableRef{}, core.Schema{}, nil, fmt.Errorf("deserialize schema: %w", err)
	}
	schema := arrowToCoreSchema(arrowSchema)

	// Validate PK columns exist in the schema.
	for _, pk := range t.PrimaryKey {
		found := false
		for _, c := range schema.Columns {
			if c.Name == pk {
				found = true
				break
			}
		}
		if !found {
			return core.TableRef{}, core.Schema{}, nil, fmt.Errorf("introspect %s: primary key column %q not in plugin schema", t.Source, pk)
		}
	}

	ref := core.TableRef{
		Source:     t.Source,
		Target:     t.Target,
		PrimaryKey: t.PrimaryKey,
	}
	return ref, schema, nil, nil
}

// InitialPosition returns the empty position (first boot). External
// plugins always start from the beginning unless a snapshot resume is
// in progress.
func (a *SourceAdapter) InitialPosition(_ context.Context) (position.Position, error) {
	return StringPosition{}, nil
}

// ParsePosition decodes a stored position string.
func (a *SourceAdapter) ParsePosition(s string) (position.Position, error) {
	return StringPosition{Offset: s}, nil
}

// Open opens the replication reader. It calls GetFlightInfo in changes
// mode to get the schema and then starts streaming via DoGet.
func (a *SourceAdapter) Open(ctx context.Context, refs []core.TableRef) (source.Reader, error) {
	ctx, cancel := context.WithCancel(ctx)
	// An in-process source's client and server are owned by this adapter, and
	// the reader's Close is the only teardown hook the source contract gives
	// (source.Reader.Close). A subprocess source's supervisor owns its client,
	// so leave it alone (issue #496).
	var closeAdapter func() error
	if a.server != nil {
		closeAdapter = a.Close
	}
	r := &sourceReader{
		clients:      a.clients,
		alloc:        a.alloc,
		refs:         refs,
		ctx:          ctx,
		cancel:       cancel,
		logger:       a.logger,
		out:          make(chan sourceResult, 16),
		closeAdapter: closeAdapter,
	}
	return r, nil
}

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

// cdcRecordToWire projects one plugin CDC record (contract §8.1, before/
// after struct shaped) straight into the flat urutau wire schema. The
// before/after struct children become top-level data columns; op maps to
// __op; offset rides __pos; ts_source rides __commit_ts. Returns the wire
// record (nil if every row had an unknown op) and the last row's offset for
// the reader's position.
func cdcRecordToWire(rec arrow.RecordBatch, arrowSchema *arrow.Schema, ref core.TableRef, alloc memory.Allocator) (arrow.RecordBatch, string, error) {
	changes, err := cdcRecordToChanges(rec, arrowSchema, ref)
	if err != nil {
		return nil, "", err
	}
	if len(changes) == 0 {
		return nil, "", nil
	}
	// The wire schema is inferred from the change images — the plugin's
	// announced Arrow schema is struct-shaped, not the flat data schema, so
	// MergeSchema over the rows is the source of the flat column types.
	cs := transport.MergeSchema(changes, core.Schema{PrimaryKey: ref.PrimaryKey})
	wire, err := transport.RecordFromChanges(changes, cs, alloc)
	if err != nil {
		return nil, "", fmt.Errorf("plugin: encode wire batch: %w", err)
	}
	return wire, changes[len(changes)-1].Position, nil
}

// cdcRecordToChanges translates one Arrow CDC record batch into change events.
func cdcRecordToChanges(rec arrow.RecordBatch, arrowSchema *arrow.Schema, ref core.TableRef) ([]rowchange.Change, error) {
	nrows := int(rec.NumRows())
	changes := make([]rowchange.Change, 0, nrows)

	opIdx := columnIndex(arrowSchema, "op")
	beforeIdx := columnIndex(arrowSchema, "before")
	afterIdx := columnIndex(arrowSchema, "after")
	offsetIdx := columnIndex(arrowSchema, "offset")
	tsIdx := columnIndex(arrowSchema, "ts_source")

	for i := 0; i < nrows; i++ {
		op := readStringCol(rec, opIdx, i)
		offset := readBinaryCol(rec, offsetIdx, i)
		var commitTS time.Time
		if tsIdx >= 0 && !rec.Column(tsIdx).IsNull(i) {
			// §8.1 fixes ts_source at Timestamp(ns, "UTC") and the schema
			// gate enforces it — decode ticks AS NANOSECONDS. The old
			// UnixMicro read misinterpreted ns ticks by 1000x.
			tsCol := rec.Column(tsIdx).(*array.Timestamp)
			commitTS = tsCol.Value(i).ToTime(arrow.Nanosecond).UTC()
		}

		var chg rowchange.Change
		switch op {
		case string(contract.OpInsert):
			chg.Op = rowchange.OpInsert
		case string(contract.OpUpdate):
			chg.Op = rowchange.OpUpdate
		case string(contract.OpDelete):
			chg.Op = rowchange.OpDelete
		default:
			continue
		}

		chg.Table = ref.Target
		chg.Position = offset
		chg.CommitTS = commitTS
		chg.IngestTS = time.Now().UTC()

		if beforeIdx >= 0 && !rec.Column(beforeIdx).IsNull(i) {
			before, err := structToMap(rec.Column(beforeIdx), i)
			if err != nil {
				return nil, err
			}
			chg.Before = before
		}
		if afterIdx >= 0 && !rec.Column(afterIdx).IsNull(i) {
			after, err := structToMap(rec.Column(afterIdx), i)
			if err != nil {
				return nil, err
			}
			chg.After = after
			chg.Key = extractPK(ref.PrimaryKey, chg.After)
		}

		if chg.Key == nil && chg.Before != nil {
			chg.Key = extractPK(ref.PrimaryKey, chg.Before)
		}

		changes = append(changes, chg)
	}
	return changes, nil
}

// validateChangeSchema enforces contract §8.1 (change record, v1, fixed) on
// the plugin's announced schema — BEFORE any record is consumed (the
// sink-side §10 rule applied symmetrically: rejection at the first batch,
// not the tenth). A plugin is third-party code in any language: a wrong
// column type here used to panic the reader deep in the stream instead of
// failing the stream cleanly.
//
// Contract shape (exactly, in order; before MAY be omitted):
//
//	op        Utf8                    NOT NULL   ("c" | "u" | "d")
//	before    Struct<table columns>   NULL       (row null on insert)  [optional]
//	after     Struct<table columns>   NULL       (row null on delete)
//	offset    Binary                  NOT NULL   (opaque, §8.2)
//	ts_source Timestamp(ns, "UTC")    NULL
func validateChangeSchema(s *arrow.Schema) error {
	// before is the only optional field; when omitted, the rest shift up.
	names := []string{"op", "after", "offset", "ts_source"}
	if s.NumFields() == 5 {
		names = []string{"op", "before", "after", "offset", "ts_source"}
	} else if s.NumFields() != 4 {
		return fmt.Errorf("plugin schema: %d fields, want 4 (before omitted) or 5 per contract §8.1", s.NumFields())
	}

	checks := map[string]struct {
		is   func(arrow.DataType) bool
		desc string
		null bool
	}{
		"op":        {func(dt arrow.DataType) bool { return dt.ID() == arrow.STRING }, "Utf8", false},
		"before":    {func(dt arrow.DataType) bool { return dt.ID() == arrow.STRUCT }, "Struct", true},
		"after":     {func(dt arrow.DataType) bool { return dt.ID() == arrow.STRUCT }, "Struct", true},
		"offset":    {func(dt arrow.DataType) bool { return dt.ID() == arrow.BINARY }, "Binary", false},
		"ts_source": {func(dt arrow.DataType) bool { return dt.ID() == arrow.TIMESTAMP }, "Timestamp(ns, UTC)", true},
	}

	for i, name := range names {
		f := s.Field(i)
		if f.Name != name {
			return fmt.Errorf("plugin schema: field %d is %q, want %q per contract §8.1", i, f.Name, name)
		}
		want := checks[name]
		if !want.is(f.Type) {
			return fmt.Errorf("plugin schema: field %d %q is %s, want %s per contract §8.1", i, f.Name, f.Type, want.desc)
		}
		if f.Nullable != want.null {
			return fmt.Errorf("plugin schema: field %d %q: nullability %v, want %v per contract §8.1", i, f.Name, f.Nullable, want.null)
		}
	}
	if s.NumFields() == 5 {
		ts := s.Field(4).Type.(*arrow.TimestampType)
		if ts.Unit != arrow.Nanosecond || ts.TimeZone != "UTC" {
			return fmt.Errorf("plugin schema: field 4 ts_source is Timestamp(%s, %q), want Timestamp(ns, UTC) per contract §8.1", ts.Unit, ts.TimeZone)
		}
	}
	return nil
}

// recordMatchesAnnouncedSchema rejects mid-stream schema drift (contract §8:
// the schema is fixed within the stream). Checked on the first record of
// every stream; a plugin that announces one schema and ships another must
// fail the stream, not shift every column read by one.
func recordMatchesAnnouncedSchema(recSchema, announced *arrow.Schema) error {
	if recSchema.NumFields() != announced.NumFields() {
		return fmt.Errorf("plugin record: %d fields, announced %d — schema drifted mid-stream (contract §8)", recSchema.NumFields(), announced.NumFields())
	}
	for i := range recSchema.NumFields() {
		rf, af := recSchema.Field(i), announced.Field(i)
		if rf.Name != af.Name {
			return fmt.Errorf("plugin record: field %d is %q, announced %q — schema drifted mid-stream (contract §8)", i, rf.Name, af.Name)
		}
		if !arrow.TypeEqual(rf.Type, af.Type) {
			return fmt.Errorf("plugin record: field %q is %s, announced %s — schema drifted mid-stream (contract §8)", rf.Name, rf.Type, af.Type)
		}
	}
	return nil
}

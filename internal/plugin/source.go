// Package plugin implements adapters that wrap the Flight client into the
// standard source.Source and sink.Sink interfaces. This lets the runner
// drive external plugins identically to native drivers.
package plugin

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
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
	"github.com/maltzsama/urutau/internal/sourcepull"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// StringPosition is an opaque position backed by a base64 offset string.
// External plugins use opaque offsets; the runner never inspects them.
type StringPosition struct {
	Offset string
}

func (p StringPosition) String() string { return p.Offset }

func (p StringPosition) Compare(other position.Position) int {
	o, ok := other.(StringPosition)
	if !ok {
		return 1
	}
	if p.Offset == o.Offset {
		return 0
	}
	if p.Offset < o.Offset {
		return -1
	}
	return 1
}

func (p StringPosition) Contains(other position.Position) bool {
	return p.Compare(other) <= 0
}

// SourceAdapter wraps a Flight client as a source.Source. It speaks the
// external plugin contract (GetFlightInfo + DoGet) and translates the
// Arrow CDC record stream into rowchange.Change events.
type SourceAdapter struct {
	client *client.Client
	spec   spec.Source
	logger *slog.Logger
	alloc  memory.Allocator
}

// NewSourceAdapter creates a source adapter over a connected Flight client.
func NewSourceAdapter(c *client.Client, src spec.Source, logger *slog.Logger) *SourceAdapter {
	if logger == nil {
		logger = slog.Default()
	}
	return &SourceAdapter{
		client: c,
		spec:   src,
		logger: logger,
		alloc:  memory.NewGoAllocator(),
	}
}

// Introspect resolves one spec table into its ref, schema, and warnings.
// For external plugins, schema comes from GetFlightInfo in snapshot mode.
// Validates that declared primary key columns exist in the returned schema.
func (a *SourceAdapter) Introspect(ctx context.Context, t spec.Table) (core.TableRef, core.Schema, []core.Warning, error) {
	info, err := a.client.GetFlightInfo(ctx, contract.GetFlightInfoRequest{
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
	out := make(chan rowchange.Change, 256)
	r := &sourceReader{
		client: a.client,
		alloc:  a.alloc,
		refs:   refs,
		ctx:    ctx,
		cancel: cancel,
		logger: a.logger,
		out:    out,
		puller: sourcepull.New(out),
	}
	return r, nil
}

// sourceReader implements source.Reader over a Flight DoGet stream.
type sourceReader struct {
	client   *client.Client
	alloc    memory.Allocator
	refs     []core.TableRef
	ctx      context.Context
	cancel   context.CancelFunc
	logger   *slog.Logger
	out      chan rowchange.Change
	puller   *sourcepull.Puller
	position StringPosition
	mu       sync.Mutex
	setConf  func() position.Position
}

func (r *sourceReader) Start(ctx context.Context, from position.Position) error {
	errCh := make(chan error, 1)
	r.puller.SetErr(errCh)
	go func() {
		defer close(r.out)
		for _, ref := range r.refs {
			if err := r.streamTable(ctx, ref, from, r.out); err != nil {
				errCh <- fmt.Errorf("stream %s: %w", ref.Source, err)
				return
			}
		}
	}()
	return nil
}

func (r *sourceReader) Next(ctx context.Context) (*dataplane.Batch, error) {
	return r.puller.Next(ctx)
}

func (r *sourceReader) streamTable(ctx context.Context, ref core.TableRef, from position.Position, out chan<- rowchange.Change) error {
	var fromOffset string
	if from != nil {
		fromOffset = from.String()
	}

	info, err := r.client.GetFlightInfo(ctx, contract.GetFlightInfoRequest{
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
		stream, err := r.client.DoGet(ctx, &flight.Ticket{Ticket: ep.Ticket.Ticket})
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
				arrowSchema = schema
				break
			}
		}

		if err := r.readBatches(stream, arrowSchema, ref, out); err != nil {
			return err
		}
	}
	return nil
}

func (r *sourceReader) readBatches(stream flight.FlightService_DoGetClient, arrowSchema *arrow.Schema, ref core.TableRef, out chan<- rowchange.Change) error {
	for {
		fd, err := stream.Recv()
		if err != nil {
			return nil // stream ended
		}
		if len(fd.DataBody) == 0 {
			continue // empty batch (liveness signal, CONTRACT §8.3)
		}

		// Deserialize the IPC record batch from DataBody.
		reader, err := ipc.NewReader(bytes.NewReader(fd.DataBody), ipc.WithAllocator(r.alloc))
		if err != nil {
			return fmt.Errorf("ipc reader: %w", err)
		}
		defer reader.Release()

		for reader.Next() {
			rec := reader.RecordBatch()
			if rec == nil {
				continue
			}
			changes := cdcRecordToChanges(rec, arrowSchema, ref)
			for i := range changes {
				r.mu.Lock()
				r.position = StringPosition{Offset: extractOffset(rec, i)}
				r.mu.Unlock()
				out <- changes[i]
			}
			rec.Retain()
			reader.Release()
		}
		if err := reader.Err(); err != nil {
			return fmt.Errorf("read ipc: %w", err)
		}
	}
}

func (r *sourceReader) Close() {
	r.cancel()
}

func (r *sourceReader) SetConfirmed(f func() position.Position) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.setConf = f
}

func (r *sourceReader) Synced() position.Position {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.position
}

func (r *sourceReader) Master(_ context.Context) (position.Position, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.position, nil
}

func (r *sourceReader) OpenWindow(_ context.Context, _ uint32) {}

func (r *sourceReader) ClearWindow() {}

// cdcRecordToChanges translates one Arrow CDC record batch into change events.
func cdcRecordToChanges(rec arrow.RecordBatch, arrowSchema *arrow.Schema, ref core.TableRef) []rowchange.Change {
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
			tsCol := rec.Column(tsIdx).(*array.Timestamp)
			v := tsCol.Value(i)
			commitTS = time.UnixMicro(int64(v)).UTC()
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
			chg.Before = structToMap(rec.Column(beforeIdx), i)
		}
		if afterIdx >= 0 && !rec.Column(afterIdx).IsNull(i) {
			chg.After = structToMap(rec.Column(afterIdx), i)
			chg.Key = extractPK(ref.PrimaryKey, chg.After)
		}

		if chg.Key == nil && chg.Before != nil {
			chg.Key = extractPK(ref.PrimaryKey, chg.Before)
		}

		changes = append(changes, chg)
	}
	return changes
}

// arrowToCoreSchema converts an Arrow schema to a core schema.
func arrowToCoreSchema(s *arrow.Schema) core.Schema {
	cols := make([]core.Column, 0, s.NumFields())
	for i := range s.NumFields() {
		f := s.Field(i)
		cols = append(cols, core.Column{
			Name: f.Name,
			Type: arrowTypeToCore(f.Type, f.Nullable),
		})
	}
	return core.Schema{Columns: cols}
}

func arrowTypeToCore(t arrow.DataType, nullable bool) core.ColumnType {
	switch t.ID() {
	case arrow.BOOL:
		return core.ColumnType{Kind: core.KindBool, Nullable: nullable}
	case arrow.INT8, arrow.INT16, arrow.INT32:
		return core.ColumnType{Kind: core.KindInt32, Nullable: nullable}
	case arrow.INT64:
		return core.ColumnType{Kind: core.KindInt64, Nullable: nullable}
	case arrow.UINT64:
		return core.ColumnType{Kind: core.KindUInt64, Nullable: nullable}
	case arrow.FLOAT32:
		return core.ColumnType{Kind: core.KindFloat32, Nullable: nullable}
	case arrow.FLOAT64:
		return core.ColumnType{Kind: core.KindFloat64, Nullable: nullable}
	case arrow.STRING, arrow.LARGE_STRING:
		return core.ColumnType{Kind: core.KindString, Nullable: nullable}
	case arrow.BINARY, arrow.LARGE_BINARY:
		return core.ColumnType{Kind: core.KindBinary, Nullable: nullable}
	case arrow.TIMESTAMP:
		return core.ColumnType{Kind: core.KindTimestampTZ, Nullable: nullable}
	default:
		return core.ColumnType{Kind: core.KindUnknown, Nullable: nullable}
	}
}

func columnIndex(s *arrow.Schema, name string) int {
	for i := range s.NumFields() {
		if s.Field(i).Name == name {
			return i
		}
	}
	return -1
}

func readStringCol(rec arrow.RecordBatch, idx, row int) string {
	if idx < 0 || rec.Column(idx).IsNull(row) {
		return ""
	}
	col := rec.Column(idx).(*array.String)
	return col.Value(row)
}

func readBinaryCol(rec arrow.RecordBatch, idx, row int) string {
	if idx < 0 || rec.Column(idx).IsNull(row) {
		return ""
	}
	col := rec.Column(idx).(*array.Binary)
	return base64.StdEncoding.EncodeToString(col.Value(row))
}

func extractOffset(rec arrow.RecordBatch, row int) string {
	idx := columnIndex(rec.Schema(), "offset")
	return readBinaryCol(rec, idx, row)
}

func structToMap(col arrow.Array, row int) map[string]any {
	if col == nil {
		return nil
	}
	structArr, ok := col.(*array.Struct)
	if !ok {
		return nil
	}
	fields := structArr.DataType().(*arrow.StructType)
	m := make(map[string]any, fields.NumFields())
	for i := range fields.NumFields() {
		if structArr.Field(i).IsNull(row) {
			continue
		}
		field := fields.Field(i)
		val := readValue(structArr.Field(i), row)
		m[field.Name] = val
	}
	return m
}

func readValue(col arrow.Array, row int) any {
	if col.IsNull(row) {
		return nil
	}
	switch c := col.(type) {
	case *array.Boolean:
		return c.Value(row)
	case *array.Int8:
		return int32(c.Value(row))
	case *array.Int16:
		return int32(c.Value(row))
	case *array.Int32:
		return c.Value(row)
	case *array.Int64:
		return c.Value(row)
	case *array.Uint8:
		return c.Value(row)
	case *array.Uint16:
		return c.Value(row)
	case *array.Uint32:
		return c.Value(row)
	case *array.Uint64:
		return c.Value(row)
	case *array.Float32:
		return c.Value(row)
	case *array.Float64:
		return c.Value(row)
	case *array.String:
		return c.Value(row)
	case *array.Binary:
		return c.Value(row)
	case *array.Timestamp:
		return time.UnixMicro(int64(c.Value(row))).UTC()
	default:
		return nil
	}
}

func extractPK(pkCols []string, row map[string]any) []any {
	if row == nil || len(pkCols) == 0 {
		return nil
	}
	keys := make([]any, 0, len(pkCols))
	for _, name := range pkCols {
		v, ok := row[name]
		if !ok {
			return nil
		}
		keys = append(keys, v)
	}
	return keys
}

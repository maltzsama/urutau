// Package plugin is a proof that the door is open: it implements a source
// and a sink entirely outside internal/, importing only the public
// contracts (source, sink, driver, core, change, position, spec) and
// registering them with the driver registry. The fake_test drives the
// collapsed runner end-to-end against these two drivers.
package plugin

import (
	"context"
	"strconv"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/driver"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// fakePos is a trivial monotonic position, enough to satisfy the contract.
type fakePos uint64

func (p fakePos) String() string { return strconv.FormatUint(uint64(p), 10) }

func (p fakePos) Compare(o position.Position) int {
	q, ok := o.(fakePos)
	if !ok {
		panic("fake: incompatible position")
	}
	switch {
	case p < q:
		return -1
	case p > q:
		return 1
	default:
		return 0
	}
}

func (p fakePos) Contains(o position.Position) bool {
	q, ok := o.(fakePos)
	if !ok {
		panic("fake: incompatible position")
	}
	return p >= q
}

// recRow is the fake sink's local row representation — the fake cannot
// import engine internals, so it decodes committed batches itself.
type recRow struct {
	Key   []any
	After map[string]any
}

// records captures every committed batch. The mutex makes the test's reads
// race-free against the worker's commits.
type records struct {
	mu      sync.Mutex
	upserts map[string][]recRow
	deletes map[string][]recRow
}

func newRecords() *records {
	return &records{
		upserts: map[string][]recRow{},
		deletes: map[string][]recRow{},
	}
}

func (r *records) commit(b *dataplane.Batch) {
	if b == nil || b.Record == nil {
		return
	}
	schema := b.Record.Schema()
	opIdx := -1
	dataIdx := make(map[string]int)
	for i := range schema.NumFields() {
		name := schema.Field(i).Name
		switch name {
		case "__op":
			opIdx = i
		default:
			if !strings.HasPrefix(name, "__") {
				dataIdx[name] = i
			}
		}
	}
	n := int(b.Record.NumRows())
	upserts := make([]recRow, 0, n)
	deletes := make([]recRow, 0, n)
	for row := 0; row < n; row++ {
		rr := recRow{After: make(map[string]any, len(dataIdx))}
		isDel := false
		if opIdx >= 0 {
			if oc, ok := b.Record.Column(opIdx).(*array.Uint8); ok && oc.Value(row) == uint8(2) { // OpDelete
				isDel = true
			}
		}
		for name, ci := range dataIdx {
			col := b.Record.Column(ci)
			if col.IsNull(row) {
				continue
			}
			v := arrowValue(col, row)
			rr.After[name] = v
			if name == "id" {
				rr.Key = []any{v}
			}
		}
		if isDel {
			deletes = append(deletes, rr)
		} else {
			upserts = append(upserts, rr)
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.upserts[b.Table] = append(r.upserts[b.Table], upserts...)
	r.deletes[b.Table] = append(r.deletes[b.Table], deletes...)
}

// arrowValue reads a scalar Arrow value at row.
func arrowValue(col arrow.Array, row int) any {
	switch c := col.(type) {
	case *array.Int64:
		return c.Value(row)
	case *array.Int32:
		return c.Value(row)
	case *array.String:
		return c.Value(row)
	case *array.Boolean:
		return c.Value(row)
	case *array.Float64:
		return c.Value(row)
	default:
		return nil
	}
}

func (r *records) rows(target string) []recRow {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]recRow, 0, len(r.upserts[target])+len(r.deletes[target]))
	out = append(out, r.upserts[target]...)
	out = append(out, r.deletes[target]...)
	return out
}

// committed is the sink singleton the runner writes into and the test reads.
var committed = newRecords()

func init() {
	if err := driver.RegisterSource("fake", source.Capabilities{Stream: true}, func(*spec.Spec, source.Runtime) (source.Source, error) {
		return Source{}, nil
	}); err != nil {
		panic(err)
	}
	if err := driver.RegisterSink("fake", func(context.Context, sink.Config) (sink.Sink, error) {
		return &Sink{rec: committed}, nil
	}); err != nil {
		panic(err)
	}
}

// Source is a stream-only fake: it emits seedRows and holds the stream open
// until the pipeline cancels it.
type Source struct{}

var _ source.Source = Source{}

func (Source) Introspect(_ context.Context, t spec.Table) (core.TableRef, core.Schema, []core.Warning, error) {
	cs := core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "v", Type: core.ColumnType{Kind: core.KindString}},
		},
		PrimaryKey: t.PrimaryKey,
	}
	return core.TableRef{Source: t.Source, Target: t.Target, PrimaryKey: t.PrimaryKey}, cs, nil, nil
}

func (Source) InitialPosition(context.Context) (position.Position, error) { return fakePos(0), nil }

func (Source) ParsePosition(s string) (position.Position, error) {
	if s == "" {
		return fakePos(0), nil
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return nil, err
	}
	return fakePos(n), nil
}

func (Source) Open(_ context.Context, refs []source.TableRef) (source.Reader, error) {
	target := "raw.t"
	if len(refs) > 0 {
		target = refs[0].Target
	}
	return &reader{batch: seedBatch(target)}, nil
}

type reader struct {
	batch *dataplane.Batch
	sent  bool
}

var _ source.Reader = &reader{}

// Start is a no-op: the batch is pre-built.
func (r *reader) Start(context.Context, position.Position) error { return nil }

// Next emits the seeded batch once, then holds open until ctx is cancelled.
func (r *reader) Next(ctx context.Context) (*dataplane.Batch, error) {
	if !r.sent {
		r.sent = true
		return r.batch, nil
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

// seedBatch builds the two-row wire batch the fake source emits (id=1 v=a,
// id=2 v=b). The fake cannot import engine internals, so it builds Arrow
// directly.
func seedBatch(target string) *dataplane.Batch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "v", Type: arrow.BinaryTypes.String},
		{Name: "__op", Type: arrow.PrimitiveTypes.Uint8, Nullable: false},
		{Name: "__pos", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "__commit_ts", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "__ingest_ts", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "__snapshot", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
	}, nil)
	bb := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	for i, v := range []string{"a", "b"} {
		bb.Field(0).(*array.Int64Builder).Append(int64(i + 1))
		bb.Field(1).(*array.StringBuilder).Append(v)
		bb.Field(2).(*array.Uint8Builder).Append(0) // insert
		bb.Field(3).(*array.StringBuilder).Append("p")
		bb.Field(4).(*array.TimestampBuilder).AppendNull()
		bb.Field(5).(*array.TimestampBuilder).AppendNull()
		bb.Field(6).(*array.BooleanBuilder).Append(false)
	}
	rec := bb.NewRecordBatch()
	return &dataplane.Batch{Table: target, Record: rec, Watermark: []byte("p2")}
}

func (r reader) Synced() position.Position                         { return fakePos(0) }
func (r reader) Master(context.Context) (position.Position, error) { return fakePos(0), nil }
func (r reader) OpenWindow(context.Context, uint32)                {}
func (r reader) ClearWindow()                                      {}
func (r reader) Close()                                            {}
func (r reader) SetConfirmed(func() position.Position)             {}

// Sink records committed batches in the shared records singleton.
type Sink struct {
	rec *records
}

var _ sink.Sink = &Sink{}

func (s *Sink) EnsureTable(context.Context, core.TableRef, core.Schema, []string, core.CastPolicy, dataplane.WriteMode) error {
	return nil
}

func (s *Sink) Writer(context.Context, core.TableRef, core.CastPolicy, []core.MetadataColumn) (sink.TableWriter, error) {
	return writer{s: s}, nil
}

func (s *Sink) Position(context.Context, core.TableRef) (string, error) { return "", nil }
func (s *Sink) SetProperties(context.Context, core.TableRef, map[string]string) error {
	return nil
}
func (s *Sink) Properties(context.Context, core.TableRef) (map[string]string, error) {
	return nil, nil
}
func (s *Sink) Close() error { return nil }

type writer struct {
	s *Sink
}

var _ sink.TableWriter = writer{}

func (w writer) Commit(_ context.Context, b *dataplane.Batch) error {
	w.s.rec.commit(b)
	return nil
}

func (w writer) Close() error { return nil }

// Package main is an example Go plugin that registers a source and sink
// via the driver registry. Compile with:
//
//	go build -buildmode=plugin -o my_plugin.so
//
// The plugin must export an Init() error function that registers drivers.
package main

import (
	"context"
	"strconv"

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

func main() {}

// Init is the plugin entry point. It registers a source and sink with the
// driver registry. Called by driver.LoadPlugin after plugin.Open.
func Init() error {
	if err := driver.RegisterSource("plugin_demo", source.Capabilities{
		Stream: true,
	}, func(*spec.Spec, source.Runtime) (source.Source, error) {
		return &pluginSource{}, nil
	}); err != nil {
		return err
	}
	return driver.RegisterSink("plugin_demo", func(context.Context, sink.Config) (sink.Sink, error) {
		return &pluginSink{records: newPluginRecords()}, nil
	})
}

// ── Source ───────────────────────────────────────────────────────────

type pluginSource struct{}

func (pluginSource) Introspect(_ context.Context, t spec.Table) (core.TableRef, core.Schema, []core.Warning, error) {
	cs := core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "v", Type: core.ColumnType{Kind: core.KindString}},
		},
		PrimaryKey: t.PrimaryKey,
	}
	return core.TableRef{Source: t.Source, Target: t.Target, PrimaryKey: t.PrimaryKey}, cs, nil, nil
}

func (pluginSource) InitialPosition(context.Context) (position.Position, error) {
	return pluginPos(0), nil
}
func (pluginSource) ParsePosition(s string) (position.Position, error) {
	if s == "" {
		return pluginPos(0), nil
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return nil, err
	}
	return pluginPos(n), nil
}

func (pluginSource) Open(_ context.Context, refs []source.TableRef) (source.Reader, error) {
	return &pluginReader{batch: pluginSeedBatch(refs)}, nil
}

type pluginReader struct {
	batch *dataplane.Batch
	sent  bool
}

func (r *pluginReader) Start(context.Context, position.Position) error { return nil }

func (r *pluginReader) Next(ctx context.Context) (*dataplane.Batch, error) {
	if !r.sent {
		r.sent = true
		return r.batch, nil
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

// pluginSeedBatch builds a two-row wire batch (test-only).
func pluginSeedBatch(refs []source.TableRef) *dataplane.Batch {
	target := "raw.t"
	if len(refs) > 0 {
		target = refs[0].Target
	}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "v", Type: arrow.BinaryTypes.String},
		{Name: "__op", Type: arrow.PrimitiveTypes.Uint8, Nullable: false},
		{Name: "__pos", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "__commit_ts", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "__ingest_ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
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

func (r *pluginReader) Synced() position.Position                         { return pluginPos(0) }
func (r *pluginReader) Master(context.Context) (position.Position, error) { return pluginPos(0), nil }
func (r *pluginReader) OpenWindow(context.Context, uint32)                {}
func (r *pluginReader) ClearWindow()                                      {}
func (r *pluginReader) Close()                                            {}
func (r *pluginReader) SetConfirmed(func() position.Position)             {}

// ── Position ─────────────────────────────────────────────────────────

type pluginPos uint64

func (p pluginPos) String() string                    { return strconv.FormatUint(uint64(p), 10) }
func (p pluginPos) Contains(o position.Position) bool { return p >= o.(pluginPos) }
func (p pluginPos) Compare(o position.Position) int {
	q := o.(pluginPos)
	switch {
	case p < q:
		return -1
	case p > q:
		return 1
	default:
		return 0
	}
}

// ── Sink ─────────────────────────────────────────────────────────────

type pluginRecords struct{}

func newPluginRecords() *pluginRecords {
	return &pluginRecords{}
}

func (r *pluginRecords) commit(b *dataplane.Batch) {
	// QUARANTINE: bridge — accepts *dataplane.Batch but stores nothing yet.
	// Dies when the plugin sink consumes RecordBatch directly.
	_ = b
}

type pluginSink struct {
	records *pluginRecords
}

func (s *pluginSink) EnsureTable(context.Context, core.TableRef, core.Schema, []string, core.CastPolicy, dataplane.WriteMode) error {
	return nil
}

func (s *pluginSink) Writer(context.Context, core.TableRef, core.CastPolicy, []core.MetadataColumn) (sink.TableWriter, error) {
	return &pluginWriter{s: s}, nil
}

func (s *pluginSink) Position(context.Context, core.TableRef) (string, error) { return "", nil }
func (s *pluginSink) SetProperties(context.Context, core.TableRef, map[string]string) error {
	return nil
}
func (s *pluginSink) Properties(context.Context, core.TableRef) (map[string]string, error) {
	return nil, nil
}
func (s *pluginSink) Close() error { return nil }

type pluginWriter struct {
	s *pluginSink
}

func (w *pluginWriter) Commit(_ context.Context, b *dataplane.Batch) error {
	w.s.records.commit(b)
	return nil
}

func (w *pluginWriter) Close() error { return nil }

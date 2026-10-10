package kafka

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/source/kafka/decoder"
	"github.com/maltzsama/urutau/internal/sourcepull"
	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
)

// directTestReader builds a columnar Reader over a Debezium decoder, wired the
// way Open wires a built-in decoder, with the given canonical schemas.
func directTestReader(schemas map[string]core.Schema, refs []source.TableRef) *Reader {
	refBySource := make(map[string]source.TableRef, len(refs))
	topics := make(map[string]string, len(refs))
	for _, ref := range refs {
		refBySource[ref.Source] = ref
		topics[ref.Source] = ref.Target
	}
	return &Reader{
		dec:         &decoder.DebeziumJSON{TopicToTable: topics},
		columnar:    true,
		batchOut:    make(chan *dataplane.Batch, 16),
		encoders:    make(map[string]*transport.RowEncoder),
		schemas:     schemas,
		refBySource: refBySource,
		logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		synced:      &position.Offsets{},
	}
}

// mapPathBatch encodes one record exactly as the row puller would for a
// single-table batch (its shared encode stage).
func mapPathBatch(t *testing.T, d decoder.Decoder, ref source.TableRef, schema core.Schema, rec *kgo.Record) *dataplane.Batch {
	t.Helper()
	changes, err := d.Decode(rec)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	for i := range changes {
		changes[i].Table = ref.Target
		changes[i].Position = recordPosition(rec)
		changes[i].Transport = transportOf(rec)
		decoder.OrderKey(&changes[i], ref.PrimaryKey)
	}
	record, err := sourcepull.EncodeChanges(changes, ref.Target, schema)
	if err != nil {
		t.Fatalf("EncodeChanges: %v", err)
	}
	return &dataplane.Batch{Table: ref.Target, Record: record, Mode: dataplane.UpsertMode}
}

// assertBatchParity compares the direct batch to the map batch: same table,
// same rows, same columns except the wall-clock __ingest_ts.
func assertBatchParity(t *testing.T, direct, mapped *dataplane.Batch) {
	t.Helper()
	if direct.Table != mapped.Table {
		t.Fatalf("table: direct=%q map=%q", direct.Table, mapped.Table)
	}
	if direct.Mode != mapped.Mode {
		t.Fatalf("mode: direct=%v map=%v", direct.Mode, mapped.Mode)
	}
	dr, mr := direct.Record, mapped.Record
	if !dr.Schema().Equal(mr.Schema()) {
		t.Fatalf("schema:\n direct = %v\n map    = %v", dr.Schema(), mr.Schema())
	}
	if dr.NumRows() != mr.NumRows() {
		t.Fatalf("rows: direct=%d map=%d", dr.NumRows(), mr.NumRows())
	}
	for j := 0; j < int(mr.NumCols()); j++ {
		name := mr.Schema().Field(j).Name
		if name == "__ingest_ts" {
			continue
		}
		if !array.Equal(mr.Column(j), dr.Column(j)) {
			t.Fatalf("column %q:\n map    = %s\n direct = %s", name, mr.Column(j), dr.Column(j))
		}
	}
}

func TestDirectConsumeMatchesMapPath(t *testing.T) {
	schema := core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "name", Type: core.ColumnType{Kind: core.KindString}},
		},
		PrimaryKey: []string{"id"},
	}
	ref := source.TableRef{Source: "shop.users", Target: "raw.users", PrimaryKey: []string{"id"}}
	msgs := [][]byte{
		[]byte(`{"op":"c","after":{"id":1,"name":"alice"},"source":{"ts_ms":1700000000000,"db":"shop","table":"users"},"ts_ms":1700000000000}`),
		[]byte(`{"op":"u","before":{"id":1,"name":"alice"},"after":{"id":1,"name":"bob"},"source":{"ts_ms":1700000000001,"db":"shop","table":"users"},"ts_ms":1700000000001}`),
	}
	mapDec := &decoder.DebeziumJSON{TopicToTable: map[string]string{ref.Source: ref.Target}}
	r := directTestReader(map[string]core.Schema{ref.Target: schema}, []source.TableRef{ref})
	ctx := context.Background()

	for i, msg := range msgs {
		rec := &kgo.Record{Topic: ref.Source, Partition: 0, Offset: int64(i), Value: msg}
		if err := r.consumeDirect(ctx, rec); err != nil {
			t.Fatalf("consumeDirect: %v", err)
		}
		if err := r.flushAll(ctx); err != nil {
			t.Fatalf("flushAll: %v", err)
		}
		var direct *dataplane.Batch
		select {
		case direct = <-r.batchOut:
		default:
			t.Fatalf("record %d emitted no batch", i)
		}
		mapped := mapPathBatch(t, mapDec, ref, schema, rec)
		assertBatchParity(t, direct, mapped)
		direct.Record.Release()
		mapped.Record.Release()
	}
}

func TestDirectDeleteFallsBackToMapPath(t *testing.T) {
	schema := core.Schema{
		Columns:    []core.Column{{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}}},
		PrimaryKey: []string{"id"},
	}
	ref := source.TableRef{Source: "s.t", Target: "raw.t", PrimaryKey: []string{"id"}}
	msg := []byte(`{"op":"d","before":{"id":1},"after":null,"source":{"ts_ms":1,"db":"s","table":"t"},"ts_ms":1}`)
	r := directTestReader(map[string]core.Schema{ref.Target: schema}, []source.TableRef{ref})
	ctx := context.Background()

	if err := r.consumeDirect(ctx, &kgo.Record{Topic: ref.Source, Partition: 0, Offset: 3, Value: msg}); err != nil {
		t.Fatalf("consumeDirect: %v", err)
	}
	var direct *dataplane.Batch
	select {
	case direct = <-r.batchOut:
	default:
		t.Fatal("delete fallback emitted no batch")
	}
	defer direct.Record.Release()

	mapped := mapPathBatch(t, &decoder.DebeziumJSON{}, ref, schema, &kgo.Record{Topic: ref.Source, Partition: 0, Offset: 3, Value: msg})
	defer mapped.Record.Release()
	assertBatchParity(t, direct, mapped)
}

func TestDirectDriftFallsBackAndFailsLoud(t *testing.T) {
	schema := core.Schema{
		Columns:    []core.Column{{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}}},
		PrimaryKey: []string{"id"},
	}
	ref := source.TableRef{Source: "s.t", Target: "raw.t", PrimaryKey: []string{"id"}}
	msg := []byte(`{"op":"c","after":{"id":1,"surprise":"x"},"source":{"ts_ms":1,"db":"s","table":"t"},"ts_ms":1}`)
	r := directTestReader(map[string]core.Schema{ref.Target: schema}, []source.TableRef{ref})

	err := r.consumeDirect(context.Background(), &kgo.Record{Topic: ref.Source, Partition: 0, Offset: 0, Value: msg})
	if err == nil || !strings.Contains(err.Error(), "schema drift") {
		t.Fatalf("drift err = %v, want the sourcepull drift error", err)
	}
}

// An unknown column carrying only null is not a hard drift: the map path
// merges it as an all-null padding column. The direct path declines it too,
// so the fallback materializes the same merged schema.
func TestDirectUnknownNullColumnFallsBackAndMerges(t *testing.T) {
	schema := core.Schema{
		Columns:    []core.Column{{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}}},
		PrimaryKey: []string{"id"},
	}
	ref := source.TableRef{Source: "s.t", Target: "raw.t", PrimaryKey: []string{"id"}}
	msg := []byte(`{"op":"c","after":{"id":1,"ghost":null},"source":{"ts_ms":1,"db":"s","table":"t"},"ts_ms":1}`)
	r := directTestReader(map[string]core.Schema{ref.Target: schema}, []source.TableRef{ref})
	ctx := context.Background()

	if err := r.consumeDirect(ctx, &kgo.Record{Topic: ref.Source, Partition: 0, Offset: 0, Value: msg}); err != nil {
		t.Fatalf("consumeDirect: %v", err)
	}
	var direct *dataplane.Batch
	select {
	case direct = <-r.batchOut:
	default:
		t.Fatal("fallback emitted no batch")
	}
	defer direct.Record.Release()

	mapped := mapPathBatch(t, &decoder.DebeziumJSON{}, ref, schema, &kgo.Record{Topic: ref.Source, Partition: 0, Offset: 0, Value: msg})
	defer mapped.Record.Release()
	assertBatchParity(t, direct, mapped)

	var found bool
	for i := 0; i < int(direct.Record.NumCols()); i++ {
		if direct.Record.Schema().Field(i).Name == "ghost" {
			found = true
		}
	}
	if !found {
		t.Fatalf("merged padding column ghost missing: %v", direct.Record.Schema())
	}
}

// A table's buffered rows are emitted as soon as they cross the row ceiling,
// so one fetch of many records becomes several wire batches, not one.
func TestDirectFlushesAtRowCeiling(t *testing.T) {
	schema := core.Schema{
		Columns:    []core.Column{{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}}},
		PrimaryKey: []string{"id"},
	}
	ref := source.TableRef{Source: "s.t", Target: "raw.t", PrimaryKey: []string{"id"}}
	r := directTestReader(map[string]core.Schema{ref.Target: schema}, []source.TableRef{ref})
	ctx := context.Background()

	for i := 0; i < maxDirectRows+1; i++ {
		msg := []byte(`{"op":"c","after":{"id":1},"source":{"ts_ms":1,"db":"s","table":"t"},"ts_ms":1}`)
		if err := r.consumeDirect(ctx, &kgo.Record{Topic: ref.Source, Partition: 0, Offset: int64(i), Value: msg}); err != nil {
			t.Fatalf("consumeDirect %d: %v", i, err)
		}
	}
	if err := r.flushAll(ctx); err != nil {
		t.Fatalf("flushAll: %v", err)
	}
	first := <-r.batchOut
	defer first.Record.Release()
	if first.Record.NumRows() != maxDirectRows {
		t.Fatalf("first batch rows = %d, want %d", first.Record.NumRows(), maxDirectRows)
	}
	second := <-r.batchOut
	defer second.Record.Release()
	if second.Record.NumRows() != 1 {
		t.Fatalf("second batch rows = %d, want 1", second.Record.NumRows())
	}
}

func TestDirectUnmappedTopicSkipped(t *testing.T) {
	schema := core.Schema{Columns: []core.Column{{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}}}}
	ref := source.TableRef{Source: "known", Target: "raw.known"}
	r := directTestReader(map[string]core.Schema{ref.Target: schema}, []source.TableRef{ref})
	msg := []byte(`{"op":"c","after":{"id":1},"source":{"ts_ms":1,"db":"s","table":"unknown"},"ts_ms":1}`)
	if err := r.consumeDirect(context.Background(), &kgo.Record{Topic: "unknown", Partition: 0, Offset: 0, Value: msg}); err != nil {
		t.Fatalf("consumeDirect: %v", err)
	}
	if err := r.flushAll(context.Background()); err != nil {
		t.Fatalf("flushAll: %v", err)
	}
	select {
	case b := <-r.batchOut:
		b.Record.Release()
		t.Fatal("an unmapped topic must emit nothing")
	default:
	}
}

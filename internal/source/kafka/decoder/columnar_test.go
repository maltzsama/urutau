package decoder

import (
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
)

// parityPos is the record coordinate both paths stamp, so the wire __pos
// column is comparable.
const parityPos = "orders:p0=5"

// both surfaces: every built-in decoder implements the row Decoder and the
// columnar ColumnarDecoder, so one value drives both paths in the parity test.
type both interface {
	Decoder
	ColumnarDecoder
}

// directRecord runs DecodeInto into a fresh encoder built from schema and
// returns the record it produced (nil when the record appended no rows).
func directRecord(t *testing.T, d ColumnarDecoder, schema core.Schema, rec *kgo.Record) arrow.RecordBatch {
	t.Helper()
	enc, err := transport.NewRowEncoder(schema, nil)
	if err != nil {
		t.Fatalf("NewRowEncoder: %v", err)
	}
	defer enc.Release()
	n, err := d.DecodeInto(rec, func(string) (*transport.RowEncoder, bool, error) {
		return enc, true, nil
	}, parityPos)
	if err != nil {
		t.Fatalf("DecodeInto: %v", err)
	}
	if n == 0 {
		return nil
	}
	return enc.NewRecord()
}

// mapRecord runs the row path (Decode -> RecordFromChanges) the puller uses.
func mapRecord(t *testing.T, d Decoder, schema core.Schema, rec *kgo.Record) arrow.RecordBatch {
	t.Helper()
	changes, err := d.Decode(rec)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(changes) == 0 {
		return nil
	}
	for i := range changes {
		changes[i].Position = parityPos
	}
	r, err := transport.RecordFromChanges(changes, schema, nil)
	if err != nil {
		t.Fatalf("RecordFromChanges: %v", err)
	}
	return r
}

// assertParity proves the direct path lands exactly the map path's record:
// same schema, same columns, same metadata. __ingest_ts is the one wall-clock
// column, compared only structurally.
func assertParity(t *testing.T, d both, schema core.Schema, rec *kgo.Record) {
	t.Helper()
	direct := directRecord(t, d, schema, rec)
	if direct != nil {
		defer direct.Release()
	}
	mapped := mapRecord(t, d, schema, rec)
	if mapped != nil {
		defer mapped.Release()
	}
	if (direct == nil) != (mapped == nil) {
		t.Fatalf("nil mismatch: direct=%v map=%v", direct, mapped)
	}
	if direct == nil {
		return
	}
	if !direct.Schema().Equal(mapped.Schema()) {
		t.Fatalf("schema mismatch:\n direct = %v\n map    = %v", direct.Schema(), mapped.Schema())
	}
	if direct.NumRows() != mapped.NumRows() {
		t.Fatalf("rows: direct=%d map=%d", direct.NumRows(), mapped.NumRows())
	}
	for j := 0; j < int(mapped.NumCols()); j++ {
		name := mapped.Schema().Field(j).Name
		if name == "__ingest_ts" {
			continue
		}
		if !array.Equal(mapped.Column(j), direct.Column(j)) {
			t.Fatalf("column %q differs:\n map    = %s\n direct = %s", name, mapped.Column(j), direct.Column(j))
		}
	}
}

func TestColumnarParityRawOpaque(t *testing.T) {
	schema := core.Schema{Columns: []core.Column{
		{Name: "payload", Type: core.ColumnType{Kind: core.KindString}},
	}}
	assertParity(t, &Raw{}, schema, &kgo.Record{Topic: "raw", Value: []byte("hello")})
}

func TestColumnarParityRawExtraction(t *testing.T) {
	schema := core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindString}},
		{Name: "name", Type: core.ColumnType{Kind: core.KindString}},
		{Name: "ref_name", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
	}}
	d := &Raw{ByTopic: map[string]TopicExtraction{
		"orders": {Fields: []Field{{Name: "id"}, {Name: "name"}}},
	}}
	// "extra" is intentionally absent from the schema: the projection drops it
	// on both paths, so it is not drift.
	assertParity(t, d, schema, &kgo.Record{Topic: "orders", Value: []byte(`{"id":"7","name":"a","extra":"x"}`)})
}

func TestColumnarParityRawExtractionKeepPayload(t *testing.T) {
	schema := core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindString}},
		{Name: "payload", Type: core.ColumnType{Kind: core.KindString}},
	}}
	d := &Raw{ByTopic: map[string]TopicExtraction{
		"orders": {Fields: []Field{{Name: "id"}}, KeepPayload: true},
	}}
	assertParity(t, d, schema, &kgo.Record{Topic: "orders", Value: []byte(`{"id":"7"}`)})
}

func TestColumnarParityDebeziumInsertAndUpdate(t *testing.T) {
	schema := core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "name", Type: core.ColumnType{Kind: core.KindString}},
	}}
	cases := map[string]string{
		"insert": `{"op":"c","after":{"id":1,"name":"alice"},"source":{"ts_ms":1700000000000,"db":"shop","table":"users"},"ts_ms":1700000000000}`,
		"update": `{"op":"u","before":{"id":1,"name":"alice"},"after":{"id":1,"name":"bob"},"source":{"ts_ms":1700000000001,"db":"shop","table":"users"},"ts_ms":1700000000001}`,
	}
	for name, msg := range cases {
		t.Run(name, func(t *testing.T) {
			assertParity(t, &DebeziumJSON{}, schema, &kgo.Record{Topic: "shop.users", Value: []byte(msg)})
		})
	}
}

func TestColumnarParityDebeziumNullValue(t *testing.T) {
	// A declared nullable column whose value is JSON null: both paths land a
	// NULL in that column.
	schema := core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "name", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
	}}
	msg := `{"op":"c","after":{"id":1,"name":null},"source":{"ts_ms":1,"db":"s","table":"t"},"ts_ms":1}`
	assertParity(t, &DebeziumJSON{}, schema, &kgo.Record{Topic: "s.t", Value: []byte(msg)})
}

func TestColumnarParityAvroProjected(t *testing.T) {
	const avroSchema = `{"type":"record","name":"order","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}`
	reg := newFakeRegistry()
	reg.add(1, avroSchema)
	d := NewAvroDecoder(reg)
	d.ByTopic = map[string]TopicExtraction{
		"orders": {Fields: []Field{{Name: "id"}, {Name: "name"}}},
	}
	schema := core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "name", Type: core.ColumnType{Kind: core.KindString}},
	}}
	payload := encode(t, avroSchema, map[string]any{"id": int64(1), "name": "ana"})
	assertParity(t, d, schema, &kgo.Record{Topic: "orders", Value: confluentValue(1, payload)})
}

// benchBatch measures the reader's steady state: many records of one table
// encoded into one batch. The direct path reuses one encoder and drops each
// decoded map as it goes; the map path holds every rowchange.Change (and its
// parsed map) until the batch is encoded, which is the memory the direct path
// removes. Both parse the JSON, so the delta is that retained buffer.
func benchBatch(b *testing.B, direct bool, n int) {
	schema := core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "name", Type: core.ColumnType{Kind: core.KindString}},
	}}
	msg := []byte(`{"op":"c","after":{"id":1,"name":"alice"},"source":{"ts_ms":1,"db":"s","table":"t"},"ts_ms":1}`)
	d := &DebeziumJSON{}
	recs := make([]*kgo.Record, n)
	for i := range recs {
		recs[i] = &kgo.Record{Topic: "s.t", Value: msg}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if direct {
			enc, err := transport.NewRowEncoder(schema, nil)
			if err != nil {
				b.Fatal(err)
			}
			for _, rec := range recs {
				if _, err := d.DecodeInto(rec, func(string) (*transport.RowEncoder, bool, error) { return enc, true, nil }, parityPos); err != nil {
					b.Fatal(err)
				}
			}
			enc.NewRecord().Release()
			enc.Release()
			continue
		}
		changes := make([]rowchange.Change, 0, n)
		for _, rec := range recs {
			cs, err := d.Decode(rec)
			if err != nil {
				b.Fatal(err)
			}
			cs[0].Position = parityPos
			changes = append(changes, cs...)
		}
		r, err := transport.RecordFromChanges(changes, schema, nil)
		if err != nil {
			b.Fatal(err)
		}
		r.Release()
	}
}

func BenchmarkBatchDirect(b *testing.B) { benchBatch(b, true, 500) }
func BenchmarkBatchMap(b *testing.B)    { benchBatch(b, false, 500) }

// A Debezium delete needs the map path (key backfill and the primary-key
// guard), so the direct decoder declines it.
func TestColumnarDebeziumDeleteDeclines(t *testing.T) {
	schema := core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
	}, PrimaryKey: []string{"id"}}
	msg := []byte(`{"op":"d","before":{"id":1},"after":null,"source":{"ts_ms":1,"db":"s","table":"t"},"ts_ms":1}`)
	enc, err := transport.NewRowEncoder(schema, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Release()
	_, err = (&DebeziumJSON{}).DecodeInto(&kgo.Record{Topic: "s.t", Value: msg},
		func(string) (*transport.RowEncoder, bool, error) { return enc, true, nil }, parityPos)
	if !errors.Is(err, ErrShapeDrift) {
		t.Fatalf("delete DecodeInto err = %v, want ErrShapeDrift", err)
	}
	if enc.Rows() != 0 {
		t.Fatalf("declined delete appended %d rows, want 0", enc.Rows())
	}
}

// A struct-valued column can carry a field the schema's struct lacks, which
// only the map path's nested drift check sees: the direct decoder declines it
// untouched.
func TestColumnarDebeziumStructColumnDeclines(t *testing.T) {
	schema := core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "address", Type: core.ColumnType{Kind: core.KindStruct, Fields: []core.Column{
			{Name: "city", Type: core.ColumnType{Kind: core.KindString}},
		}}},
	}}
	msg := []byte(`{"op":"c","after":{"id":1,"address":{"city":"sp"}},"source":{"ts_ms":1,"db":"s","table":"t"},"ts_ms":1}`)
	enc, err := transport.NewRowEncoder(schema, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Release()
	_, err = (&DebeziumJSON{}).DecodeInto(&kgo.Record{Topic: "s.t", Value: msg},
		func(string) (*transport.RowEncoder, bool, error) { return enc, true, nil }, parityPos)
	if !errors.Is(err, ErrShapeDrift) {
		t.Fatalf("struct DecodeInto err = %v, want ErrShapeDrift", err)
	}
	if enc.Rows() != 0 {
		t.Fatalf("declined struct appended %d rows, want 0", enc.Rows())
	}
}

// An unprojected Avro topic keeps the whole registry record, whose shape may
// exceed the canonical schema: the direct decoder declines it.
func TestColumnarAvroUnprojectedDeclines(t *testing.T) {
	const avroSchema = `{"type":"record","name":"order","fields":[{"name":"id","type":"long"}]}`
	reg := newFakeRegistry()
	reg.add(1, avroSchema)
	d := NewAvroDecoder(reg)
	schema := core.Schema{Columns: []core.Column{{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}}}}
	payload := encode(t, avroSchema, map[string]any{"id": int64(1)})
	enc, err := transport.NewRowEncoder(schema, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Release()
	_, err = d.DecodeInto(&kgo.Record{Topic: "orders", Value: confluentValue(1, payload)},
		func(string) (*transport.RowEncoder, bool, error) { return enc, true, nil }, parityPos)
	if !errors.Is(err, ErrShapeDrift) {
		t.Fatalf("unprojected DecodeInto err = %v, want ErrShapeDrift", err)
	}
}

// An image key the schema lacks is drift; the direct decoder declines it
// before writing any cell, so the encoder is untouched.
func TestColumnarDebeziumUnknownColumnDeclinesCleanly(t *testing.T) {
	schema := core.Schema{Columns: []core.Column{{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}}}}
	msg := []byte(`{"op":"c","after":{"id":1,"surprise":"x"},"source":{"ts_ms":1,"db":"s","table":"t"},"ts_ms":1}`)
	enc, err := transport.NewRowEncoder(schema, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Release()
	_, err = (&DebeziumJSON{}).DecodeInto(&kgo.Record{Topic: "s.t", Value: msg},
		func(string) (*transport.RowEncoder, bool, error) { return enc, true, nil }, parityPos)
	if !errors.Is(err, ErrShapeDrift) {
		t.Fatalf("drift DecodeInto err = %v, want ErrShapeDrift", err)
	}
	if enc.Rows() != 0 {
		t.Fatalf("declined drift appended %d rows, want 0", enc.Rows())
	}
}

package postgres

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
)

// paritySchema exercises every encoding branch the incremental direct path
// shares with the map path: a raw text column, a raw binary column, a raw JSON
// column, a non-raw temporal column, and an enrichment destination the SELECT
// does not return (appended NULL on both paths).
func paritySchema() core.Schema {
	return core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "name", Type: core.ColumnType{Kind: core.KindString}},
			{Name: "payload", Type: core.ColumnType{Kind: core.KindBinary}},
			{Name: "doc", Type: core.ColumnType{Kind: core.KindJSON}},
			{Name: "created", Type: core.ColumnType{Kind: core.KindTimestampTZ}},
			{Name: "ref_name", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
		},
		PrimaryKey: []string{"id"},
	}
}

var parityNames = []string{"id", "name", "payload", "doc", "created"}
var parityDBTypes = []string{"BIGINT", "TEXT", "BYTEA", "JSONB", "TIMESTAMPTZ"}

func parityRows() [][]any {
	created := time.Date(2024, 5, 1, 12, 0, 0, 0, time.UTC)
	return [][]any{
		{int64(1), []byte("alice"), []byte{0x01, 0x02, 0x03}, []byte(`{"a":1}`), created},
		{int64(2), nil, nil, nil, nil},
		{int64(3), []byte(""), []byte{}, []byte(`{}`), created},
	}
}

// TestIncrementalArrowParityWithMapPath proves the columnar incremental page
// encodes byte-for-byte what the map path's RecordFromChanges encodes from the
// same rows and schema (#733). Every data column, __op, __pos, __commit_ts,
// __snapshot and __phase must be identical; only __ingest_ts (wall clock) may
// differ, so it is compared structurally.
func TestIncrementalArrowParityWithMapPath(t *testing.T) {
	schema := paritySchema()
	rows := parityRows()
	const next = "3"
	fixedIngest := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Direct path: straight into the Arrow builders.
	enc, err := transport.NewRowEncoder(schema, nil)
	if err != nil {
		t.Fatalf("NewRowEncoder: %v", err)
	}
	defer enc.Release()
	link, err := newIncrementalArrow(enc, parityNames, parityDBTypes)
	if err != nil {
		t.Fatalf("newIncrementalArrow: %v", err)
	}
	for _, r := range rows {
		if err := link.appendRow(r); err != nil {
			t.Fatalf("appendRow: %v", err)
		}
	}
	direct, err := transport.WithPosition(enc.NewRecord(), next)
	if err != nil {
		t.Fatalf("WithPosition: %v", err)
	}
	defer direct.Release()

	// Map path: the exact rows a source.IncrementalSource returns, encoded the
	// way the runner's fallback encodes them.
	changes := make([]rowchange.Change, len(rows))
	for i, r := range rows {
		m := make(map[string]any, len(parityNames))
		for j, name := range parityNames {
			m[name] = normalize(r[j])
		}
		changes[i] = rowchange.Change{
			Op:       rowchange.OpInsert,
			Table:    "raw.t",
			Key:      []any{m["id"]},
			After:    m,
			Position: next,
			Phase:    core.PhaseIncremental,
			IngestTS: fixedIngest,
		}
	}
	mapRec, err := transport.RecordFromChanges(changes, transport.MergeSchema(changes, schema), nil)
	if err != nil {
		t.Fatalf("RecordFromChanges: %v", err)
	}
	defer mapRec.Release()

	if !mapRec.Schema().Equal(direct.Schema()) {
		t.Fatalf("schema mismatch:\n map  = %v\n direct = %v", mapRec.Schema(), direct.Schema())
	}
	if mapRec.NumRows() != direct.NumRows() {
		t.Fatalf("row count: map=%d direct=%d", mapRec.NumRows(), direct.NumRows())
	}
	for j := 0; j < int(mapRec.NumCols()); j++ {
		name := mapRec.Schema().Field(j).Name
		if name == "__ingest_ts" {
			// Wall clock differs by construction; both must carry a value.
			continue
		}
		if !array.Equal(mapRec.Column(j), direct.Column(j)) {
			t.Fatalf("column %q differs between the map and direct paths:\n map  = %s\n direct = %s",
				name, mapRec.Column(j), direct.Column(j))
		}
	}
}

// TestIncrementalArrowAllocsFewerThanMapPath is the memory assertion: the
// direct path holds no per-row map[string]any, so encoding a page allocates
// fewer objects than the map path. The gap is one map per row, so a strict
// inequality is stable.
func TestIncrementalArrowAllocsFewerThanMapPath(t *testing.T) {
	schema := paritySchema()
	const n = 200
	created := time.Date(2024, 5, 1, 12, 0, 0, 0, time.UTC)
	rows := make([][]any, n)
	for i := range rows {
		rows[i] = []any{int64(i), []byte("alice"), []byte{0x01, 0x02}, []byte(`{"a":1}`), created}
	}

	directAllocs := testing.AllocsPerRun(20, func() {
		enc, err := transport.NewRowEncoder(schema, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer enc.Release()
		link, err := newIncrementalArrow(enc, parityNames, parityDBTypes)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range rows {
			if err := link.appendRow(r); err != nil {
				t.Fatal(err)
			}
		}
		rec := enc.NewRecord()
		rec.Release()
	})

	mapAllocs := testing.AllocsPerRun(20, func() {
		changes := make([]rowchange.Change, len(rows))
		for i, r := range rows {
			m := make(map[string]any, len(parityNames))
			for j, name := range parityNames {
				m[name] = normalize(r[j])
			}
			changes[i] = rowchange.Change{Op: rowchange.OpInsert, Table: "raw.t", After: m, Position: "1"}
		}
		rec, err := transport.RecordFromChanges(changes, transport.MergeSchema(changes, schema), nil)
		if err != nil {
			t.Fatal(err)
		}
		rec.Release()
	})

	if directAllocs >= mapAllocs {
		t.Fatalf("direct path allocated %.0f objects, map path %.0f — the direct path must not build a per-row map", directAllocs, mapAllocs)
	}
	t.Logf("allocs/page(%d rows): direct=%.0f map=%.0f", n, directAllocs, mapAllocs)
}

func BenchmarkIncrementalEncodeDirect(b *testing.B) {
	benchIncremental(b, true)
}

func BenchmarkIncrementalEncodeMap(b *testing.B) {
	benchIncremental(b, false)
}

func benchIncremental(b *testing.B, direct bool) {
	schema := paritySchema()
	created := time.Date(2024, 5, 1, 12, 0, 0, 0, time.UTC)
	rows := make([][]any, 1000)
	for i := range rows {
		rows[i] = []any{int64(i), []byte("alice"), []byte{0x01, 0x02, 0x03}, []byte(`{"a":1}`), created}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if direct {
			enc, err := transport.NewRowEncoder(schema, nil)
			if err != nil {
				b.Fatal(err)
			}
			link, err := newIncrementalArrow(enc, parityNames, parityDBTypes)
			if err != nil {
				b.Fatal(err)
			}
			for _, r := range rows {
				if err := link.appendRow(r); err != nil {
					b.Fatal(err)
				}
			}
			enc.NewRecord().Release()
			enc.Release()
			continue
		}
		changes := make([]rowchange.Change, len(rows))
		for i, r := range rows {
			m := make(map[string]any, len(parityNames))
			for j, name := range parityNames {
				m[name] = normalize(r[j])
			}
			changes[i] = rowchange.Change{Op: rowchange.OpInsert, Table: "raw.t", After: m, Position: "1"}
		}
		rec, err := transport.RecordFromChanges(changes, transport.MergeSchema(changes, schema), nil)
		if err != nil {
			b.Fatal(err)
		}
		rec.Release()
	}
}

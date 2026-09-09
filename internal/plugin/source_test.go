package plugin

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/position"
)

func TestStringPositionCompare(t *testing.T) {
	tests := []struct {
		name     string
		a, b     StringPosition
		expected int
	}{
		// Identity-only: an opaque offset (contract §8.2) has no order other
		// than identity. Different offsets are Incomparable, never < or >.
		{"equal", StringPosition{Offset: "abc"}, StringPosition{Offset: "abc"}, 0},
		{"different", StringPosition{Offset: "aaa"}, StringPosition{Offset: "bbb"}, position.Incomparable},
		{"different-reverse", StringPosition{Offset: "ccc"}, StringPosition{Offset: "bbb"}, position.Incomparable},
		{"empty", StringPosition{}, StringPosition{}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.Compare(tt.b); got != tt.expected {
				t.Errorf("Compare() = %d, want %d", got, tt.expected)
			}
		})
	}
}

func TestStringPositionContains(t *testing.T) {
	a := StringPosition{Offset: "abc"}
	b := StringPosition{Offset: "abc"}
	if !a.Contains(b) {
		t.Error("should contain equal position")
	}

	c := StringPosition{Offset: "aaa"}
	if a.Contains(c) {
		t.Error("should not contain a different offset")
	}
}

func TestStringPositionString(t *testing.T) {
	p := StringPosition{Offset: "test123"}
	if p.String() != "test123" {
		t.Errorf("String() = %q, want %q", p.String(), "test123")
	}
}

func TestArrowToCoreSchema(t *testing.T) {
	arrowSchema := mustArrowSchema(t)
	coreSchema := arrowToCoreSchema(arrowSchema)

	if len(coreSchema.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(coreSchema.Columns))
	}

	names := []string{"id", "name", "active"}
	for i, name := range names {
		if coreSchema.Columns[i].Name != name {
			t.Errorf("column %d: got name %q, want %q", i, coreSchema.Columns[i].Name, name)
		}
	}

	if coreSchema.Columns[0].Type.Kind.String() != "int64" {
		t.Errorf("id kind: got %q, want int64", coreSchema.Columns[0].Type.Kind)
	}
	if coreSchema.Columns[1].Type.Kind.String() != "string" {
		t.Errorf("name kind: got %q, want string", coreSchema.Columns[1].Type.Kind)
	}
	if coreSchema.Columns[2].Type.Kind.String() != "bool" {
		t.Errorf("active kind: got %q, want bool", coreSchema.Columns[2].Type.Kind)
	}
}

func TestColumnIndex(t *testing.T) {
	schema := mustArrowSchema(t)

	if got := columnIndex(schema, "name"); got != 1 {
		t.Errorf("columnIndex(name) = %d, want 1", got)
	}
	if got := columnIndex(schema, "nonexistent"); got != -1 {
		t.Errorf("columnIndex(nonexistent) = %d, want -1", got)
	}
}

func TestExtractPK(t *testing.T) {
	row := map[string]any{"id": int64(42), "name": "test"}
	pk := extractPK([]string{"id"}, row)
	if len(pk) != 1 || pk[0] != int64(42) {
		t.Errorf("extractPK = %v, want [42]", pk)
	}

	row2 := map[string]any{"name": "test"}
	pk2 := extractPK([]string{"id"}, row2)
	if pk2 != nil {
		t.Errorf("extractPK missing col = %v, want nil", pk2)
	}

	pk3 := extractPK([]string{"id"}, nil)
	if pk3 != nil {
		t.Errorf("extractPK nil row = %v, want nil", pk3)
	}
}

func TestRecordsFromReader(t *testing.T) {
	alloc := memory.NewGoAllocator()
	// A wire-schema record: insert, update, delete — the delete carries
	// its image in the flat columns (DELETE IMAGE CONTRACT).
	data, err := transport.CoreSchemaToArrow(core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "name", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
	}, PrimaryKey: []string{"id"}})
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	bld := array.NewRecordBuilder(alloc, data)
	defer bld.Release()

	appendRow := func(op rowchange.Op, id int64, name string) {
		bld.Field(0).(*array.Int64Builder).Append(id)
		bld.Field(1).(*array.StringBuilder).Append(name)
		bld.Field(2).(*array.Uint8Builder).Append(uint8(op))
		bld.Field(3).(*array.StringBuilder).Append("pos" + fmt.Sprint(id))
		bld.Field(4).(*array.TimestampBuilder).Append(0)
		bld.Field(5).(*array.TimestampBuilder).Append(0)
		bld.Field(6).(*array.BooleanBuilder).Append(false)
	}
	appendRow(rowchange.OpInsert, 1, "alice")
	appendRow(rowchange.OpUpdate, 2, "bob")
	appendRow(rowchange.OpDelete, 3, "charlie")
	rec := bld.NewRecordBatch()
	defer rec.Release()

	reader, err := transport.NewBatchReader(rec, []string{"id"})
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	records := recordsFromReader(reader, alloc)
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	defer records[0].Release()

	if records[0].NumRows() != 3 {
		t.Errorf("expected 3 rows, got %d", records[0].NumRows())
	}
	if records[0].NumCols() != 5 { // op, id, name, offset, ts_source
		t.Errorf("expected 5 cols, got %d", records[0].NumCols())
	}
	ops := records[0].Column(0).(*array.String)
	if ops.Value(0) != "c" || ops.Value(1) != "c" || ops.Value(2) != "d" {
		t.Errorf("op column = %q,%q,%q, want c,c,d", ops.Value(0), ops.Value(1), ops.Value(2))
	}
	names := records[0].Column(2).(*array.String)
	if names.Value(0) != "alice" {
		t.Errorf("name = %q, want alice", names.Value(0))
	}
	offsets := records[0].Column(3).(*array.Binary)
	if string(offsets.Value(2)) != "pos3" {
		t.Errorf("delete offset = %q, want pos3", offsets.Value(2))
	}
}

func mustArrowSchema(t *testing.T) *arrow.Schema {
	t.Helper()
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	}, nil)
}

// TestStringPositionOpaqueOffsetsNotOrdered is the regression for the
// audit finding: base64 lexicographic order can INVERT the byte order of the
// encoded offsets, so an opaque offset must never be compared for order.
//
//	C = base64([0xFF, 0x01]) = "/wE="
//	D = base64([0x02, 0x00]) = "AgA="
//
// Byte-wise C > D, but lexicographically "/wE=" < "AgA=" — the old Compare
// returned -1 here, so a resume decision would treat the OLDER offset C as
// "covered" by the NEWER D and skip its batch: silent data loss.
func TestStringPositionOpaqueOffsetsNotOrdered(t *testing.T) {
	older := StringPosition{Offset: "/wE="} // byte-wise [0xFF, 0x01]
	newer := StringPosition{Offset: "AgA="} // byte-wise [0x02, 0x00]

	if c := older.Compare(newer); c != position.Incomparable {
		t.Fatalf("older.Compare(newer) = %d, want Incomparable (lexicographic order is inverted)", c)
	}
	if c := newer.Compare(older); c != position.Incomparable {
		t.Fatalf("newer.Compare(older) = %d, want Incomparable", c)
	}

	// Coverage of the older by the newer must not be decided — that is what
	// would skip the older batch. position.Min must keep the first.
	if older.Contains(newer) {
		t.Fatal("opaque offset must not 'contain' a different offset")
	}
	got := position.Min([]position.Position{older, newer})
	if got.String() != older.String() {
		t.Fatalf("Min kept %s, want the first (%s) when order is undefined", got, older)
	}
}

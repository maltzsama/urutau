package iceberg

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/transport"
)

// V5: a uint64 column (Iceberg has no unsigned) maps to Decimal(20,0) and
// projects losslessly — including values above MaxInt64, which a long
// mapping would silently wrap.
func TestProjectRecordUint64(t *testing.T) {
	cs := core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "big", Type: core.ColumnType{Kind: core.KindUInt64}},
	}, PrimaryKey: []string{"id"}}

	wire, err := transport.CoreSchemaToArrow(cs)
	if err != nil {
		t.Fatalf("wire schema: %v", err)
	}
	bld := array.NewRecordBuilder(memory.DefaultAllocator, wire)
	defer bld.Release()
	bld.Field(0).(*array.Int64Builder).Append(1)
	// A value above MaxInt64 — must survive.
	bld.Field(1).(*array.Uint64Builder).Append(18446744073709551615)
	for j := 2; j < int(wire.NumFields()); j++ {
		bld.Field(j).AppendNull()
	}
	rec := bld.NewRecordBatch()
	defer rec.Release()

	// Target schema: id Int64, big Decimal(20,0).
	target := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "big", Type: &arrow.Decimal128Type{Precision: 20, Scale: 0}},
	}, nil)
	w := &TableWriter{dataSchema: target, metaByName: map[string]core.MetadataColumn{}, cast: core.CastPolicy{}}
	b := &dataplane.Batch{Table: "t", Record: rec, Watermark: []byte("p"), Mode: dataplane.UpsertMode}
	out, err := w.projectRecord(context.Background(), b)
	if err != nil {
		t.Fatalf("projectRecord: %v", err)
	}
	defer out.Release()
	d := out.Column(1).(*array.Decimal128)
	if d.ValueStr(0) != "18446744073709551615" {
		t.Fatalf("big = %q, want the full uint64 value", d.ValueStr(0))
	}
}

// V5 shadow: a uint64 PRIMARY KEY must produce the SAME canonical decimal
// form on the equality-delete path as on the data path. Before the fix,
// scalarValue returned the raw uint64 and deleteRecord failed with
// "cannot append uint64 as decimal" — a delete on a uint64-PK table never
// landed. One value, one wire representation.
func TestDeleteRecordUint64PK(t *testing.T) {
	cs := core.Schema{
		Columns:    []core.Column{{Name: "big", Type: core.ColumnType{Kind: core.KindUInt64}}},
		PrimaryKey: []string{"big"},
	}
	wire, err := transport.CoreSchemaToArrow(cs)
	if err != nil {
		t.Fatalf("wire schema: %v", err)
	}
	bld := array.NewRecordBuilder(memory.DefaultAllocator, wire)
	defer bld.Release()
	bld.Field(0).(*array.Uint64Builder).Append(18446744073709551615) // > MaxInt64
	for j := 1; j < int(wire.NumFields()); j++ {
		bld.Field(j).AppendNull()
	}
	rec := bld.NewRecordBatch()
	defer rec.Release()
	b := &dataplane.Batch{Table: "t", Record: rec, Watermark: []byte("p")}

	keys, err := extractKeys([]*dataplane.Batch{b}, []string{"big"})
	if err != nil {
		t.Fatalf("extractKeys: %v", err)
	}

	// The delete schema mirrors the Iceberg PK type: decimal(20,0).
	w := &TableWriter{
		delCols: []string{"big"},
		delSchema: arrow.NewSchema([]arrow.Field{
			{Name: "big", Type: &arrow.Decimal128Type{Precision: 20, Scale: 0}},
		}, nil),
	}
	dr, err := w.deleteRecord(keys)
	if err != nil {
		t.Fatalf("deleteRecord must succeed for a uint64 PK: %v", err)
	}
	defer dr.Release()
	d := dr.Column(0).(*array.Decimal128)
	if d.ValueStr(0) != "18446744073709551615" {
		t.Fatalf("delete key = %q, want the canonical decimal form of the uint64 PK", d.ValueStr(0))
	}
}

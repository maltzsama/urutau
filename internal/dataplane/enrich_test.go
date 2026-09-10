package dataplane_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/dataplane"
)

func TestCastIntToString(t *testing.T) {
	alloc := checkedAlloc(t)
	b := dataplane.GenerateBatch(42, dataplane.GeneratorOpts{NumRows: 10, Allocator: alloc})
	defer b.Release()

	policy := dataplane.CastPolicy{"id": {Type: core.ColumnType{Kind: core.KindString}}}
	out, _, err := dataplane.Cast(context.Background(), b, policy)
	if err != nil {
		t.Fatalf("Cast: %v", err)
	}
	defer out.Release()

	idField := out.Record.Schema().Field(0)
	if idField.Type.ID() != arrow.STRING {
		t.Errorf("expected string type after cast, got %v", idField.Type)
	}
	if out.Record.NumRows() != 10 {
		t.Errorf("expected 10 rows, got %d", out.Record.NumRows())
	}
}

func TestCastPreservesNonCastColumns(t *testing.T) {
	alloc := checkedAlloc(t)
	b := dataplane.GenerateBatch(42, dataplane.GeneratorOpts{NumRows: 5, Allocator: alloc})
	defer b.Release()

	policy := dataplane.CastPolicy{"id": {Type: core.ColumnType{Kind: core.KindString}}}
	out, _, err := dataplane.Cast(context.Background(), b, policy)
	if err != nil {
		t.Fatalf("Cast: %v", err)
	}
	defer out.Release()

	valField := out.Record.Schema().Field(1)
	if valField.Name != "val" {
		t.Errorf("unexpected field name: %s", valField.Name)
	}
}

func TestCastEmptyPolicy(t *testing.T) {
	alloc := checkedAlloc(t)
	b := dataplane.GenerateBatch(42, dataplane.GeneratorOpts{NumRows: 5, Allocator: alloc})
	defer b.Release()

	out, _, err := dataplane.Cast(context.Background(), b, nil)
	if err != nil {
		t.Fatalf("Cast: %v", err)
	}
	if out != b {
		t.Error("expected same batch returned for nil policy")
	}
}

func TestCastPreservesWatermark(t *testing.T) {
	alloc := checkedAlloc(t)
	b := dataplane.GenerateBatch(42, dataplane.GeneratorOpts{NumRows: 5, Allocator: alloc})
	defer b.Release()

	policy := dataplane.CastPolicy{"id": {Type: core.ColumnType{Kind: core.KindString}}}
	out, _, err := dataplane.Cast(context.Background(), b, policy)
	if err != nil {
		t.Fatalf("Cast: %v", err)
	}
	defer out.Release()

	if string(out.Watermark) != string(b.Watermark) {
		t.Errorf("Watermark changed: %q -> %q", b.Watermark, out.Watermark)
	}
	if out.Table != b.Table {
		t.Errorf("Table changed: %q -> %q", b.Table, out.Table)
	}
}

func TestCastNoOpSameType(t *testing.T) {
	alloc := checkedAlloc(t)
	b := dataplane.GenerateBatch(42, dataplane.GeneratorOpts{NumRows: 5, Allocator: alloc})
	defer b.Release()

	policy := dataplane.CastPolicy{"val": {Type: core.ColumnType{Kind: core.KindString}}}
	out, _, err := dataplane.Cast(context.Background(), b, policy)
	if err != nil {
		t.Fatalf("Cast: %v", err)
	}
	defer out.Release()

	if out.Record.NumRows() != 5 {
		t.Errorf("expected 5 rows, got %d", out.Record.NumRows())
	}
}

func TestCastPreservesNullability(t *testing.T) {
	alloc := checkedAlloc(t)
	b := dataplane.GenerateBatch(42, dataplane.GeneratorOpts{NumRows: 5, Allocator: alloc})
	defer b.Release()

	policy := dataplane.CastPolicy{"id": {Type: core.ColumnType{Kind: core.KindString}}}
	out, _, err := dataplane.Cast(context.Background(), b, policy)
	if err != nil {
		t.Fatalf("Cast: %v", err)
	}
	defer out.Release()

	idField := out.Record.Schema().Field(0)
	if idField.Nullable {
		t.Error("expected id Nullable: false preserved after cast")
	}
}

func TestCastPolicyUnknownColumnIsError(t *testing.T) {
	alloc := checkedAlloc(t)
	b := dataplane.GenerateBatch(1, dataplane.GeneratorOpts{NumRows: 5, Allocator: alloc})
	defer b.Release()

	// W-1: a policy naming a column the batch does not carry is a spec
	// typo — fail loudly instead of silently no-oping.
	_, _, err := dataplane.Cast(context.Background(), b,
		dataplane.CastPolicy{"nope": {Type: core.ColumnType{Kind: core.KindInt64}}})
	if err == nil {
		t.Fatal("unknown policy column must error")
	}
}

func TestCastInt64OverflowPreserved(t *testing.T) {
	alloc := checkedAlloc(t)
	b := dataplane.AdversarialInt64Overflow(alloc)
	defer b.Release()

	policy := dataplane.CastPolicy{"id": {Type: core.ColumnType{Kind: core.KindString}}}
	out, _, err := dataplane.Cast(context.Background(), b, policy)
	if err != nil {
		t.Fatalf("Cast: %v", err)
	}
	defer out.Release()

	idArr := out.Record.Column(0).(*array.String)
	for i := range idArr.Len() {
		val := idArr.Value(i)
		if len(val) == 0 {
			t.Errorf("row %d: empty string after cast", i)
		}
	}
}

func TestCastStructReturnsError(t *testing.T) {
	alloc := checkedAlloc(t)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: arrow.StructOf(arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int64}), Nullable: true},
	}, nil)
	bb := array.NewRecordBuilder(alloc, schema)
	bb.Field(0).(*array.StructBuilder).Append(true)
	bb.Field(0).(*array.StructBuilder).FieldBuilder(0).(*array.Int64Builder).Append(1)
	rec := bb.NewRecordBatch()
	bb.Release()
	defer rec.Release()

	b := &dataplane.Batch{Table: "t", Record: rec, Watermark: []byte("p")}
	_, _, err := dataplane.Cast(context.Background(), b, dataplane.CastPolicy{
		"s": {Type: core.ColumnType{Kind: core.KindString}},
	})
	if err == nil {
		t.Error("expected error for struct → string cast (no columnar executor yet)")
	}
}

func TestCastListReturnsError(t *testing.T) {
	alloc := checkedAlloc(t)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "lst", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64), Nullable: true},
	}, nil)
	bb := array.NewRecordBuilder(alloc, schema)
	bb.Field(0).(*array.ListBuilder).Append(true)
	bb.Field(0).(*array.ListBuilder).ValueBuilder().(*array.Int64Builder).Append(1)
	rec := bb.NewRecordBatch()
	bb.Release()
	defer rec.Release()

	b := &dataplane.Batch{Table: "t", Record: rec, Watermark: []byte("p")}
	_, _, err := dataplane.Cast(context.Background(), b, dataplane.CastPolicy{
		"lst": {Type: core.ColumnType{Kind: core.KindString}},
	})
	if err == nil {
		t.Error("expected error for list → string cast (no columnar executor yet)")
	}
}

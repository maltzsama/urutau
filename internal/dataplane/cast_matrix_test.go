package dataplane_test

// T-14 (W-1 / D-4): dataplane.Cast obeys the core matrix — narrowing and
// parse casts are vetoed even though the arrow kernel could technically
// execute them; binary → string(hex) runs the explicit encoding kernel;
// timestamp → timestamptz(assume_utc) works and warns.

import (
	"context"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/dataplane"
)

// binaryPayloadBatch: a wire-schema batch with a binary payload column.
func binaryPayloadBatch(t *testing.T, alloc memory.Allocator) *dataplane.Batch {
	t.Helper()
	data := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "payload", Type: arrow.BinaryTypes.Binary, Nullable: true},
	}, nil)
	bld := array.NewRecordBuilder(alloc, data)
	defer bld.Release()
	bld.Field(0).(*array.Int64Builder).Append(1)
	bld.Field(1).(*array.BinaryBuilder).Append([]byte{0xde, 0xad, 0xbe, 0xef})
	rec := bld.NewRecordBatch()
	return &dataplane.Batch{Table: "t", Record: rec, Watermark: []byte("w")}
}

func TestCastVetoesNarrowingEvenThoughArrowCould(t *testing.T) {
	alloc := checkedAlloc(t)
	b := dataplane.GenerateBatch(42, dataplane.GeneratorOpts{NumRows: 3, Allocator: alloc})
	defer b.Release()

	// id is int64: int64 → int32 is matrix-narrowing, though compute
	// would happily truncate.
	_, _, err := dataplane.Cast(context.Background(), b, dataplane.CastPolicy{
		"id": {Type: core.ColumnType{Kind: core.KindInt32}},
	})
	if err == nil {
		t.Fatal("int64 → int32 must be vetoed by the matrix")
	}
	if !strings.Contains(err.Error(), "int32 is not allowed") {
		t.Errorf("error must cite the matrix, got: %v", err)
	}
}

func TestCastVetoesParseCast(t *testing.T) {
	alloc := checkedAlloc(t)
	b := dataplane.GenerateBatch(42, dataplane.GeneratorOpts{NumRows: 3, Allocator: alloc})
	defer b.Release()

	// val is string: string → int64 is a parse, not a cast — matrix veto.
	_, _, err := dataplane.Cast(context.Background(), b, dataplane.CastPolicy{
		"val": {Type: core.ColumnType{Kind: core.KindInt64}},
	})
	if err == nil {
		t.Fatal("string → int64 must be vetoed by the matrix")
	}
}

func TestCastBinaryToHexString(t *testing.T) {
	alloc := checkedAlloc(t)
	b := binaryPayloadBatch(t, alloc)
	defer b.Release()

	out, _, err := dataplane.Cast(context.Background(), b, dataplane.CastPolicy{
		"payload": {Type: core.ColumnType{Kind: core.KindString}, Encoding: "hex"},
	})
	if err != nil {
		t.Fatalf("Cast: %v", err)
	}
	defer out.Release()

	payload := out.Record.Column(1).(*array.String)
	if got, want := payload.Value(0), hex.EncodeToString([]byte{0xde, 0xad, 0xbe, 0xef}); got != want {
		t.Errorf("hex payload = %q, want %q", got, want)
	}
}

func TestCastBinaryToPlainStringIsAmbiguous(t *testing.T) {
	alloc := checkedAlloc(t)
	b := binaryPayloadBatch(t, alloc)
	defer b.Release()

	_, _, err := dataplane.Cast(context.Background(), b, dataplane.CastPolicy{
		"payload": {Type: core.ColumnType{Kind: core.KindString}},
	})
	if err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("plain binary → string must error as ambiguous, got: %v", err)
	}
}

func TestCastNaiveTimestampToTimestamptzAssumeUTC(t *testing.T) {
	alloc := checkedAlloc(t)

	// Batch with a naive timestamp column (no TZ on the wire type).
	data := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond}}, // naive
	}, nil)
	bld := array.NewRecordBuilder(alloc, data)
	defer bld.Release()
	bld.Field(0).(*array.Int64Builder).Append(1)
	bld.Field(1).(*array.TimestampBuilder).Append(arrow.Timestamp(1_700_000_000_000_000)) // µs
	rec := bld.NewRecordBatch()
	b := &dataplane.Batch{Table: "t", Record: rec, Watermark: []byte("w")}
	defer b.Release()

	out, warns, err := dataplane.Cast(context.Background(), b, dataplane.CastPolicy{
		"ts": {Type: core.ColumnType{Kind: core.KindTimestampTZ}, AssumeUTC: true},
	})
	if err != nil {
		t.Fatalf("Cast: %v", err)
	}
	defer out.Release()

	if len(warns) != 0 {
		t.Errorf("assume_utc cast is clean, got warnings %v", warns)
	}
	tsField := out.Record.Schema().Field(1)
	tt, ok := tsField.Type.(*arrow.TimestampType)
	if !ok || tt.TimeZone != "UTC" {
		t.Fatalf("ts type = %v, want Timestamp(µs, UTC)", tsField.Type)
	}
	// The value is asserted, never shifted: same ticks.
	if got := out.Record.Column(1).(*array.Timestamp).Value(0); got != 1_700_000_000_000_000 {
		t.Errorf("ticks changed under assume_utc: %d", got)
	}
}

func TestCastWithoutAssumeUTCIsVetoed(t *testing.T) {
	alloc := checkedAlloc(t)
	data := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond}}, // naive
	}, nil)
	bld := array.NewRecordBuilder(alloc, data)
	defer bld.Release()
	bld.Field(0).(*array.Int64Builder).Append(1)
	bld.Field(1).(*array.TimestampBuilder).Append(0)
	rec := bld.NewRecordBatch()
	b := &dataplane.Batch{Table: "t", Record: rec, Watermark: []byte("w")}
	defer b.Release()

	_, _, err := dataplane.Cast(context.Background(), b, dataplane.CastPolicy{
		"ts": {Type: core.ColumnType{Kind: core.KindTimestampTZ}}, // no AssumeUTC
	})
	if err == nil || !strings.Contains(err.Error(), "assume_utc") {
		t.Fatalf("timestamp → timestamptz without assume_utc must be vetoed, got: %v", err)
	}
}

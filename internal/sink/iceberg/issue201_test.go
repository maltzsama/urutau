package iceberg

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"

	"github.com/maltzsama/urutau/core"
)

// #201.1: an int64 beyond 2^53 cannot be represented exactly as float64 and
// must be rejected instead of rounded silently.
func TestAppendColumnInt64Float64Precision(t *testing.T) {
	b := array.NewFloat64Builder(memory.DefaultAllocator)
	defer b.Release()
	field := arrow.Field{Name: "f", Type: arrow.PrimitiveTypes.Float64}

	if err := appendColumn(b, field, []any{maxExactFloat64}); err != nil {
		t.Fatalf("2^53 is exactly representable, got %v", err)
	}
	if err := appendColumn(b, field, []any{-maxExactFloat64}); err != nil {
		t.Fatalf("-2^53 is exactly representable, got %v", err)
	}
	if err := appendColumn(b, field, []any{maxExactFloat64 + 1}); err == nil {
		t.Fatal("2^53+1 loses precision as float64, want an error")
	}
	if err := appendColumn(b, field, []any{-maxExactFloat64 - 1}); err == nil {
		t.Fatal("-2^53-1 loses precision as float64, want an error")
	}
}

// #201.4: NewTableWriter rejects an empty primary key before touching the
// catalog (a nil catalog proves the early return).
func TestNewTableWriterRejectsEmptyPrimaryKey(t *testing.T) {
	_, err := NewTableWriter(context.Background(), nil, table.Identifier{"raw", "t"}, nil, core.CastPolicy{}, nil, "", 0)
	if err == nil || !strings.Contains(err.Error(), "primary key") {
		t.Fatalf("empty primary key = %v, want a primary-key error", err)
	}
}

// #201.5: the legacy descriptor kill switch rejects the pre-fingerprint magic
// when set, and attempts it (decoding) when unset.
func TestRejectLegacyStagedKillSwitch(t *testing.T) {
	old := rejectLegacyStaged
	t.Cleanup(func() { rejectLegacyStaged = old })

	rejectLegacyStaged = true
	_, err := decodeStaged([]byte{stagedMagic}, iceberg.PartitionSpec{}, nil, 2)
	if err == nil || !strings.Contains(err.Error(), "rejected") {
		t.Fatalf("kill switch on: legacy descriptor = %v, want rejected", err)
	}

	rejectLegacyStaged = false
	_, err = decodeStaged([]byte{stagedMagic}, iceberg.PartitionSpec{}, nil, 2)
	if err != nil && strings.Contains(err.Error(), "rejected") {
		t.Fatalf("kill switch off: the legacy format must be attempted, got %v", err)
	}
}

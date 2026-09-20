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
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
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

// #201.4: an upsert commit needs an equality-delete key; the writer rejects an
// empty one before key extraction fails obscurely. An append-only table (no
// primary key) never reaches this branch, so it must still construct.
func TestCommitUpsertRequiresPrimaryKey(t *testing.T) {
	b := wireBatch(t, [3]any{int64(1), "x", rowchange.OpInsert})
	defer b.Release()
	b.Mode = dataplane.UpsertMode

	w := &TableWriter{ident: table.Identifier{"raw", "t"}}
	if err := w.Commit(context.Background(), b); err == nil || !strings.Contains(err.Error(), "primary key") {
		t.Fatalf("upsert with no primary key = %v, want a primary-key error", err)
	}
}

// An append-only table has no primary key; NewTableWriter must still build.
func TestNewTableWriterAllowsEmptyPrimaryKeyForAppend(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	ref := core.TableRef{Target: "events"} // append-only: no primary key
	if err := s.EnsureTable(ctx, ref, canonicalSchema(), nil, core.CastPolicy{}, dataplane.AppendMode); err != nil {
		t.Fatalf("EnsureTable(append): %v", err)
	}
	if _, err := NewTableWriter(ctx, s.cat, s.ident("events"), nil, core.CastPolicy{}, nil, "src.events", 0); err != nil {
		t.Fatalf("an append-only table must construct a writer with no primary key: %v", err)
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

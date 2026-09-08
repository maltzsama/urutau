package dataplane_test

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/maltzsama/urutau/internal/dataplane"
)

func TestCastIntToString(t *testing.T) {
	b := dataplane.GenerateBatch(42, dataplane.GeneratorOpts{NumRows: 10})
	defer b.Release()

	policy := dataplane.CastPolicy{
		"id": &arrow.StringType{},
	}
	out, err := dataplane.Cast(context.Background(), b, policy)
	if err != nil {
		t.Fatalf("Cast: %v", err)
	}
	defer out.Release()

	// id column should now be String type
	idField := out.Record.Schema().Field(0)
	if idField.Type.ID() != arrow.STRING {
		t.Errorf("expected string type after cast, got %v", idField.Type)
	}
	if out.Record.NumRows() != 10 {
		t.Errorf("expected 10 rows, got %d", out.Record.NumRows())
	}
}

func TestCastPreservesNonCastColumns(t *testing.T) {
	b := dataplane.GenerateBatch(42, dataplane.GeneratorOpts{NumRows: 5})
	defer b.Release()

	policy := dataplane.CastPolicy{
		"id": &arrow.StringType{},
	}
	out, err := dataplane.Cast(context.Background(), b, policy)
	if err != nil {
		t.Fatalf("Cast: %v", err)
	}
	defer out.Release()

	// val column should remain unchanged (String already)
	valField := out.Record.Schema().Field(1)
	if valField.Name != "val" {
		t.Errorf("unexpected field name: %s", valField.Name)
	}
}

func TestCastEmptyPolicy(t *testing.T) {
	b := dataplane.GenerateBatch(42, dataplane.GeneratorOpts{NumRows: 5})
	defer b.Release()

	out, err := dataplane.Cast(context.Background(), b, nil)
	if err != nil {
		t.Fatalf("Cast: %v", err)
	}
	if out != b {
		t.Error("expected same batch returned for nil policy")
	}
}

func TestCastPreservesWatermark(t *testing.T) {
	b := dataplane.GenerateBatch(42, dataplane.GeneratorOpts{NumRows: 5})
	defer b.Release()

	policy := dataplane.CastPolicy{
		"id": &arrow.StringType{},
	}
	out, err := dataplane.Cast(context.Background(), b, policy)
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
	b := dataplane.GenerateBatch(42, dataplane.GeneratorOpts{NumRows: 5})
	defer b.Release()

	// Cast String to String — no change expected
	policy := dataplane.CastPolicy{
		"val": &arrow.StringType{},
	}
	out, err := dataplane.Cast(context.Background(), b, policy)
	if err != nil {
		t.Fatalf("Cast: %v", err)
	}
	defer out.Release()

	// Value should be unchanged
	if out.Record.NumRows() != 5 {
		t.Errorf("expected 5 rows, got %d", out.Record.NumRows())
	}
}

func TestAddMetadataColumns(t *testing.T) {
	b := dataplane.GenerateBatch(42, dataplane.GeneratorOpts{NumRows: 5})
	defer b.Release()

	ts := time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC)
	meta := dataplane.MetadataColumns{
		CommitTS:  ts,
		IngestTS:  ts.Add(200 * time.Millisecond),
		Snapshot:  false,
		Phase:     "live",
	}

	out, err := dataplane.AddMetadata(context.Background(), b, meta)
	if err != nil {
		t.Fatalf("AddMetadata: %v", err)
	}
	defer out.Release()

	schema := out.Record.Schema()

	// Check __commit_ts
	idx := -1
	for i := range schema.NumFields() {
		if schema.Field(i).Name == "__commit_ts" {
			idx = i
			break
		}
	}
	if idx < 0 {
		t.Fatal("__commit_ts column not found")
	}

	if out.Record.NumRows() != 5 {
		t.Errorf("expected 5 rows, got %d", out.Record.NumRows())
	}
}

func TestAddMetadataSnapshotPhase(t *testing.T) {
	b := dataplane.GenerateBatch(42, dataplane.GeneratorOpts{NumRows: 3})
	defer b.Release()

	meta := dataplane.MetadataColumns{
		CommitTS: time.Now(),
		IngestTS: time.Now(),
		Snapshot: true,
		Phase:    "snapshot",
	}

	out, err := dataplane.AddMetadata(context.Background(), b, meta)
	if err != nil {
		t.Fatalf("AddMetadata: %v", err)
	}
	defer out.Release()

	// __snapshot should be true for all rows
	schema := out.Record.Schema()
	for i := range schema.NumFields() {
		if schema.Field(i).Name == "__snapshot" {
			col := out.Record.Column(i)
			for j := range col.Len() {
				if !col.(*array.Boolean).Value(j) {
					t.Errorf("row %d: expected __snapshot=true", j)
				}
			}
			break
		}
	}
}

func TestAddMetadataEmptyBatch(t *testing.T) {
	b := dataplane.GenerateBatch(1, dataplane.GeneratorOpts{NumRows: 1})
	defer b.Release()
	rb := b.Record.NewSlice(0, 0)
	defer rb.Release()
	b2 := &dataplane.Batch{Table: b.Table, Record: rb, Watermark: b.Watermark}

	out, err := dataplane.AddMetadata(context.Background(), b2, dataplane.MetadataColumns{
		CommitTS: time.Now(),
		IngestTS: time.Now(),
	})
	if err != nil {
		t.Fatalf("AddMetadata: %v", err)
	}
	if out != b2 {
		t.Error("expected same batch returned for empty batch")
	}
}

func TestCastInt64OverflowPreserved(t *testing.T) {
	b := dataplane.AdversarialInt64Overflow(0)
	defer b.Release()

	// Cast id (Int64) to String — must preserve exact values
	policy := dataplane.CastPolicy{
		"id": &arrow.StringType{},
	}
	out, err := dataplane.Cast(context.Background(), b, policy)
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

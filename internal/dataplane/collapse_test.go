package dataplane_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/maltzsama/urutau/internal/dataplane"
)

func TestCollapseBasic(t *testing.T) {
	b := dataplane.GenerateBatch(1, dataplane.GeneratorOpts{NumRows: 10})
	defer b.Release()

	ups, dels, err := dataplane.Collapse(context.Background(), b, []string{"id"})
	if err != nil {
		t.Fatalf("Collapse: %v", err)
	}
	defer func() {
		if ups != nil {
			ups.Release()
		}
		if dels != nil {
			dels.Release()
		}
	}()

	total := 0
	if ups != nil {
		total += int(ups.Record.NumRows())
	}
	if dels != nil {
		total += int(dels.Record.NumRows())
	}
	// Seed 1, 10 rows: IDs range from 1-10, but some may collide depending on
	// the generator. Collapse should keep exactly 1 row per unique PK.
	if total < 1 || total > 10 {
		t.Errorf("unexpected collapsed row count %d", total)
	}
}

func TestCollapseDeleteLast(t *testing.T) {
	b := dataplane.AdversarialDeleteLast(0)
	defer b.Release()

	ups, dels, err := dataplane.Collapse(context.Background(), b, []string{"id"})
	if err != nil {
		t.Fatalf("Collapse: %v", err)
	}
	defer func() {
		if ups != nil {
			ups.Release()
		}
		if dels != nil {
			dels.Release()
		}
	}()

	// PK=1 last op is DELETE → should be in deletes, not upserts
	if ups != nil {
		t.Errorf("expected no upserts for delete-last, got %d", ups.Record.NumRows())
	}
	if dels == nil || dels.Record.NumRows() != 1 {
		t.Errorf("expected 1 delete, got %v", dels)
	}
}

func TestCollapseInsertAfterDelete(t *testing.T) {
	b := dataplane.AdversarialInsertAfterDelete(0)
	defer b.Release()

	ups, dels, err := dataplane.Collapse(context.Background(), b, []string{"id"})
	if err != nil {
		t.Fatalf("Collapse: %v", err)
	}
	defer func() {
		if ups != nil {
			ups.Release()
		}
		if dels != nil {
			dels.Release()
		}
	}()

	// Last occurrence is INSERT → should be in upserts
	if ups == nil || ups.Record.NumRows() != 1 {
		t.Errorf("expected 1 upsert for reinsert, got %v", ups)
	}
	if dels != nil {
		t.Errorf("expected no deletes, got %d", dels.Record.NumRows())
	}
}

func TestCollapseCompositeKey(t *testing.T) {
	b := dataplane.AdversarialCompositeKey(0)
	defer b.Release()

	ups, dels, err := dataplane.Collapse(context.Background(), b, []string{"pk1", "pk2"})
	if err != nil {
		t.Fatalf("Collapse: %v", err)
	}
	defer func() {
		if ups != nil {
			ups.Release()
		}
		if dels != nil {
			dels.Release()
		}
	}()

	// Two distinct PKs → both should survive
	total := 0
	if ups != nil {
		total += int(ups.Record.NumRows())
	}
	if dels != nil {
		total += int(dels.Record.NumRows())
	}
	if total != 2 {
		t.Errorf("expected 2 rows for distinct composite keys, got %d", total)
	}
}

func TestCollapseNullPK(t *testing.T) {
	b := dataplane.GenerateBatch(1, dataplane.GeneratorOpts{
		NumRows:       3,
		IncludeNullPK: true,
	})
	defer b.Release()

	_, _, err := dataplane.Collapse(context.Background(), b, []string{"id"})
	if err == nil {
		t.Fatal("expected error for null PK")
	}
}

func TestCollapsePreservesWatermark(t *testing.T) {
	b := dataplane.GenerateBatch(42, dataplane.GeneratorOpts{NumRows: 10})
	defer b.Release()

	ups, _, err := dataplane.Collapse(context.Background(), b, []string{"id"})
	if err != nil {
		t.Fatalf("Collapse: %v", err)
	}
	defer func() {
		if ups != nil {
			ups.Release()
		}
	}()

	if ups != nil && string(ups.Watermark) != string(b.Watermark) {
		t.Errorf("Watermark changed: %q -> %q", b.Watermark, ups.Watermark)
	}
}

func TestCollapseEmptyBatch(t *testing.T) {
	// Build a zero-row batch
	rb := dataplane.GenerateBatch(1, dataplane.GeneratorOpts{NumRows: 1})
	defer rb.Release()
	empty := rb.Record.NewSlice(0, 0)
	defer empty.Release()

	b := &dataplane.Batch{Table: rb.Table, Record: empty, Watermark: rb.Watermark}

	ups, dels, err := dataplane.Collapse(context.Background(), b, []string{"id"})
	if err != nil {
		t.Fatalf("Collapse: %v", err)
	}
	if ups != nil {
		ups.Release()
		t.Error("expected nil upserts for empty batch")
	}
	if dels != nil {
		dels.Release()
		t.Error("expected nil deletes for empty batch")
	}
}

func TestCollapseInt64Overflow(t *testing.T) {
	b := dataplane.AdversarialInt64Overflow(0)
	defer b.Release()

	ups, _, err := dataplane.Collapse(context.Background(), b, []string{"id"})
	if err != nil {
		t.Fatalf("Collapse: %v", err)
	}
	defer func() {
		if ups != nil {
			ups.Release()
		}
	}()

	if ups == nil || ups.Record.NumRows() != 2 {
		t.Errorf("expected 2 upserts for 2 distinct int64 PKs, got %v", ups)
	}

	// Verify int64 values preserved
	if ups != nil {
		bigCol := ups.Record.Column(1).(*array.Int64)
		if bigCol.Value(0) != (int64(1)<<53)+1 {
			t.Errorf("int64 overflow: got %d, want %d", bigCol.Value(0), (int64(1)<<53)+1)
		}
	}
}

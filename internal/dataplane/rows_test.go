package dataplane_test

import (
	"strings"
	"testing"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	publicdp "github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/dataplane"
	"github.com/maltzsama/urutau/internal/dataplane/dataplanetest"
)

// payloadRecord builds an id/v wire-shaped record with n rows whose v cell is
// size bytes, so a copy can be detected by the data buffer's address.
func payloadRecord(t *testing.T, alloc memory.Allocator, n, size int) arrow.RecordBatch {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "v", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	bb := array.NewRecordBuilder(alloc, schema)
	defer bb.Release()
	for i := range n {
		bb.Field(0).(*array.Int64Builder).Append(int64(i + 1))
		bb.Field(1).(*array.StringBuilder).Append(strings.Repeat("x", size))
	}
	return bb.NewRecordBatch()
}

// vData is the address of the v column's data buffer: equal addresses mean
// the rows were not copied.
func vData(t *testing.T, rec arrow.RecordBatch) uintptr {
	t.Helper()
	for i := range int(rec.NumCols()) {
		if rec.ColumnName(i) == "v" {
			b := rec.Column(i).Data().Buffers()[2].Bytes()
			return uintptr(unsafe.Pointer(&b[0]))
		}
	}
	t.Fatal("no v column")
	return 0
}

func TestCountOpsNilBatch(t *testing.T) {
	u, d := dataplane.CountOps(nil)
	if u != 0 || d != 0 {
		t.Errorf("CountOps(nil) = %d, %d, want 0, 0", u, d)
	}
}

func TestCountOpsSplitsByOp(t *testing.T) {
	alloc := checkedAlloc(t)
	b := dataplanetest.GenerateBatch(1, dataplanetest.GeneratorOpts{NumRows: 50, Allocator: alloc})
	defer b.Release()
	up, del := dataplane.CountOps(b)
	if up+del != int(b.Record.NumRows()) {
		t.Fatalf("upserts %d + deletes %d != rows %d", up, del, b.Record.NumRows())
	}
}

// Keeping every row in order is the batch itself: SelectRows must not copy it.
func TestSelectRowsEveryRowDoesNotCopy(t *testing.T) {
	alloc := checkedAlloc(t)
	rec := payloadRecord(t, alloc, 100, 1024)
	b := &publicdp.Batch{Table: "raw.orders", Record: rec, Mode: publicdp.UpsertMode}
	defer b.Release()

	idx := make([]int32, 100)
	for i := range idx {
		idx[i] = int32(i)
	}
	out, err := dataplane.SelectRows(b, idx, "p100", publicdp.AppendMode, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if vData(t, out.Record) != vData(t, b.Record) {
		t.Fatal("selecting every row in order copied the batch")
	}
	if string(out.Watermark) != "p100" || out.Mode != publicdp.AppendMode {
		t.Fatalf("watermark %q mode %v", out.Watermark, out.Mode)
	}
}

// SelectRows carries the cycle key and staged flag of the source batch.
func TestSelectRowsCarriesSeqStaged(t *testing.T) {
	alloc := checkedAlloc(t)
	rec := payloadRecord(t, alloc, 3, 8)
	b := &publicdp.Batch{Table: "t", Record: rec, Seq: 9, Staged: true}
	defer b.Release()
	out, err := dataplane.SelectRows(b, []int32{2, 0}, "p", publicdp.UpsertMode, "s", []uint32{1})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Seq != 9 || !out.Staged {
		t.Fatalf("Seq/Staged = %d/%v, want 9/true", out.Seq, out.Staged)
	}
	if out.Record.NumRows() != 2 {
		t.Fatalf("rows = %d, want 2", out.Record.NumRows())
	}
}

func TestSelectRowsEmptyIndices(t *testing.T) {
	alloc := checkedAlloc(t)
	rec := payloadRecord(t, alloc, 2, 4)
	b := &publicdp.Batch{Table: "t", Record: rec}
	defer b.Release()
	out, err := dataplane.SelectRows(b, nil, "", publicdp.AppendMode, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if out != nil {
		t.Fatal("SelectRows with no indices returned a batch")
	}
}

// EmptyBatch preserves the schema and carries the cycle metadata.
func TestEmptyBatch(t *testing.T) {
	alloc := checkedAlloc(t)
	rec := payloadRecord(t, alloc, 2, 4)
	b := &publicdp.Batch{Table: "t", Record: rec, Watermark: []byte("p9"), Seq: 4, Staged: true}
	defer b.Release()
	out := dataplane.EmptyBatch(b, publicdp.AppendMode)
	defer out.Release()
	if out.Record.NumRows() != 0 {
		t.Fatalf("rows = %d, want 0", out.Record.NumRows())
	}
	if out.Record.Schema().NumFields() != rec.Schema().NumFields() {
		t.Fatalf("schema fields = %d, want %d", out.Record.Schema().NumFields(), rec.Schema().NumFields())
	}
	if string(out.Watermark) != "p9" || out.Mode != publicdp.AppendMode || out.Seq != 4 || !out.Staged {
		t.Fatalf("metadata = %q/%v/%d/%v", out.Watermark, out.Mode, out.Seq, out.Staged)
	}
}

func TestLastRowPos(t *testing.T) {
	alloc := checkedAlloc(t)
	b := dataplanetest.GenerateBatch(2, dataplanetest.GeneratorOpts{NumRows: 7, Allocator: alloc})
	defer b.Release()
	if got, want := dataplane.LastRowPos(b), string(b.Watermark); got != want {
		t.Fatalf("LastRowPos = %q, want %q", got, want)
	}
}

func TestLastRowPosEmpty(t *testing.T) {
	alloc := checkedAlloc(t)
	rec := payloadRecord(t, alloc, 0, 4)
	b := &publicdp.Batch{Table: "t", Record: rec}
	defer b.Release()
	if got := dataplane.LastRowPos(b); got != "" {
		t.Fatalf("LastRowPos(empty) = %q, want empty", got)
	}
}

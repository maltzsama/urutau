package worker

// __op is NOT NULL by contract (dataplane.Batch, transport.WireMetadataFields).
// A null __op is read as 0 (insert) by array.Uint8.Value, so every op consumer
// that trusts the column without checking validity silently mis-handles it:
//
//   - append mode never calls Collapse; appendRowsToKeep reads Op(i) and keeps
//     the row as an insert, so the malformed row is committed;
//   - enrichment builds its keep mask with is_not_null(__op), so the row is
//     filtered out before Collapse can see (and reject) it.
//
// Validation must run at the boundaries that consume __op, not only inside
// Collapse. A contract violation is an error on every path.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/enrich"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/spec"
)

// TestAppendModeRejectsNullOp: append mode bypasses Collapse, so without a
// check at the commit boundary appendRowsToKeep classifies the null op as 0
// (insert) and the malformed row reaches the sink.
func TestAppendModeRejectsNullOp(t *testing.T) {
	fc := &fakeCommitter{}
	w := New(Config{MaxRows: 100, MaxInterval: time.Hour})
	regTable(t, w, "t", fc, dataplane.AppendMode)

	src := wireBatch(t, "t", dataplane.AppendMode, []rowchange.Change{
		chg("t", rowchange.OpInsert, 1, "a", "p1"),
	})
	bad := &dataplane.Batch{Table: "t", Record: withNullOpAt(t, src.Record, 0), Mode: dataplane.AppendMode}
	src.Release()

	ing := make(chan Ingest, 1)
	ing <- Ingest{Table: "t", Batch: bad}
	close(ing)

	err := w.Run(context.Background(), ing)
	if err == nil {
		t.Fatalf("append-mode batch with null __op accepted; committed %d batches", len(fc.batches))
	}
	if !strings.Contains(err.Error(), "null __op at row 0") {
		t.Fatalf("error = %v, want a null __op rejection", err)
	}
	if len(fc.batches) != 0 {
		t.Fatalf("committed %d batches after rejecting the row, want 0", len(fc.batches))
	}
}

// TestEnrichRejectsNullOpBeforeFiltering: the enrichment stage's keep mask is
// is_not_null(__op), so a null-op row is dropped (and counted as an inner-join
// drop) before Collapse validates it. It must be an error, not a silent loss.
func TestEnrichRejectsNullOpBeforeFiltering(t *testing.T) {
	st, decl := nullOpEnrichStage(t)
	fc := &fakeCommitter{}
	w := New(Config{MaxRows: 100, MaxInterval: time.Hour})
	regTable(t, w, "t", fc, dataplane.UpsertMode)
	w.SetKnownSchema("t", decl)
	w.SetEnricher("t", st)

	src := wireBatch(t, "t", dataplane.UpsertMode, []rowchange.Change{{
		Op: rowchange.OpInsert, Key: []any{int64(1)}, Position: "p1",
		After: map[string]any{"id": int64(1), "v": "a", "user_ref": int64(7)},
	}})
	bad := &dataplane.Batch{Table: "t", Record: withNullOpAt(t, src.Record, 0), Mode: dataplane.UpsertMode}
	src.Release()

	ing := make(chan Ingest, 1)
	ing <- Ingest{Table: "t", Batch: bad}
	close(ing)

	err := w.Run(context.Background(), ing)
	if err == nil {
		t.Fatalf("enrichment silently dropped the null-__op row (EnrichDropped=%d, committed=%d)",
			w.EnrichDropped("t"), len(fc.batches))
	}
	if !strings.Contains(err.Error(), "null __op at row 0") {
		t.Fatalf("error = %v, want a null __op rejection", err)
	}
	if len(fc.batches) != 0 {
		t.Fatalf("committed %d batches after rejecting the row, want 0", len(fc.batches))
	}
	if got := w.EnrichDropped("t"); got != 0 {
		t.Fatalf("EnrichDropped = %d, want 0: the row is a contract violation, not a join miss", got)
	}
}

// TestEnrichRejectsNullOpInWindow covers the other applyEnrich call site: a
// snapshot window's rows are enriched on Closes, so a null-op window row has
// the same silent-drop path unless validation runs before the join.
func TestEnrichRejectsNullOpInWindow(t *testing.T) {
	st, decl := nullOpEnrichStage(t)
	fc := &fakeCommitter{}
	w := New(Config{MaxRows: 100, MaxInterval: time.Hour})
	regTable(t, w, "t", fc, dataplane.UpsertMode)
	w.SetKnownSchema("t", decl)
	w.SetEnricher("t", st)

	src := wireBatch(t, "t", dataplane.AppendMode, []rowchange.Change{{
		Op: rowchange.OpInsert, Key: []any{int64(1)}, Position: "p1",
		After: map[string]any{"id": int64(1), "v": "a", "user_ref": int64(7)},
	}})
	bad := &dataplane.Batch{Table: "t", Record: withNullOpAt(t, src.Record, 0), Mode: dataplane.AppendMode}
	src.Release()
	if err := w.AddWindowRows("t", 7, bad); err != nil {
		t.Fatalf("AddWindowRows: %v", err)
	}

	ing := make(chan Ingest, 1)
	ing <- Ingest{Table: "t", Win: &rowchange.Window{WindowID: 7, Closes: true}, Position: "p9"}
	close(ing)

	err := w.Run(context.Background(), ing)
	if err == nil {
		t.Fatalf("enrichment silently dropped the null-__op window row (EnrichDropped=%d, committed=%d)",
			w.EnrichDropped("t"), len(fc.batches))
	}
	if !strings.Contains(err.Error(), "null __op at row 0") {
		t.Fatalf("error = %v, want a null __op rejection", err)
	}
	if len(fc.batches) != 0 {
		t.Fatalf("committed %d batches after rejecting the row, want 0", len(fc.batches))
	}
	if got := w.EnrichDropped("t"); got != 0 {
		t.Fatalf("EnrichDropped = %d, want 0: the row is a contract violation, not a join miss", got)
	}
}

// nullOpEnrichStage builds a hot left-join stage matching the id/v/user_ref
// fixture schema. The reference loader is shared with enrich_test.go.
func nullOpEnrichStage(t *testing.T) (*enrich.Stage, core.Schema) {
	t.Helper()
	decl := core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "v", Type: core.ColumnType{Kind: core.KindString}},
		{Name: "user_ref", Type: core.ColumnType{Kind: core.KindInt64}},
	}, PrimaryKey: []string{"id"}}
	cfg := spec.Enrich{
		Table:    "users",
		Source:   spec.EnrichSource{URI: "mysql://refdb/internal", Query: "SELECT id, name FROM users"},
		On:       map[string]string{"user_ref": "id"},
		Select:   []string{"name"},
		JoinType: "left",
	}
	st, err := enrich.New([]spec.Enrich{cfg}, decl, nil)
	if err != nil {
		t.Fatalf("enrich.New: %v", err)
	}
	if err := st.UseLoader("users", &staticLoader{}); err != nil {
		t.Fatalf("UseLoader: %v", err)
	}
	st.Start(context.Background())
	t.Cleanup(st.Stop)
	deadline := time.Now().Add(2 * time.Second)
	for !st.Ready() {
		if time.Now().After(deadline) {
			t.Fatal("reference never went hot")
		}
		time.Sleep(time.Millisecond)
	}
	return st, decl
}

// withNullOpAt returns a record sharing every column with rec except __op,
// whose value at row is null. The caller releases the returned record.
func withNullOpAt(t *testing.T, rec arrow.RecordBatch, row int) arrow.RecordBatch {
	t.Helper()
	opIdx := -1
	for i := range rec.Schema().NumFields() {
		if rec.Schema().Field(i).Name == "__op" {
			opIdx = i
			break
		}
	}
	if opIdx < 0 {
		t.Fatal("__op column not found")
	}
	src, ok := rec.Column(opIdx).(*array.Uint8)
	if !ok {
		t.Fatalf("__op column type %T, want *array.Uint8", rec.Column(opIdx))
	}
	if row < 0 || row >= int(rec.NumRows()) {
		t.Fatalf("row %d out of range for %d rows", row, rec.NumRows())
	}

	opBld := array.NewUint8Builder(memory.DefaultAllocator)
	defer opBld.Release()
	for i := range int(rec.NumRows()) {
		if i == row {
			opBld.AppendNull()
			continue
		}
		opBld.Append(src.Value(i))
	}
	opArr := opBld.NewUint8Array()
	defer opArr.Release()

	cols := make([]arrow.Array, rec.NumCols())
	for i := range int(rec.NumCols()) {
		if i == opIdx {
			opArr.Retain()
			cols[i] = opArr
		} else {
			rec.Column(i).Retain()
			cols[i] = rec.Column(i)
		}
	}
	out := array.NewRecordBatch(rec.Schema(), cols, rec.NumRows())
	for _, c := range cols {
		c.Release()
	}
	return out
}

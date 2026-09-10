package enrich

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	dpint "github.com/maltzsama/urutau/internal/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/spec"
)

// TestDeleteKeepsNullRefColumns — a delete bypasses the join entirely: it
// survives every join type and carries the reference columns as NULL.
func TestDeleteKeepsNullRefColumns(t *testing.T) {
	s, _ := newTestStage(t, refCfg(nil), usersRows())
	out, err := s.applyChanges(t, []rowchange.Change{
		{Op: rowchange.OpInsert, Key: []any{int64(1)}, After: map[string]any{"id": int64(1), "user_ref": int64(1), "q": "x"}},
		{Op: rowchange.OpDelete, Key: []any{int64(1)}},
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("rows = %d, want 2 (insert + delete)", len(out))
	}
	var del *rowchange.Change
	for i := range out {
		if out[i].Op == rowchange.OpDelete {
			del = &out[i]
		}
	}
	if del == nil {
		t.Fatal("delete row missing")
	}
	if _, ok := del.After["users.name"]; ok {
		t.Fatalf("delete must carry NULL ref columns: %v", del.After)
	}
}

// TestColdDropWholeBatch — onColdStart=drop with no snapshot returns nil
// (whole batch dropped); the miss counter is not bumped.
func TestColdDropWholeBatch(t *testing.T) {
	cfg := refCfg(func(c *spec.Enrich) { c.OnColdStart = "drop" })
	s, err := New([]spec.Enrich{cfg}, evSchema("id", "user_ref", "q"), nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	// Loader that never returns → the reference stays cold.
	_ = s.UseLoader("users", fakeErr(errNeverLoads))
	// Do NOT Start — the snapshot is nil.
	out, err := s.applyChanges(t, []rowchange.Change{
		{Op: rowchange.OpInsert, Key: []any{int64(1)}, After: map[string]any{"id": int64(1), "user_ref": int64(1), "q": "x"}},
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if out != nil {
		t.Fatalf("coldDrop must drop the whole batch, got %d rows", len(out))
	}
	if s.misses.Load() != 0 {
		t.Fatalf("coldDrop must not count misses, got %d", s.misses.Load())
	}
}

var errNeverLoads = errNever{}

type errNever struct{}

func (errNever) Error() string { return "reference never loads (test)" }

// TestColdPassMissesEveryRow — onColdStart=pass with no snapshot: every row
// passes with NULL ref columns, misses counted.
func TestColdPassMissesEveryRow(t *testing.T) {
	cfg := refCfg(func(c *spec.Enrich) { c.OnColdStart = "pass" })
	s, err := New([]spec.Enrich{cfg}, evSchema("id", "user_ref", "q"), nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	_ = s.UseLoader("users", fakeErr(errNeverLoads))
	out, err := s.applyChanges(t, []rowchange.Change{
		{Op: rowchange.OpInsert, Key: []any{int64(1)}, After: map[string]any{"id": int64(1), "user_ref": int64(1), "q": "x"}},
		{Op: rowchange.OpInsert, Key: []any{int64(2)}, After: map[string]any{"id": int64(2), "user_ref": int64(2), "q": "y"}},
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("coldPass must keep every row, got %d", len(out))
	}
	if s.misses.Load() != 2 {
		t.Fatalf("misses = %d, want 2", s.misses.Load())
	}
}

// BenchmarkColumnarJoin — absolute, no comparison against any prior
// version. 512-row batch, one left-join reference, a third of rows miss.
// Local run (Ryzen 7 Pro 7735U, arrow-go v18.7.0):
//
//	BenchmarkColumnarJoin-16   ~210 µs/op   ~47 KB/op   ~390 allocs/op
//
// The allocs are the compute kernels' internal buffers (the v18.7.0 leak,
// tracked in internal/dataplane/arrowgo_residual_test.go) plus the encode
// of the input batch (excluded from the timed section).
func BenchmarkColumnarJoin(b *testing.B) {
	s, err := New([]spec.Enrich{refCfg(nil)}, evSchema("id", "user_ref", "q"), nil)
	if err != nil {
		b.Fatal(err)
	}
	fl := &fakeLoader{}
	fl.SetRec(benchRefRec())
	_ = s.UseLoader("users", fl)
	s.Start(b.Context())
	b.Cleanup(s.Stop)
	for !s.Ready() {
	}

	cs := core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "user_ref", Type: core.ColumnType{Kind: core.KindInt64, Nullable: true}},
			{Name: "q", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
			{Name: "users.name", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
			{Name: "users.tier", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
		},
		PrimaryKey: []string{"id"},
	}
	changes := make([]rowchange.Change, 512)
	for i := range changes {
		ur := int64(1)
		if i%3 == 0 {
			ur = 99
		}
		changes[i] = rowchange.Change{
			Op: rowchange.OpInsert, Key: []any{int64(i)},
			After: map[string]any{"id": int64(i), "user_ref": ur, "q": "x"},
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		rec, encErr := transport.RecordFromChanges(changes, cs, nil)
		if encErr != nil {
			b.Fatal(encErr)
		}
		in := &dpint.Batch{Table: "events", Record: rec, Mode: dataplane.UpsertMode}
		b.StartTimer()

		out, jErr := s.ColumnarJoin(b.Context(), in)
		if jErr != nil {
			b.Fatal(jErr)
		}

		b.StopTimer()
		if out != nil {
			out.Release()
		}
		in.Release()
		b.StartTimer()
	}
}

func benchRefRec() arrow.RecordBatch {
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "tier", Type: arrow.BinaryTypes.String, Nullable: true},
	}
	rb := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema(fields, nil))
	defer rb.Release()
	rb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
	rb.Field(1).(*array.StringBuilder).AppendValues([]string{"ana", "beto"}, nil)
	rb.Field(2).(*array.StringBuilder).AppendValues([]string{"gold", "silver"}, nil)
	return rb.NewRecordBatch()
}

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

// ── semi / anti join ────────────────────────────────────────────────────

func semiCfg(jt string) spec.Enrich {
	c := refCfg(nil)
	c.JoinType = jt
	c.Select = nil // semi/anti emit no reference columns
	return c
}

// TestSemiJoinKeepsMatchesWithoutRefColumns — left semi keeps the rows that
// hit the reference, and adds NO reference columns.
func TestSemiJoinKeepsMatchesWithoutRefColumns(t *testing.T) {
	s, _ := newTestStage(t, semiCfg("left semi"), usersRows())
	out, err := s.applyChanges(t, []rowchange.Change{
		{Op: rowchange.OpInsert, Key: []any{int64(1)}, After: map[string]any{"id": int64(1), "user_ref": int64(1), "q": "x"}},  // hit
		{Op: rowchange.OpInsert, Key: []any{int64(2)}, After: map[string]any{"id": int64(2), "user_ref": int64(99), "q": "y"}}, // miss → dropped
		{Op: rowchange.OpInsert, Key: []any{int64(3)}, After: map[string]any{"id": int64(3), "user_ref": int64(2), "q": "z"}},  // hit
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("semi survivors = %d, want 2 (misses dropped)", len(out))
	}
	for _, r := range out {
		for k := range r.After {
			if len(k) > 6 && k[:6] == "users." {
				t.Fatalf("semi join must not emit reference columns, got %q", k)
			}
		}
	}
}

// TestAntiJoinKeepsMissesWithoutRefColumns — left anti keeps the rows that
// missed, and adds NO reference columns.
func TestAntiJoinKeepsMissesWithoutRefColumns(t *testing.T) {
	s, _ := newTestStage(t, semiCfg("left anti"), usersRows())
	out, err := s.applyChanges(t, []rowchange.Change{
		{Op: rowchange.OpInsert, Key: []any{int64(1)}, After: map[string]any{"id": int64(1), "user_ref": int64(1), "q": "x"}},  // hit → dropped
		{Op: rowchange.OpInsert, Key: []any{int64(2)}, After: map[string]any{"id": int64(2), "user_ref": int64(99), "q": "y"}}, // miss → kept
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(out) != 1 || out[0].Key[0] != int64(2) {
		t.Fatalf("anti survivors = %v, want [key 2]", out)
	}
	for k := range out[0].After {
		if len(k) > 6 && k[:6] == "users." {
			t.Fatalf("anti join must not emit reference columns, got %q", k)
		}
	}
}

// TestSemiAntiDeleteSurvives — a delete bypasses the join, so it survives
// BOTH semi and anti (the v9 kernel-inversion bug would drop it from one).
func TestSemiAntiDeleteSurvives(t *testing.T) {
	for _, jt := range []string{"left semi", "left anti"} {
		t.Run(jt, func(t *testing.T) {
			s, _ := newTestStage(t, semiCfg(jt), usersRows())
			out, err := s.applyChanges(t, []rowchange.Change{
				{Op: rowchange.OpDelete, Key: []any{int64(7)}},
			})
			if err != nil {
				t.Fatalf("apply: %v", err)
			}
			if len(out) != 1 || out[0].Op != rowchange.OpDelete {
				t.Fatalf("%s: delete must survive, got %v", jt, out)
			}
		})
	}
}

// TestSemiAntiColdStart — cold (no snapshot): semi drops non-deletes
// (nothing matches), anti keeps everything.
func TestSemiAntiColdStart(t *testing.T) {
	changes := []rowchange.Change{
		{Op: rowchange.OpInsert, Key: []any{int64(1)}, After: map[string]any{"id": int64(1), "user_ref": int64(1), "q": "x"}},
		{Op: rowchange.OpDelete, Key: []any{int64(2)}},
	}

	semiCfgWithPass := semiCfg("left semi")
	semiCfgWithPass.OnColdStart = "pass"
	semi, err := New([]spec.Enrich{semiCfgWithPass}, evSchema("id", "user_ref", "q"), nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = semi.UseLoader("users", fakeErr(errNeverLoads))
	out, err := semi.applyChanges(t, changes)
	if err != nil {
		t.Fatalf("semi cold: %v", err)
	}
	if len(out) != 1 || out[0].Op != rowchange.OpDelete {
		t.Fatalf("semi cold must keep only the delete, got %v", out)
	}

	antiCfgWithPass := semiCfg("left anti")
	antiCfgWithPass.OnColdStart = "pass"
	anti, _ := New([]spec.Enrich{antiCfgWithPass}, evSchema("id", "user_ref", "q"), nil)
	_ = anti.UseLoader("users", fakeErr(errNeverLoads))
	out, err = anti.applyChanges(t, changes)
	if err != nil {
		t.Fatalf("anti cold: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("anti cold must keep everything, got %d", len(out))
	}
}

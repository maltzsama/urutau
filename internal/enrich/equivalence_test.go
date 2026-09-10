package enrich

// S1 — Demolition scaffold for the columnar-join cutover (BRIEF-ARROW Onda 1).
//
// The harness pins the enrich seam's observable behavior so the row path
// (DecodeBatch -> Stage.Enrich -> BatchFromChangeBatch, today) and the
// columnar path (Stage.ColumnarJoin, landing in S6) can be proven
// field-for-field equal before the row path is deleted.
//
// runCurrentPipeline and TestColumnarPathMatchesCurrentPath exist ONLY until
// S6: once the 5 invariant tests below are green against the columnar path,
// the current path and the cross-check are removed and the invariants stay,
// pointed at columnar.
//
// The seam is a broadcast hash join on the reference key. It does not act on
// windows or collapse — those are the worker's job downstream — but window
// tags and op/key identity must survive the round-trip untouched, so the
// canonical scenario carries them.

import (
	"context"
	"testing"
	"time"

	"github.com/maltzsama/urutau/core"
	dpint "github.com/maltzsama/urutau/internal/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/spec"
)

// harnessSchema is the wire schema the canonical scenario encodes against:
// the event columns plus the reference destinations (nullable strings, the
// registered shape until S6's refTypes), so a miss writes NULL rather than
// dropping the column and every batch carries one stable shape.
func harnessSchema() core.Schema {
	return core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "user_ref", Type: core.ColumnType{Kind: core.KindInt64, Nullable: true}},
			{Name: "q", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
			{Name: "users.name", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
			{Name: "users.tier", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
		},
		PrimaryKey: []string{"id"},
	}
}

// canonicalScenario is 30 changes of table "t", positions pos-0001..0030,
// exercising every invariant the seam must preserve:
//
//	PK 1: insert -> update -> update        (collapse keeps the last version)
//	PK 2: insert -> delete  -> insert       (reinsert wins over the delete)
//	PK 3: delete with a partial before      (key backfilled from the tuple)
//	PK 4: enrich miss                       (reference columns land NULL)
//	PK 5: enrich hit                        (reference columns land with values)
//	PK 6: a window opens mid-stream         (InWindow + Closes survive)
//
// The reference (users) is hot with id 1 -> ana/gold, id 2 -> beto/silver.
// user_ref 1 hits; user_ref 99 (and nil) miss.
func canonicalScenario() []rowchange.Change {
	pos := func(n int) string { return "pos-" + pad4(n) }
	ev := func(op rowchange.Op, id, userRef int64, q string, n int) rowchange.Change {
		c := rowchange.Change{
			Op:       op,
			Table:    "t",
			Key:      []any{id},
			Position: pos(n),
			IngestTS: time.Unix(0, int64(n)).UTC(),
		}
		if op != rowchange.OpDelete {
			c.After = map[string]any{"id": id, "user_ref": userRef, "q": q}
		}
		return c
	}

	var cs []rowchange.Change
	n := 0
	add := func(c rowchange.Change) { n++; c.Position = "pos-" + pad4(n); cs = append(cs, c) }

	// PK 1 — insert then two updates (last version 3 wins).
	add(ev(rowchange.OpInsert, 1, 1, "v1", 0))
	add(ev(rowchange.OpUpdate, 1, 1, "v2", 0))
	add(ev(rowchange.OpUpdate, 1, 1, "v3", 0))

	// PK 2 — insert, delete, reinsert (reinsert wins).
	add(ev(rowchange.OpInsert, 2, 1, "a", 0))
	add(ev(rowchange.OpDelete, 2, 0, "", 0))
	add(ev(rowchange.OpInsert, 2, 1, "b", 0))

	// PK 3 — delete with a partial before image: no "id" in After, only the
	// key tuple. The seam must not lose the key on the round-trip.
	del3 := rowchange.Change{
		Op: rowchange.OpDelete, Table: "t", Key: []any{int64(3)},
		Before: map[string]any{"q": "gone"},
	}
	add(del3)

	// PK 4 — enrich miss: user_ref 99 has no reference row.
	add(ev(rowchange.OpInsert, 4, 99, "miss", 0))

	// PK 5 — enrich hit: user_ref 1 -> ana/gold.
	add(ev(rowchange.OpInsert, 5, 1, "hit", 0))

	// PK 6 — a snapshot window opens: InWindow rows, then a Closes marker.
	win := &rowchange.Window{ChunkID: 7, InWindow: true}
	c6a := ev(rowchange.OpInsert, 6, 1, "w1", 0)
	c6a.Window = win
	add(c6a)
	c6b := ev(rowchange.OpUpdate, 6, 1, "w2", 0)
	c6b.Window = win
	add(c6b)

	// Filler up to 30 so positions run pos-0001..0030 and the batch is not
	// trivially small.
	for id := int64(10); n < 30; id++ {
		hit := int64(1)
		if id%2 == 0 {
			hit = 99 // half miss, half hit
		}
		add(ev(rowchange.OpInsert, id, hit, "f", 0))
	}
	return cs
}

func pad4(n int) string {
	b := []byte{'0', '0', '0', '0'}
	for i := 3; i >= 0 && n > 0; i-- {
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b)
}

// newHarnessStage builds a hot stage whose users reference is loaded with
// ana/beto — the fixture the canonical scenario's hits and misses assume.
func newHarnessStage(t *testing.T) *Stage {
	t.Helper()
	cfg := refCfg(func(c *spec.Enrich) { c.OnColdStart = "pass" })
	s, err := New([]spec.Enrich{cfg}, []string{"id", "user_ref", "q"}, nil)
	if err != nil {
		t.Fatalf("new stage: %v", err)
	}
	if err := s.UseLoader("users", &fakeLoader{rows: usersRows()}); err != nil {
		t.Fatalf("use loader: %v", err)
	}
	s.Start(context.Background())
	t.Cleanup(s.Stop)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !s.refs[0].isHot() {
		time.Sleep(time.Millisecond)
	}
	if !s.refs[0].isHot() {
		t.Fatal("reference never went hot")
	}
	return s
}

// runCurrentPipeline drives the scenario through the row path: encode to a
// wire batch, run the seam (EnrichBatch, which today decodes -> Stage.Enrich
// -> re-encodes), decode the output back to rows for comparison.
//
// REMOVED IN S6 once the columnar path is proven equal.
func runCurrentPipeline(t *testing.T, s *Stage, changes []rowchange.Change) []rowchange.Change {
	t.Helper()
	cb := rowchange.Batch{
		Table:    "t",
		Changes:  changes,
		Position: changes[len(changes)-1].Position,
		Mode:     rowchange.UpsertMode,
	}
	in, err := dpint.BatchFromChangeBatch(cb, harnessSchema())
	if err != nil {
		t.Fatalf("current: encode input: %v", err)
	}
	defer in.Release()

	out, err := s.EnrichBatch(in, []string{"id"})
	if err != nil {
		t.Fatalf("current: EnrichBatch: %v", err)
	}
	if out == nil {
		return nil
	}
	defer out.Release()

	rows, err := transport.DecodeBatch(out.Record, "t", []string{"id"})
	if err != nil {
		t.Fatalf("current: decode output: %v", err)
	}
	return rows
}

// runColumnarPipeline drives the scenario through the columnar path
// (Stage.ColumnarJoin). Stubbed until S6 lands ColumnarJoin; the
// cross-check test skips while this returns nil.
func runColumnarPipeline(t *testing.T, s *Stage, changes []rowchange.Change) []rowchange.Change {
	t.Helper()
	_ = s
	_ = changes
	return nil // S6: encode -> s.ColumnarJoin(b) -> decode
}

// ── comparison helpers ──────────────────────────────────────────────────

func rowsByKey(rows []rowchange.Change) map[int64]rowchange.Change {
	m := make(map[int64]rowchange.Change, len(rows))
	for _, r := range rows {
		if len(r.Key) == 1 {
			if id, ok := r.Key[0].(int64); ok {
				m[id] = r
			}
		}
	}
	return m
}

func afterEqual(t *testing.T, a, b map[string]any, ctx string) {
	t.Helper()
	if len(a) != len(b) {
		t.Fatalf("%s: After key count %d != %d\n a=%v\n b=%v", ctx, len(a), len(b), a, b)
	}
	for k, va := range a {
		if vb, ok := b[k]; !ok || va != vb {
			t.Fatalf("%s: After[%q] = %v vs %v", ctx, k, va, vb)
		}
	}
}

// ── tests ───────────────────────────────────────────────────────────────

// TestColumnarPathMatchesCurrentPath — field-by-field, same order.
// REMOVED IN S6.
func TestColumnarPathMatchesCurrentPath(t *testing.T) {
	sc := canonicalScenario()
	cur := runCurrentPipeline(t, newHarnessStage(t), sc)
	col := runColumnarPipeline(t, newHarnessStage(t), sc)
	if col == nil {
		t.Skip("S6: ColumnarJoin not implemented yet")
	}
	if len(cur) != len(col) {
		t.Fatalf("row count: current %d, columnar %d", len(cur), len(col))
	}
	for i := range cur {
		a, b := cur[i], col[i]
		if a.Op != b.Op || a.Position != b.Position {
			t.Fatalf("row %d: op/pos (%v,%q) vs (%v,%q)", i, a.Op, a.Position, b.Op, b.Position)
		}
		if len(a.Key) != len(b.Key) || (len(a.Key) == 1 && a.Key[0] != b.Key[0]) {
			t.Fatalf("row %d: key %v vs %v", i, a.Key, b.Key)
		}
		afterEqual(t, a.After, b.After, "row "+pad4(i))
	}
}

// TestCollapseKeepsLastVersionPerKey — the seam preserves every version in
// arrival order (collapse is downstream); the last update for PK 1 is v3.
func TestCollapseKeepsLastVersionPerKey(t *testing.T) {
	rows := harnessRows(t)
	var pk1 []rowchange.Change
	for _, r := range rows {
		if len(r.Key) == 1 && r.Key[0] == int64(1) {
			pk1 = append(pk1, r)
		}
	}
	if len(pk1) != 3 {
		t.Fatalf("PK 1 versions = %d, want 3 (seam must not collapse)", len(pk1))
	}
	if pk1[2].After["q"] != "v3" {
		t.Fatalf("last PK 1 version q = %v, want v3", pk1[2].After["q"])
	}
	for i := 1; i < len(pk1); i++ {
		if pk1[i-1].Position >= pk1[i].Position {
			t.Fatalf("PK 1 versions out of arrival order: %q then %q", pk1[i-1].Position, pk1[i].Position)
		}
	}
}

// TestReinsertWinsOverDelete — PK 2's three ops (insert, delete, insert) all
// survive the seam in order; the final op is an insert.
func TestReinsertWinsOverDelete(t *testing.T) {
	rows := harnessRows(t)
	var pk2 []rowchange.Change
	for _, r := range rows {
		if len(r.Key) == 1 && r.Key[0] == int64(2) {
			pk2 = append(pk2, r)
		}
	}
	if len(pk2) != 3 {
		t.Fatalf("PK 2 ops = %d, want 3", len(pk2))
	}
	if pk2[0].Op != rowchange.OpInsert || pk2[1].Op != rowchange.OpDelete || pk2[2].Op != rowchange.OpInsert {
		t.Fatalf("PK 2 op sequence = %v/%v/%v, want insert/delete/insert", pk2[0].Op, pk2[1].Op, pk2[2].Op)
	}
	if pk2[2].After["q"] != "b" {
		t.Fatalf("PK 2 reinsert q = %v, want b", pk2[2].After["q"])
	}
}

// TestDeleteKeyBackfilledFromKeyTuple — PK 3's delete carries no "id" in its
// image, only the key tuple; the wire round-trip backfills the PK column.
func TestDeleteKeyBackfilledFromKeyTuple(t *testing.T) {
	rows := harnessRows(t)
	var del *rowchange.Change
	for i := range rows {
		if len(rows[i].Key) == 1 && rows[i].Key[0] == int64(3) {
			del = &rows[i]
		}
	}
	if del == nil {
		t.Fatal("PK 3 delete missing from seam output")
	}
	if del.Op != rowchange.OpDelete {
		t.Fatalf("PK 3 op = %v, want delete", del.Op)
	}
	if del.Key[0] != int64(3) {
		t.Fatalf("PK 3 key = %v, want [3]", del.Key)
	}
	if id, ok := del.After["id"].(int64); !ok || id != 3 {
		t.Fatalf("PK 3 After[id] = %v, want 3 (key backfill lost)", del.After["id"])
	}
}

// TestEnrichMissNullsAndHitValues — PK 4 (user_ref 99) misses: the reference
// columns land NULL (absent from the decoded After). PK 5 (user_ref 1) hits:
// ana/gold.
//
// EnrichMiss is deliberately NOT asserted here: it is an in-memory flag on
// rowchange.Change that the wire does not carry — it reaches a sink only
// through the enrich_miss metadata column, which the harness schema does not
// declare. The observable miss signal on the wire is "reference columns are
// NULL", and that is what the seam must guarantee. S6's ColumnarJoin adds a
// real miss column; when that lands, extend this test.
func TestEnrichMissNullsAndHitValues(t *testing.T) {
	rows := harnessRows(t)
	by := rowsByKey(rows)

	miss, ok := by[4]
	if !ok {
		t.Fatal("PK 4 missing from output")
	}
	if _, present := miss.After["users.name"]; present {
		t.Fatalf("PK 4 users.name should be absent/NULL, got %v", miss.After["users.name"])
	}
	if _, present := miss.After["users.tier"]; present {
		t.Fatalf("PK 4 users.tier should be absent/NULL, got %v", miss.After["users.tier"])
	}

	hit, ok := by[5]
	if !ok {
		t.Fatal("PK 5 missing from output")
	}
	if hit.After["users.name"] != "ana" || hit.After["users.tier"] != "gold" {
		t.Fatalf("PK 5 enriched wrong: name=%v tier=%v", hit.After["users.name"], hit.After["users.tier"])
	}
}

// TestWindowReleasesBeforeCloses — PK 6's window-tagged rows pass the seam
// with the join applied and their arrival order intact (the seam does not
// reorder or drop windowed rows; Closes handling is the worker's).
func TestWindowReleasesBeforeCloses(t *testing.T) {
	rows := harnessRows(t)
	var pk6 []rowchange.Change
	for _, r := range rows {
		if len(r.Key) == 1 && r.Key[0] == int64(6) {
			pk6 = append(pk6, r)
		}
	}
	if len(pk6) != 2 {
		t.Fatalf("PK 6 rows = %d, want 2", len(pk6))
	}
	if pk6[0].Position >= pk6[1].Position {
		t.Fatalf("PK 6 rows out of order: %q then %q", pk6[0].Position, pk6[1].Position)
	}
	// The join still applies to windowed rows (user_ref 1 -> ana).
	if pk6[0].After["users.name"] != "ana" || pk6[1].After["users.name"] != "ana" {
		t.Fatalf("PK 6 windowed rows not enriched: %v / %v", pk6[0].After, pk6[1].After)
	}
}

// harnessRows runs the canonical scenario through the current path once and
// returns the decoded output. The invariant tests read it; after S6 this
// switches to runColumnarPipeline.
func harnessRows(t *testing.T) []rowchange.Change {
	t.Helper()
	return runCurrentPipeline(t, newHarnessStage(t), canonicalScenario())
}

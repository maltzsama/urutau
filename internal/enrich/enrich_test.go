package enrich

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/go-sql-driver/mysql"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	dpint "github.com/maltzsama/urutau/internal/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/spec"
)

// refCfg is a left-join reference against users, joined on user_ref → id.
func refCfg(mutate func(*spec.Enrich)) spec.Enrich {
	cfg := spec.Enrich{
		Table: "users",
		Source: spec.EnrichSource{
			URI:   "mysql://refdb/internal",
			Query: "SELECT id, name, tier FROM users",
		},
		On:       map[string]string{"user_ref": "id"},
		Select:   []string{"name", "tier"},
		JoinType: "left",
	}
	if mutate != nil {
		mutate(&cfg)
	}
	return cfg
}

func usersRows() []map[string]any {
	return []map[string]any{
		{"id": int64(1), "name": "ana", "tier": "gold"},
		{"id": int64(2), "name": "beto", "tier": "silver"},
	}
}

func searchEvent(id int64, userRef any) rowchange.Change {
	return rowchange.Change{
		Op: rowchange.OpInsert, Key: []any{id},
		After:    map[string]any{"id": id, "user_ref": userRef, "q": "flight"},
		IngestTS: time.Now(),
	}
}

// newTestStage builds a hot stage with a fake loader already loaded. The
// rows fixtures stay column-value maps for readability; rowsToRec turns
// them into the Arrow record the loader contract now returns. The event
// join column's type is derived from the fixture's reference join column
// so the P4 boot check (types must match) passes for string-keyed refs.
func newTestStage(t *testing.T, cfg spec.Enrich, rows []map[string]any) (*Stage, *fakeLoader) {
	t.Helper()
	kinds := map[string]core.Kind{}
	for ev, ref := range cfg.On {
		if len(rows) > 0 {
			switch rows[0][ref].(type) {
			case string:
				kinds[ev] = core.KindString
			case float64, float32:
				kinds[ev] = core.KindFloat64
			case bool:
				kinds[ev] = core.KindBool
			}
		}
	}
	s, err := New([]spec.Enrich{cfg}, evSchemaTyped(kinds, "id", "user_ref", "order_ref", "q"), nil)
	if err != nil {
		t.Fatalf("new stage: %v", err)
	}
	fl := &fakeLoader{}
	fl.SetRec(rowsToRec(t, rows))
	if err := s.UseLoader(cfg.Table, fl); err != nil {
		t.Fatalf("use loader: %v", err)
	}
	s.Start(context.Background())
	// Wait for the async first load to flip hot.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.refs[0].isHot() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !s.refs[0].isHot() {
		t.Fatal("reference never went hot")
	}
	t.Cleanup(s.Stop)
	return s, fl
}

func (rj *refJoin) isHot() bool {
	return rj.snap.Load() != nil
}

// applyChanges drives changes through the columnar seam: encode to a wire
// batch against a schema carrying the event columns plus the reference
// destinations (nullable strings, the registered pre-load shape), run
// ColumnarJoin, decode back to rows for assertion. A nil result (whole
// batch dropped) returns nil.
func (s *Stage) applyChanges(t *testing.T, changes []rowchange.Change) ([]rowchange.Change, error) {
	t.Helper()
	// Base: every key any change carries, inferred nullable, plus the
	// standard event columns so the PK ("id") and join column always exist.
	inferred := transport.InferSchemaFromChanges(changes)
	for i := range inferred.Columns {
		inferred.Columns[i].Type.Nullable = true
	}
	inferred.PrimaryKey = []string{"id"}
	seen := map[string]bool{}
	for _, c := range inferred.Columns {
		seen[c.Name] = true
	}
	ensure := func(name string, kind core.Kind) {
		if !seen[name] {
			inferred.Columns = append(inferred.Columns, core.Column{Name: name, Type: core.ColumnType{Kind: kind, Nullable: true}})
			seen[name] = true
		}
	}
	ensure("id", core.KindInt64)
	ensure("user_ref", core.KindInt64)
	ensure("order_ref", core.KindInt64)
	for _, rj := range s.refs {
		for _, name := range rj.refDests {
			ensure(name, core.KindString)
		}
	}
	rec, err := transport.RecordFromChanges(changes, inferred, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	in := &dpint.Batch{Table: "events", Record: rec, Mode: dataplane.UpsertMode}
	defer in.Release()
	out, err := s.ColumnarJoin(in)
	if err != nil {
		return nil, err
	}
	if out == nil {
		return nil, nil
	}
	defer out.Release()
	rows, derr := transport.DecodeBatch(out.Record, "events", []string{"id"})
	if derr != nil {
		t.Fatalf("decode: %v", derr)
	}
	return rows, nil
}

func (s *Stage) applyOne(t *testing.T, c rowchange.Change) ([]rowchange.Change, error) {
	t.Helper()
	return s.applyChanges(t, []rowchange.Change{c})
}

// 1 — Broadcast join: N events × M rows in O(N); the loader runs once per
// refresh, never per event.
func TestBroadcastJoinLoaderCalledOnce(t *testing.T) {
	s, fl := newTestStage(t, refCfg(nil), usersRows())
	for i := range 50 {
		out, err := s.applyOne(t, searchEvent(int64(i), int64(1)))
		if err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
		if len(out) != 1 {
			t.Fatalf("event %d vanished", i)
		}
		if out[0].After["users.name"] != "ana" || out[0].After["users.tier"] != "gold" {
			t.Fatalf("event %d enriched wrong: %v", i, out[0].After)
		}
	}
	if fl.loads != 1 {
		t.Fatalf("loader called %d times for 50 events, want 1", fl.loads)
	}
}

// 2 — Left miss: the event passes, reference columns are NULL, enrich_miss
// is set.
func TestLeftMissPassesWithNullsAndFlag(t *testing.T) {
	s, _ := newTestStage(t, refCfg(nil), usersRows())
	out, err := s.applyOne(t, searchEvent(9, int64(42)))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("left miss dropped the event")
	}
	if _, ok := out[0].After["users.name"]; ok {
		t.Fatalf("miss columns not NULL: %v", out[0].After)
	}
	if _, ok := out[0].After["users.tier"]; ok {
		t.Fatalf("miss columns not NULL: %v", out[0].After)
	}
	if s.misses.Load() != 1 {
		t.Fatalf("misses = %d, want 1", s.misses.Load())
	}
}

// 3 — Inner miss: the event is dropped; survivors are the matches.
func TestInnerMissDrops(t *testing.T) {
	s, _ := newTestStage(t, refCfg(func(c *spec.Enrich) { c.JoinType = "inner" }), usersRows())
	out, err := s.applyChanges(t, []rowchange.Change{
		searchEvent(1, int64(1)),  // hit
		searchEvent(2, int64(99)), // miss → dropped
		searchEvent(3, int64(2)),  // hit
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("survivors = %d, want 2 (misses dropped)", len(out))
	}
	if s.innerDropped.Load() != 1 {
		t.Fatalf("innerDropped = %d, want 1", s.innerDropped.Load())
	}
}

// 7 — Refresh is an atomic swap: readers in flight never see a partial
// map (run with -race; the swap replaces the whole pointer under mu).
func TestRefreshAtomicSwapUnderConcurrency(t *testing.T) {
	s, fl := newTestStage(t, refCfg(func(c *spec.Enrich) { c.Refresh = "10ms" }), usersRows())
	var wg sync.WaitGroup
	stop := make(chan struct{})
	// Reader: hammers Apply while refreshes happen.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				out, err := s.applyOne(t, searchEvent(1, int64(2)))
				if err != nil {
					t.Errorf("apply: %v", err)
					return
				}
				if out[0].After["users.name"] != "beto" {
					t.Errorf("torn read: %v", out[0].After)
					return
				}
			}
		}
	}()
	// Refresher: replaces the image repeatedly (bigger each time).
	for i := range 5 {
		fl.SetRows(t, append(usersRows(), map[string]any{"id": int64(100 + i), "name": fmt.Sprintf("x%d", i), "tier": "bronze"}))
		time.Sleep(15 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
}

// 8 — Determinism is contained: the same reference image yields identical
// output on replay; the stage itself adds no non-determinism.
func TestSameImageDeterministicOutput(t *testing.T) {
	s, _ := newTestStage(t, refCfg(nil), usersRows())
	c1 := searchEvent(1, int64(1))
	c2 := searchEvent(1, int64(1))
	o1, err := s.applyOne(t, c1)
	if err != nil {
		t.Fatalf("apply 1: %v", err)
	}
	o2, err := s.applyOne(t, c2)
	if err != nil {
		t.Fatalf("apply 2: %v", err)
	}
	if o1[0].After["users.name"] != o2[0].After["users.name"] || o1[0].After["users.tier"] != o2[0].After["users.tier"] {
		t.Fatalf("same image, different output: %v vs %v", o1[0].After, o2[0].After)
	}
}

// Boot validation: the event side of the join must exist in the schema.
func TestNewRejectsUnknownEventColumn(t *testing.T) {
	cfg := refCfg(func(c *spec.Enrich) {
		c.On = map[string]string{"nope": "id"}
	})
	if _, err := New([]spec.Enrich{cfg}, evSchema("id", "user_ref", "q"), nil); err == nil {
		t.Fatal("unknown event column accepted")
	}
}

// First-load validation: the reference side and uniqueness are checked
// when the image is built, and the failure is sticky.
func TestFirstLoadRejectsBadReference(t *testing.T) {
	cfg := refCfg(nil)
	s, err := New([]spec.Enrich{cfg}, evSchema("id", "user_ref", "q"), nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	// The query result has no "id" column (the on reference side).
	_ = s.UseLoader(cfg.Table, fakeRows(t, []map[string]any{{"pk": int64(1), "name": "ana"}}))
	s.Start(context.Background())
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.refs[0].stickyErr() != nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := s.applyOne(t, searchEvent(1, int64(1))); err == nil {
		t.Fatal("broken reference surfaced no error on the first event")
	}
	s.Stop()
}

// A refresh that fails keeps the previous image hot — a reference outage
// must not blind an already-warm pipeline.
func TestFailedRefreshKeepsPreviousImage(t *testing.T) {
	s, fl := newTestStage(t, refCfg(nil), usersRows())
	fl.SetErr(errors.New("reference db down"))
	// Direct refresh call: fails, keeps the old image.
	s.refs[0].refresh(context.Background(), s.log)
	if !s.refs[0].isHot() {
		t.Fatal("failed refresh cooled the reference")
	}
	out, err := s.applyOne(t, searchEvent(1, int64(1)))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if out[0].After["users.name"] != "ana" {
		t.Fatalf("previous image lost: %v", out[0].After)
	}
}

// normalizeKey: int widths collapse (uint64 == int64 for non-negatives);
// []byte becomes string; string and int stay distinct; float widths stay
// distinct. The join-key type contract, now on the snapshot index.
func TestNormalizeKey(t *testing.T) {
	if normalizeKey(int64(5)) != normalizeKey(int32(5)) {
		t.Fatal("int widths should normalize to one key")
	}
	if normalizeKey(int64(5)) != normalizeKey(uint64(5)) {
		t.Fatal("int64 and uint64 non-negative should normalize (MySQL UNSIGNED case)")
	}
	if normalizeKey("5") == normalizeKey(int64(5)) {
		t.Fatal("string and int must NOT bridge — cast in SQL instead")
	}
	if normalizeKey([]byte("x")) != normalizeKey("x") {
		t.Fatal("[]byte and string should normalize to one key")
	}
	if normalizeKey(int64(-1)) == normalizeKey(uint64(18446744073709551615)) {
		t.Fatal("negative int must not collide with a large uint64")
	}
}

// Enrich on a key-only delete is a no-op: tombstones carry nothing.
func TestDeleteChangePassesThrough(t *testing.T) {
	s, _ := newTestStage(t, refCfg(nil), usersRows())
	del := rowchange.Change{Op: rowchange.OpDelete, Key: []any{int64(1)}}
	out, err := s.applyOne(t, del)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("delete must pass untouched: %+v", out)
	}
}

// ── CR-044: explicit projection and renaming ────────────────────────────

// refRows are richer than the join needs: email and created_at exist in
// the query result but must never reach the event unless selected.
func refRows() []map[string]any {
	return []map[string]any{
		{"id": int64(1), "name": "ana", "tier": "gold", "email": "ana@x", "created_at": "2020-01-01"},
		{"id": int64(2), "name": "beto", "tier": "silver", "email": "beto@x", "created_at": "2020-01-02"},
	}
}

// refRowsStrKey is refRows with a string join key: reference destinations
// travel as nullable strings on the wire (until CR-069), so PROJECTING the
// join key requires it to be a string — an int64 key fails the load loud.
func refRowsStrKey() []map[string]any {
	return []map[string]any{
		{"id": "1", "name": "ana", "tier": "gold", "email": "ana@x", "created_at": "2020-01-01"},
		{"id": "2", "name": "beto", "tier": "silver", "email": "beto@x", "created_at": "2020-01-02"},
	}
}

// 5.1 — Projection: only the selected columns reach the event.
func TestProjectionOnlySelectedColumns(t *testing.T) {
	s, _ := newTestStage(t, refCfg(nil), refRows())
	out, err := s.applyOne(t, searchEvent(1, int64(1)))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if out[0].After["users.name"] != "ana" || out[0].After["users.tier"] != "gold" {
		t.Fatalf("selected columns missing: %v", out[0].After)
	}
	for _, absent := range []string{"email", "created_at"} {
		if _, ok := out[0].After[absent]; ok {
			t.Fatalf("unselected column %q leaked into the event", absent)
		}
	}
}

// 5.2 — Renaming resolves at LOAD time: the event receives the final name.
func TestRenameAtLoad(t *testing.T) {
	cfg := refCfg(func(c *spec.Enrich) {
		c.As = map[string]string{"users.name": "user_name"} // tier keeps its name
	})
	s, _ := newTestStage(t, cfg, refRows())
	out, err := s.applyOne(t, searchEvent(1, int64(1)))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if out[0].After["user_name"] != "ana" {
		t.Fatalf("rename not applied: %v", out[0].After)
	}
	if _, ok := out[0].After["name"]; ok {
		t.Fatalf("original name survived the rename: %v", out[0].After)
	}
	if out[0].After["users.tier"] != "gold" {
		t.Fatalf("unrenamed column lost: %v", out[0].After)
	}
}

// 5.3 — Collision: with table-prefixed names, the reference's join column
// coexists with the source under "table.column". Renaming via "as" gives
// a custom destination name.
func TestCollisionOverwriteAndCoexistence(t *testing.T) {
	// Coexistence: select [id, name] — reference id is projected as
	// "users.id" while source id=99 remains untouched.
	cfg := refCfg(func(c *spec.Enrich) {
		c.Select = []string{"id", "name"}
	})
	s, _ := newTestStage(t, cfg, refRowsStrKey())
	out, err := s.applyOne(t, searchEvent(99, "1")) // source id=99
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if out[0].After["id"] != int64(99) {
		t.Fatalf("source id was overwritten: %v", out[0].After)
	}
	if out[0].After["users.id"] != "1" {
		t.Fatalf("reference id not projected with prefix: %v", out[0].After)
	}

	// Rename: explicit "as" gives a custom destination name.
	cfgAs := refCfg(func(c *spec.Enrich) {
		c.Select = []string{"id", "name"}
		c.As = map[string]string{"users.id": "ref_id"}
	})
	sAs, _ := newTestStage(t, cfgAs, refRowsStrKey())
	outAs, err := sAs.applyOne(t, searchEvent(99, "1"))
	if err != nil {
		t.Fatalf("apply as: %v", err)
	}
	if outAs[0].After["id"] != int64(99) || outAs[0].After["ref_id"] != "1" {
		t.Fatalf("coexistence broken: %v", outAs[0].After)
	}
}

// 5.4 — Star projection: everything lands with table prefixes; the
// source's own columns remain unprefixed.
func TestStarProjectionPreservesJoinKey(t *testing.T) {
	cfg := refCfg(func(c *spec.Enrich) {
		c.Select = []string{"*"}
	})
	s, _ := newTestStage(t, cfg, refRowsStrKey())
	out, err := s.applyOne(t, searchEvent(1, "2"))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	// Reference columns are prefixed with table name.
	for _, want := range []string{"users.name", "users.tier", "users.email", "users.created_at"} {
		if _, ok := out[0].After[want]; !ok {
			t.Fatalf("star projection missed %q: %v", want, out[0].After)
		}
	}
	// Source columns remain unprefixed.
	if out[0].After["id"] != int64(1) {
		t.Fatalf("source id overwritten: %v", out[0].After)
	}
	// Reference join column is projected with prefix.
	if out[0].After["users.id"] != "2" {
		t.Fatalf("reference join column not projected: %v", out[0].After)
	}
	if out[0].After["users.name"] != "beto" {
		t.Fatalf("star values wrong: %v", out[0].After)
	}
}

// 5.5 — Star with as: the renamed join column injects the reference
// value under the custom name while the source's own column survives.
func TestStarWithRenameInjectsJoinColumnAsNewName(t *testing.T) {
	cfg := refCfg(func(c *spec.Enrich) {
		c.Select = []string{"*"}
		c.As = map[string]string{"users.id": "ref_id"}
	})
	s, _ := newTestStage(t, cfg, refRowsStrKey())
	out, err := s.applyOne(t, searchEvent(1, "2"))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	// Source id preserved.
	if out[0].After["id"] != int64(1) {
		t.Fatalf("source id overwritten: %v", out[0].After)
	}
	// Reference id injected under the renamed key (no prefix because of "as").
	if out[0].After["ref_id"] != "2" {
		t.Fatalf("renamed join column missing or wrong: %v", out[0].After)
	}
	// Other reference columns are prefixed.
	if out[0].After["users.name"] != "beto" {
		t.Fatalf("other reference columns not prefixed: %v", out[0].After)
	}
}

// The refTable carries ONLY the projected columns (plus the join key at
// column 0), under their final prefixed/renamed names.
func TestRefTableHoldsProjectedColumnsOnly(t *testing.T) {
	cfg := refCfg(func(c *spec.Enrich) {
		c.As = map[string]string{"users.name": "user_name"}
	})
	s, _ := newTestStage(t, cfg, refRows())
	snap := s.refs[0].snap.Load()
	if snap == nil {
		t.Fatal("snapshot not loaded")
	}
	sch := snap.refTable.Schema()
	// column 0 is the join key; 1..N are the dests.
	if sch.NumFields() != len(cfg.Select)+1 {
		t.Fatalf("refTable has %d columns, want %d (join key + %d dests)",
			sch.NumFields(), len(cfg.Select)+1, len(cfg.Select))
	}
	names := map[string]bool{}
	for i := 1; i < sch.NumFields(); i++ {
		names[sch.Field(i).Name] = true
	}
	if !names["user_name"] {
		t.Fatalf("refTable missing renamed column: %v", names)
	}
	if !names["users.tier"] {
		t.Fatalf("refTable missing prefixed column: %v", names)
	}
	if names["name"] || names["tier"] {
		t.Fatalf("refTable holds a pre-rename / unprefixed name: %v", names)
	}
}

// Select listing a column the query does not return rejects the load —
// sticky before hot.
func TestSelectColumnMissingFromQueryRejected(t *testing.T) {
	cfg := refCfg(func(c *spec.Enrich) {
		c.Select = []string{"name", "nope"}
	})
	s, err := New([]spec.Enrich{cfg}, evSchema("id", "user_ref", "q"), nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	_ = s.UseLoader(cfg.Table, fakeRows(t, refRows()))
	s.Start(context.Background())
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.refs[0].stickyErr() != nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := s.applyOne(t, searchEvent(1, int64(1))); err == nil {
		t.Fatal("missing select column surfaced no error")
	}
	s.Stop()
}

// 6 — Multi-reference collision: when two references inject columns with
// the same name, the table prefix prevents silent overwrites.
func TestMultiReferenceCollisionWithPrefix(t *testing.T) {
	// Two references: both have a "name" column.
	cfg1 := spec.Enrich{
		Table: "users",
		Source: spec.EnrichSource{
			URI:   "mysql://refdb/internal",
			Query: "SELECT id, name FROM users",
		},
		On:       map[string]string{"user_ref": "id"},
		Select:   []string{"name"},
		JoinType: "left",
	}
	cfg2 := spec.Enrich{
		Table: "products",
		Source: spec.EnrichSource{
			URI:   "mysql://refdb/internal",
			Query: "SELECT id, name FROM products",
		},
		On:       map[string]string{"product_ref": "id"},
		Select:   []string{"name"},
		JoinType: "left",
	}

	s, err := New([]spec.Enrich{cfg1, cfg2}, evSchema("id", "user_ref", "product_ref", "q"), nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	// Load users reference.
	fl1 := fakeRows(t, []map[string]any{
		{"id": int64(1), "name": "ana"},
	})
	if err := s.UseLoader(cfg1.Table, fl1); err != nil {
		t.Fatalf("use loader 1: %v", err)
	}

	// Load products reference.
	fl2 := fakeRows(t, []map[string]any{
		{"id": int64(10), "name": "laptop"},
	})
	if err := s.UseLoader(cfg2.Table, fl2); err != nil {
		t.Fatalf("use loader 2: %v", err)
	}

	s.Start(context.Background())
	t.Cleanup(s.Stop)

	// Wait for both references to go hot.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.refs[0].isHot() && s.refs[1].isHot() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !s.refs[0].isHot() || !s.refs[1].isHot() {
		t.Fatal("references never went hot")
	}

	// Event with both join keys set.
	event := rowchange.Change{
		Op: rowchange.OpInsert, Key: []any{int64(1)},
		After:    map[string]any{"id": int64(1), "user_ref": int64(1), "product_ref": int64(10), "q": "test"},
		IngestTS: time.Now(),
	}

	out, err := s.applyChanges(t, []rowchange.Change{event})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("event vanished")
	}

	// Both "name" columns coexist with table prefixes.
	if out[0].After["users.name"] != "ana" {
		t.Fatalf("users.name not injected: %v", out[0].After)
	}
	if out[0].After["products.name"] != "laptop" {
		t.Fatalf("products.name not injected: %v", out[0].After)
	}
	// Source columns remain unprefixed.
	if out[0].After["id"] != int64(1) {
		t.Fatalf("source id overwritten: %v", out[0].After)
	}
}

// pollUntil asserts cond() becomes true within the deadline (RV-09):
// fixed sleeps wait instead of establishing state, so a slow CI runner
// observes a mid-transition stage and flakes.
func pollUntil(t *testing.T, deadline time.Duration, cond func() bool, msg string) {
	t.Helper()
	for start := time.Now(); time.Since(start) < deadline; time.Sleep(time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatal(msg)
}

// --- audit fixes: enrich.go ------------------------------------------------

func TestFirstErrClearedOnSuccess(t *testing.T) {
	// Build stage manually so we can fail the FIRST load (before hot).
	// Use a fast refresh so the second load happens promptly.
	fl := fakeErr(errors.New("db down"))
	cfg := refCfg(nil)
	cfg.Refresh = "10ms"
	s, err := New([]spec.Enrich{cfg}, evSchema("id", "user_ref", "q"), nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := s.UseLoader("users", fl); err != nil {
		t.Fatalf("use loader: %v", err)
	}
	s.Start(context.Background())
	defer s.Stop()
	// First load fails -> snap is nil -> firstErr is set.
	pollUntil(t, 2*time.Second, func() bool {
		return s.refs[0].stickyErr() != nil
	}, "expected sticky error before first success")

	// Fix the loader; the next refresh tick clears firstErr (audit #1).
	fl.SetErr(nil)
	fl.SetRows(t, usersRows())
	pollUntil(t, 2*time.Second, func() bool {
		return s.refs[0].isHot() && s.refs[0].stickyErr() == nil
	}, "firstErr not cleared after success")
}

func TestEmptyRefGoesHot(t *testing.T) {
	s, _ := newTestStage(t, refCfg(nil), []map[string]any{}) // legitimately empty
	if !s.refs[0].isHot() {
		t.Fatal("empty reference should go hot (audit #3)")
	}
	out, err := s.applyOne(t, rowchange.Change{
		After: map[string]any{"user_ref": int64(1), "v": "x"},
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if _, ok := out[0].After["users.name"]; ok {
		t.Fatalf("all joins must miss against empty image: %v", out[0].After)
	}
}

func TestDeleteBypassAllPolicies(t *testing.T) {
	// Production deletes NEVER arrive with After == nil: DecodeBatch
	// always allocates the map and the encode side backfills the join key.
	// The bypass guard is therefore Op == OpDelete (After == nil is kept
	// only as a defensive check for in-process changes). All three shapes
	// below must survive every policy.
	//
	// Shape 1: wire-format delete (After allocated) vs coldDrop — the drop
	// policy must not eat the tombstone before the first successful load.
	s, _ := newTestStage(t, refCfg(func(c *spec.Enrich) { c.OnColdStart = "drop" }), usersRows())
	out, err := s.applyOne(t, rowchange.Change{
		Op:     rowchange.OpDelete,
		After:  map[string]any{"user_ref": int64(1)}, // wire: allocated, key backfilled
		Before: map[string]any{"id": int64(1)},
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(out) != 1 {
		t.Fatal("wire-format delete must bypass cold drop (audit #4)")
	}

	// Shape 2: wire-format delete + inner join + hot miss — an inner miss
	// drops events; deletes must bypass.
	innerCfg := refCfg(func(c *spec.Enrich) { c.JoinType = "inner" })
	si, _ := newTestStage(t, innerCfg, usersRows())
	out, err = si.applyOne(t, rowchange.Change{
		Op:     rowchange.OpDelete,
		After:  map[string]any{"user_ref": int64(99)}, // no match in ref
		Before: map[string]any{"id": int64(99)},
	})
	if err != nil {
		t.Fatalf("apply inner: %v", err)
	}
	if len(out) != 1 {
		t.Fatal("delete must bypass inner-join miss")
	}

	// Shape 3: defensive in-process tombstone (After nil) — still survives.
	out, err = s.applyOne(t, rowchange.Change{
		Op:     rowchange.OpDelete,
		After:  nil,
		Before: map[string]any{"id": int64(1)},
	})
	if err != nil {
		t.Fatalf("apply tombstone: %v", err)
	}
	if len(out) != 1 {
		t.Fatal("nil-After tombstone must bypass cold drop")
	}
}

func TestNullJoinKeyMiss(t *testing.T) {
	s, _ := newTestStage(t, refCfg(nil), usersRows())
	out, err := s.applyOne(t, rowchange.Change{
		After: map[string]any{"user_ref": nil, "v": "x"},
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if _, ok := out[0].After["users.name"]; ok {
		t.Fatalf("NULL join key must miss, not match \"nil\" (audit #7): %v", out[0].After)
	}
}

func TestStickyErrAtStart(t *testing.T) {
	// Build stage manually so the first load fails (before hot).
	fl := fakeErr(errors.New("broken"))
	s, err := New([]spec.Enrich{refCfg(nil)}, evSchema("id", "user_ref", "q"), nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := s.UseLoader("users", fl); err != nil {
		t.Fatalf("use loader: %v", err)
	}
	s.Start(context.Background())
	defer s.Stop()
	// The failing first load installs the sticky error; poll for it.
	pollUntil(t, 2*time.Second, func() bool {
		_, err := s.applyChanges(t, []rowchange.Change{{Op: rowchange.OpInsert, Key: []any{int64(1)}, After: map[string]any{"id": int64(1), "user_ref": int64(1)}}})
		return err != nil
	}, "sticky error must block Enrich at top (audit #8)")
}

func TestJoinTypeValidation(t *testing.T) {
	_, err := New([]spec.Enrich{refCfg(func(c *spec.Enrich) { c.JoinType = "cross" })}, evSchema("user_ref", "v"), nil)
	if err == nil {
		t.Fatal("unknown join_type must be rejected at boot (audit #10)")
	}
}

func TestMaxWaitValidation(t *testing.T) {
	_, err := New([]spec.Enrich{refCfg(func(c *spec.Enrich) { c.BufferLimits.MaxWait = "not-a-duration" })}, evSchema("user_ref", "v"), nil)
	if err == nil {
		t.Fatal("invalid maxWait must be rejected at boot (audit #10)")
	}
	// R-3: a negative duration parses but silently disables the cap at drain
	// time — reject it too.
	if _, err := New([]spec.Enrich{refCfg(func(c *spec.Enrich) { c.BufferLimits.MaxWait = "-1s" })}, evSchema("user_ref", "v"), nil); err == nil {
		t.Fatal("negative maxWait must be rejected at boot")
	}
	// "0s" and absent stay valid (both mean "no cap").
	if _, err := New([]spec.Enrich{refCfg(func(c *spec.Enrich) { c.BufferLimits.MaxWait = "0s" })}, evSchema("user_ref", "v"), nil); err != nil {
		t.Fatalf("0s maxWait must boot: %v", err)
	}
	if _, err := New([]spec.Enrich{refCfg(nil)}, evSchema("user_ref", "v"), nil); err != nil {
		t.Fatalf("absent maxWait must boot: %v", err)
	}
}

func TestMaxEventsValidation(t *testing.T) {
	_, err := New([]spec.Enrich{refCfg(func(c *spec.Enrich) { c.BufferLimits.MaxEvents = -1 })}, evSchema("user_ref", "v"), nil)
	if err == nil {
		t.Fatal("negative maxEvents must be rejected at boot (audit #10)")
	}
}

func TestDestCollisionRejected(t *testing.T) {
	// Two columns project to the same destination name via overlapping "as".
	_, err := New([]spec.Enrich{refCfg(func(c *spec.Enrich) {
		c.Select = []string{"*"}
		c.As = map[string]string{"users.name": "collided", "users.tier": "collided"}
	})}, evSchema("user_ref", "v"), nil)
	if err == nil {
		t.Fatal("two renames to the same destination must be rejected (audit #11)")
	}
}

func TestCrossRefDefaultCollisionRejected(t *testing.T) {
	// RV-07: ref A selects "name" (default destination "users.name"); ref B
	// renames onto "users.name" — the rename silently overwrites A's
	// projection. Both directions must be rejected at boot.
	refA := refCfg(func(c *spec.Enrich) { c.Table = "users" })
	refB := refCfg(func(c *spec.Enrich) {
		c.Table = "orders"
		c.Select = []string{"id"}
		c.As = map[string]string{"orders.id": "users.name"}
	})
	if _, err := New([]spec.Enrich{refA, refB}, evSchema("user_ref", "v"), nil); err == nil {
		t.Fatal("rename onto another ref's default destination must be rejected")
	}

	// Reverse order: a default projecting onto a name already claimed by
	// another ref's rename must also collide. refC (users, select name)
	// defaults to destination "users.name" — claimed above by refB's rename.
	refC := refCfg(func(c *spec.Enrich) {
		c.Table = "users"
		c.Select = []string{"name"}
	})
	if _, err := New([]spec.Enrich{refB, refC}, evSchema("user_ref", "v"), nil); err == nil {
		t.Fatal("default projecting over another ref's rename must be rejected")
	}
}

func TestMySQLConfig(t *testing.T) {
	tests := []struct {
		name  string
		in    string
		check func(t *testing.T, cfg *mysql.Config)
		err   bool
	}{
		{
			name: "explicit port",
			in:   "mysql://u:p@myhost:3307/mydb",
			check: func(t *testing.T, cfg *mysql.Config) {
				if cfg.User != "u" || cfg.Passwd != "p" || cfg.Addr != "myhost:3307" || cfg.DBName != "mydb" {
					t.Fatalf("cfg = %q/%q@%s/%s", cfg.User, cfg.Passwd, cfg.Addr, cfg.DBName)
				}
			},
		},
		{
			name: "default port",
			in:   "mysql://u:p@myhost/mydb",
			check: func(t *testing.T, cfg *mysql.Config) {
				if cfg.Addr != "myhost:3306" {
					t.Fatalf("addr = %q, want myhost:3306", cfg.Addr)
				}
			},
		},
		{
			name: "parseTime on",
			in:   "mysql://u:p@myhost/mydb",
			check: func(t *testing.T, cfg *mysql.Config) {
				if !cfg.ParseTime {
					t.Fatal("ParseTime must stay on (DATETIME -> time.Time)")
				}
			},
		},
		{
			name: "password with DSN delimiters survives decoded",
			in:   "mysql://u:p%2Fass%3Fx%40y@myhost/mydb",
			check: func(t *testing.T, cfg *mysql.Config) {
				if cfg.Passwd != "p/ass?x@y" {
					t.Fatalf("passwd = %q, want %q", cfg.Passwd, "p/ass?x@y")
				}
			},
		},
		{
			name: "query params carried through",
			in:   "mysql://u:p@myhost/mydb?timeout=10s&charset=utf8mb4",
			check: func(t *testing.T, cfg *mysql.Config) {
				if cfg.Params["timeout"] != "10s" || cfg.Params["charset"] != "utf8mb4" {
					t.Fatalf("params = %v, want timeout+charset", cfg.Params)
				}
			},
		},
		{
			name: "missing db is an error",
			in:   "mysql://u:p@myhost/",
			err:  true,
		},
		{
			name: "wrong scheme is an error",
			in:   "postgres://u:p@myhost/mydb",
			err:  true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := mysqlConfig(tc.in)
			if (err != nil) != tc.err {
				t.Fatalf("err=%v, wantErr=%v", err, tc.err)
			}
			if err == nil && tc.check != nil {
				tc.check(t, cfg)
			}
		})
	}
}

// CR-069: a non-string reference value is now representable natively — the
// destination column lands typed (Int64 here), no CAST workaround, no
// sticky load error.
func TestNonStringReferenceLandsTyped(t *testing.T) {
	cfg := refCfg(func(c *spec.Enrich) { c.Select = []string{"name", "tier"} })
	s, err := New([]spec.Enrich{cfg}, evSchema("id", "user_ref", "q"), nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	loader := fakeRows(t, []map[string]any{
		{"id": int64(1), "name": "ana", "tier": int64(3)},
	})
	if err := s.UseLoader("users", loader); err != nil {
		t.Fatal(err)
	}
	s.Start(context.Background())
	defer s.Stop()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !s.refs[0].isHot() {
		time.Sleep(time.Millisecond)
	}
	if !s.refs[0].isHot() {
		t.Fatalf("reference did not go hot: %v", s.refs[0].stickyErr())
	}
	if got := s.refs[0].snap.Load().refType("users.tier"); got.ID() != arrow.INT64 {
		t.Fatalf("users.tier refType = %s, want int64", got)
	}

	// Encode the batch with the reference columns pre-declared with their
	// resolved types (what AddRefColumns does once a snapshot exists) so the
	// decoded value comes back as int64, not a stringified column.
	rec, err := transport.RecordFromChanges(
		[]rowchange.Change{searchEvent(1, int64(1))},
		core.Schema{
			Columns: []core.Column{
				{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
				{Name: "user_ref", Type: core.ColumnType{Kind: core.KindInt64, Nullable: true}},
				{Name: "q", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
				{Name: "users.name", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
				{Name: "users.tier", Type: core.ColumnType{Kind: core.KindInt64, Nullable: true}},
			},
			PrimaryKey: []string{"id"},
		}, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	in := &dpint.Batch{Table: "events", Record: rec, Mode: dataplane.UpsertMode}
	defer in.Release()
	out, err := s.ColumnarJoin(in)
	if err != nil {
		t.Fatalf("ColumnarJoin: %v", err)
	}
	defer out.Release()
	rows, err := transport.DecodeBatch(out.Record, "events", []string{"id"})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if rows[0].After["users.tier"] != int64(3) {
		t.Fatalf("users.tier = %#v, want int64(3)", rows[0].After["users.tier"])
	}
	if rows[0].After["users.name"] != "ana" {
		t.Fatalf("users.name = %v, want ana", rows[0].After["users.name"])
	}
}

// CR-069: a wildcard select projects the join key too; an int64 key lands
// as an Int64 column, no load failure.
func TestProjectedNonStringJoinKeyLandsTyped(t *testing.T) {
	cfg := refCfg(func(c *spec.Enrich) { c.Select = []string{"*"} })
	s, err := New([]spec.Enrich{cfg}, evSchema("id", "user_ref", "q"), nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	loader := fakeRows(t, []map[string]any{{"id": int64(1), "name": "ana", "tier": "gold"}})
	if err := s.UseLoader("users", loader); err != nil {
		t.Fatal(err)
	}
	s.Start(context.Background())
	defer s.Stop()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !s.refs[0].isHot() {
		time.Sleep(time.Millisecond)
	}
	if !s.refs[0].isHot() {
		t.Fatalf("reference did not go hot: %v", s.refs[0].stickyErr())
	}
	if got := s.refs[0].snap.Load().refType("users.id"); got.ID() != arrow.INT64 {
		t.Fatalf("users.id refType = %s, want int64", got)
	}
}

// FT-1: RefColumnsFor is the single computation the three schema owners
// (runner, worker, coordinator) apply — explicit selects only, final names
// with renames, empty for wildcard.
func TestRefColumnsFor(t *testing.T) {
	cfgs := []spec.Enrich{
		{Table: "users", Select: []string{"name", "tier"}},
		{Table: "orders", Select: []string{"total"}, As: map[string]string{"orders.total": "order_total"}},
		{Table: "geo", Select: []string{"*"}}, // wildcard: unknown at boot
	}
	got := RefColumnsFor(cfgs)
	want := []string{"users.name", "users.tier", "order_total"}
	if len(got) != len(want) {
		t.Fatalf("RefColumnsFor = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("RefColumnsFor[%d] = %q, want %q", i, got[i], want[i])
		}
	}
	if len(RefColumnsFor([]spec.Enrich{{Table: "geo", Select: []string{"*"}}})) != 0 {
		t.Fatal("wildcard must contribute no boot-time columns")
	}
}

// FT-1: AddRefColumns appends the destinations as nullable strings, dedups
// against existing columns, and leaves the source columns untouched.
func TestAddRefColumns(t *testing.T) {
	cs := core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "users.name", Type: core.ColumnType{Kind: core.KindString, Nullable: true}}, // already present
	}}
	got := AddRefColumns(cs, []spec.Enrich{{Table: "users", Select: []string{"name", "tier"}}})
	if len(got.Columns) != 3 {
		t.Fatalf("columns = %v, want id + users.name (dedup) + users.tier", got.Columns)
	}
	col, ok := got.Column("users.tier")
	if !ok || col.Type.Kind != core.KindString || !col.Type.Nullable {
		t.Fatalf("users.tier = %+v, want nullable string", col.Type)
	}
	if n := len(got.Columns); n != 3 {
		t.Fatalf("dedup failed: %d columns", n)
	}
}

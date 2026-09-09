package enrich

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/maltzsama/urutau/internal/rowchange"
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

// newTestStage builds a hot stage with a fake loader already loaded.
func newTestStage(t *testing.T, cfg spec.Enrich, rows []map[string]any) (*Stage, *fakeLoader) {
	t.Helper()
	s, err := New([]spec.Enrich{cfg}, []string{"id", "user_ref", "q"}, nil)
	if err != nil {
		t.Fatalf("new stage: %v", err)
	}
	fl := &fakeLoader{rows: rows}
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

func (s *Stage) applyOne(t *testing.T, c rowchange.Change) ([]rowchange.Change, error) {
	t.Helper()
	return s.Enrich([]rowchange.Change{c})
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
	if out[0].After["users.name"] != nil || out[0].After["users.tier"] != nil {
		t.Fatalf("miss columns not NULL: %v", out[0].After)
	}
	if !out[0].EnrichMiss {
		t.Fatal("EnrichMiss not set on a left miss")
	}
	if s.misses.Load() != 1 {
		t.Fatalf("misses = %d, want 1", s.misses.Load())
	}
}

// 3 — Inner miss: the event is dropped; survivors are the matches.
func TestInnerMissDrops(t *testing.T) {
	s, _ := newTestStage(t, refCfg(func(c *spec.Enrich) { c.JoinType = "inner" }), usersRows())
	out, err := s.Enrich([]rowchange.Change{
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

// 4 — Boot buffer: events arriving cold are queued, then drained in order
// once the reference is hot.
func TestColdStartBufferDrainsInOrder(t *testing.T) {
	cfg := refCfg(func(c *spec.Enrich) {
		c.OnColdStart = "buffer"
		c.BufferLimits = spec.EnrichBufferLimits{MaxEvents: 100, MaxWait: "30s"}
	})
	s, err := New([]spec.Enrich{cfg}, []string{"id", "user_ref", "q"}, nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	fl := &fakeLoader{rows: usersRows()}
	_ = s.UseLoader(cfg.Table, fl)
	// NOT started: the reference stays cold.
	_, _ = s.Enrich(nil) // warm call: no-op

	var survived []rowchange.Change
	for i := 1; i <= 3; i++ {
		out, err := s.Enrich([]rowchange.Change{searchEvent(int64(i), int64(1))})
		if err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
		survived = append(survived, out...)
	}
	if len(survived) != 0 {
		t.Fatalf("cold events should be buffered, %d leaked through", len(survived))
	}
	// Start: the first load flips hot; the NEXT Apply drains the queue.
	s.Start(context.Background())
	t.Cleanup(s.Stop)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.refs[0].isHot() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	drained, err := s.Enrich(nil)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(drained) != 3 {
		t.Fatalf("drained = %d, want 3", len(drained))
	}
	for i, c := range drained {
		if c.After["id"] != int64(i+1) {
			t.Fatalf("drain order broken at %d: %v", i, c.After)
		}
		if c.After["users.name"] != "ana" {
			t.Fatalf("drained event %d not enriched: %v", i, c.After)
		}
	}
}

// 5 — MaxEvents expiry: the OLDEST event is evacuated and follows the
// join type (left → NULLs + flag).
func TestBufferMaxEventsEvictsOldest(t *testing.T) {
	cfg := refCfg(func(c *spec.Enrich) {
		c.BufferLimits = spec.EnrichBufferLimits{MaxEvents: 2}
	})
	s, err := New([]spec.Enrich{cfg}, []string{"id", "user_ref", "q"}, nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	_ = s.UseLoader(cfg.Table, &fakeLoader{rows: usersRows()})

	// Three events cold with a queue of 2: the first is evacuated
	// (follows left-join miss) and surfaces in the output.
	out, err := s.Enrich([]rowchange.Change{
		searchEvent(1, int64(1)),
		searchEvent(2, int64(1)),
		searchEvent(3, int64(1)),
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("evacuated = %d, want 1", len(out))
	}
	if out[0].After["id"] != int64(1) {
		t.Fatalf("evicted the wrong event: %v", out[0].After)
	}
	if !out[0].EnrichMiss {
		t.Fatal("evacuated event did not follow the left-join miss policy")
	}
	if s.evicted.Load() != 1 {
		t.Fatalf("evicted counter = %d, want 1", s.evicted.Load())
	}
	s.Stop()
}

// 6 — MaxWait expiry: an event queued past the cap follows the join type
// at drain time.
func TestBufferMaxWaitExpires(t *testing.T) {
	cfg := refCfg(func(c *spec.Enrich) {
		c.OnColdStart = "buffer"
		c.BufferLimits = spec.EnrichBufferLimits{MaxWait: "1ms"}
	})
	s, err := New([]spec.Enrich{cfg}, []string{"id", "user_ref", "q"}, nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	_ = s.UseLoader(cfg.Table, &fakeLoader{rows: usersRows()})
	if _, err := s.Enrich([]rowchange.Change{searchEvent(1, int64(1))}); err != nil {
		t.Fatalf("park: %v", err)
	}
	time.Sleep(5 * time.Millisecond) // the parked event is now past MaxWait
	// Pretend the reference went hot with an empty drain list... no: the
	// real path flips hot in refresh; simulate by flipping manually.
	img, dests, star, _ := buildImage(s.refs[0], usersRows())
	s.refs[0].snap.Store(&snapshot{image: img, dests: dests, star: star})
	s.refs[0].mu.Lock()
	s.refs[0].pendingDrain = s.refs[0].queue
	s.refs[0].queue = nil
	s.refs[0].mu.Unlock()

	out, err := s.Enrich(nil)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(out) != 1 || !out[0].EnrichMiss {
		t.Fatalf("expired event did not follow the miss policy: %+v", out)
	}
	if s.evicted.Load() != 1 {
		t.Fatalf("evicted = %d, want 1", s.evicted.Load())
	}
	s.Stop()
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
		fl.SetRows(append(fl.rows, map[string]any{"id": int64(100 + i), "name": fmt.Sprintf("x%d", i), "tier": "bronze"}))
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
	if _, err := New([]spec.Enrich{cfg}, []string{"id", "user_ref", "q"}, nil); err == nil {
		t.Fatal("unknown event column accepted")
	}
}

// First-load validation: the reference side and uniqueness are checked
// when the image is built, and the failure is sticky.
func TestFirstLoadRejectsBadReference(t *testing.T) {
	cfg := refCfg(nil)
	s, err := New([]spec.Enrich{cfg}, []string{"id", "user_ref", "q"}, nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	// The query result has no "id" column (the on reference side).
	_ = s.UseLoader(cfg.Table, &fakeLoader{rows: []map[string]any{{"pk": int64(1), "name": "ana"}}})
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

// Join key typing: int64 and string do not bridge; int widths do.
func TestJoinKeyTyping(t *testing.T) {
	if joinKey(int64(5)) != joinKey(int32(5)) {
		t.Fatal("int widths should normalize to one key")
	}
	if joinKey("5") == joinKey(int64(5)) {
		t.Fatal("string and int must NOT bridge — cast in SQL instead")
	}
	if joinKey([]byte("x")) != joinKey("x") {
		t.Fatal("[]byte and string should normalize to one key")
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
	if len(out) != 1 || out[0].EnrichMiss {
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
	s, _ := newTestStage(t, cfg, refRows())
	out, err := s.applyOne(t, searchEvent(99, int64(1))) // source id=99
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if out[0].After["id"] != int64(99) {
		t.Fatalf("source id was overwritten: %v", out[0].After)
	}
	if out[0].After["users.id"] != int64(1) {
		t.Fatalf("reference id not projected with prefix: %v", out[0].After)
	}

	// Rename: explicit "as" gives a custom destination name.
	cfgAs := refCfg(func(c *spec.Enrich) {
		c.Select = []string{"id", "name"}
		c.As = map[string]string{"users.id": "ref_id"}
	})
	sAs, _ := newTestStage(t, cfgAs, refRows())
	outAs, err := sAs.applyOne(t, searchEvent(99, int64(1)))
	if err != nil {
		t.Fatalf("apply as: %v", err)
	}
	if outAs[0].After["id"] != int64(99) || outAs[0].After["ref_id"] != int64(1) {
		t.Fatalf("coexistence broken: %v", outAs[0].After)
	}
}

// 5.4 — Star projection: everything lands with table prefixes; the
// source's own columns remain unprefixed.
func TestStarProjectionPreservesJoinKey(t *testing.T) {
	cfg := refCfg(func(c *spec.Enrich) {
		c.Select = []string{"*"}
	})
	s, _ := newTestStage(t, cfg, refRows())
	out, err := s.applyOne(t, searchEvent(1, int64(2)))
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
	if out[0].After["users.id"] != int64(2) {
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
	s, _ := newTestStage(t, cfg, refRows())
	out, err := s.applyOne(t, searchEvent(1, int64(2)))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	// Source id preserved.
	if out[0].After["id"] != int64(1) {
		t.Fatalf("source id overwritten: %v", out[0].After)
	}
	// Reference id injected under the renamed key (no prefix because of "as").
	if out[0].After["ref_id"] != int64(2) {
		t.Fatalf("renamed join column missing or wrong: %v", out[0].After)
	}
	// Other reference columns are prefixed.
	if out[0].After["users.name"] != "beto" {
		t.Fatalf("other reference columns not prefixed: %v", out[0].After)
	}
}

// The image itself carries ONLY the projected columns, under their final
// prefixed names — the memory contract of the map.
func TestImageHoldsProjectedColumnsOnly(t *testing.T) {
	cfg := refCfg(func(c *spec.Enrich) {
		c.As = map[string]string{"users.name": "user_name"}
	})
	s, _ := newTestStage(t, cfg, refRows())
	rj := s.refs[0]
	snap := rj.snap.Load()
	if snap == nil {
		t.Fatal("snapshot not loaded")
	}
	img := snap.image
	for k, row := range img {
		if len(row) != len(cfg.Select) {
			t.Fatalf("key %s: image row has %d columns, want %d: %v", k, len(row), len(cfg.Select), row)
		}
		// Renamed column uses the custom name.
		if _, ok := row["user_name"]; !ok {
			t.Fatalf("image missing renamed column: %v", row)
		}
		// Unrenamed column uses prefixed name.
		if _, ok := row["users.tier"]; !ok {
			t.Fatalf("image missing prefixed column: %v", row)
		}
		// Original unprefixed name should not exist.
		if _, ok := row["name"]; ok {
			t.Fatalf("image holds the pre-rename name: %v", row)
		}
		if _, ok := row["tier"]; ok {
			t.Fatalf("image holds unprefixed name: %v", row)
		}
	}
}

// Select listing a column the query does not return rejects the load —
// sticky before hot.
func TestSelectColumnMissingFromQueryRejected(t *testing.T) {
	cfg := refCfg(func(c *spec.Enrich) {
		c.Select = []string{"name", "nope"}
	})
	s, err := New([]spec.Enrich{cfg}, []string{"id", "user_ref", "q"}, nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	_ = s.UseLoader(cfg.Table, &fakeLoader{rows: refRows()})
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

	s, err := New([]spec.Enrich{cfg1, cfg2}, []string{"id", "user_ref", "product_ref", "q"}, nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	// Load users reference.
	fl1 := &fakeLoader{rows: []map[string]any{
		{"id": int64(1), "name": "ana"},
	}}
	if err := s.UseLoader(cfg1.Table, fl1); err != nil {
		t.Fatalf("use loader 1: %v", err)
	}

	// Load products reference.
	fl2 := &fakeLoader{rows: []map[string]any{
		{"id": int64(10), "name": "laptop"},
	}}
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

	out, err := s.Enrich([]rowchange.Change{event})
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
	fl := &fakeLoader{rows: nil, err: errors.New("db down")}
	cfg := refCfg(nil)
	cfg.Refresh = "10ms"
	s, err := New([]spec.Enrich{cfg}, []string{"id", "user_ref", "q"}, nil)
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
		_, err := s.Enrich(nil)
		return err != nil
	}, "expected sticky error before first success")

	// Fix the loader; the next refresh tick clears firstErr (audit #1).
	fl.SetErr(nil)
	fl.SetRows(usersRows())
	pollUntil(t, 2*time.Second, func() bool {
		_, err := s.Enrich(nil)
		return err == nil
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
	if !out[0].EnrichMiss {
		t.Fatal("all joins must miss against empty image")
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
	if !out[0].EnrichMiss {
		t.Fatal("NULL join key must miss, not match \"nil\" (audit #7)")
	}
}

func TestStickyErrAtStart(t *testing.T) {
	// Build stage manually so the first load fails (before hot).
	fl := &fakeLoader{rows: nil, err: errors.New("broken")}
	s, err := New([]spec.Enrich{refCfg(nil)}, []string{"id", "user_ref", "q"}, nil)
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
		_, err := s.Enrich([]rowchange.Change{{After: map[string]any{"user_ref": int64(1)}}})
		return err != nil
	}, "sticky error must block Enrich at top (audit #8)")
}

func TestJoinTypeValidation(t *testing.T) {
	_, err := New([]spec.Enrich{refCfg(func(c *spec.Enrich) { c.JoinType = "cross" })}, []string{"user_ref", "v"}, nil)
	if err == nil {
		t.Fatal("unknown join_type must be rejected at boot (audit #10)")
	}
}

func TestMaxWaitValidation(t *testing.T) {
	_, err := New([]spec.Enrich{refCfg(func(c *spec.Enrich) { c.BufferLimits.MaxWait = "not-a-duration" })}, []string{"user_ref", "v"}, nil)
	if err == nil {
		t.Fatal("invalid maxWait must be rejected at boot (audit #10)")
	}
}

func TestMaxEventsValidation(t *testing.T) {
	_, err := New([]spec.Enrich{refCfg(func(c *spec.Enrich) { c.BufferLimits.MaxEvents = -1 })}, []string{"user_ref", "v"}, nil)
	if err == nil {
		t.Fatal("negative maxEvents must be rejected at boot (audit #10)")
	}
}

func TestDestCollisionRejected(t *testing.T) {
	// Two columns project to the same destination name via overlapping "as".
	_, err := New([]spec.Enrich{refCfg(func(c *spec.Enrich) {
		c.Select = []string{"*"}
		c.As = map[string]string{"users.name": "collided", "users.tier": "collided"}
	})}, []string{"user_ref", "v"}, nil)
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
	if _, err := New([]spec.Enrich{refA, refB}, []string{"user_ref", "v"}, nil); err == nil {
		t.Fatal("rename onto another ref's default destination must be rejected")
	}

	// Reverse order: a default projecting onto a name already claimed by
	// another ref's rename must also collide. refC (users, select name)
	// defaults to destination "users.name" — claimed above by refB's rename.
	refC := refCfg(func(c *spec.Enrich) {
		c.Table = "users"
		c.Select = []string{"name"}
	})
	if _, err := New([]spec.Enrich{refB, refC}, []string{"user_ref", "v"}, nil); err == nil {
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

package enrich

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/maltzsama/urutau/spec"
)

// TestDuplicateKeyErrorCitesColumnAndCount — a reference with a
// non-unique join column fails the load; the error names the column and
// the count of duplicates, and NEVER a value or a position (P5: a value
// would leak a row, a position would need a mask loop).
func TestDuplicateKeyErrorCitesColumnAndCount(t *testing.T) {
	cfg := refCfg(nil) // joins on user_ref → id, selects name/tier
	s, err := New([]spec.Enrich{cfg}, evSchema("id", "user_ref", "q"), nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	// id appears twice (1, 1) and a third dup (2, 2, 2) → 2 duplicate keys.
	loader := fakeRows(t, []map[string]any{
		{"id": int64(1), "name": "ana", "tier": "gold"},
		{"id": int64(1), "name": "ana2", "tier": "silver"},
		{"id": int64(2), "name": "beto", "tier": "bronze"},
		{"id": int64(2), "name": "beto2", "tier": "bronze"},
		{"id": int64(2), "name": "beto3", "tier": "bronze"},
	})
	if uerr := s.UseLoader("users", loader); uerr != nil {
		t.Fatal(uerr)
	}
	s.Start(t.Context())
	t.Cleanup(s.Stop)

	waitSticky := func() error {
		for i := 0; i < 200; i++ {
			if e := s.refs[0].stickyErr(); e != nil {
				return e
			}
			time.Sleep(time.Millisecond)
		}
		return nil
	}
	serr := waitSticky()
	if serr == nil {
		t.Fatal("non-unique reference key did not fail the load")
	}
	msg := serr.Error()
	if !strings.Contains(msg, `"id"`) {
		t.Fatalf("error must cite the column %q: %v", "id", serr)
	}
	// 3 dups: rows 2 (id=1), 4 and 5 (id=2).
	if !strings.Contains(msg, "3 duplicate") {
		t.Fatalf("error must cite the duplicate count (3): %v", serr)
	}
	// Must NOT contain a row position or the key value.
	if regexp.MustCompile(`\brow \d+\b|\bposition\b|\bat index\b`).MatchString(msg) {
		t.Fatalf("error must not cite a position: %v", serr)
	}
	if strings.Contains(msg, "= 1 ") || strings.Contains(msg, "value 1") || strings.Contains(msg, "value 2") {
		t.Fatalf("error must not cite the key value: %v", serr)
	}
}

// TestMaxRowsRejectsOversizedReference — a reference load that returns
// more rows than MaxRows (or DefaultMaxRows when unset) is a rejected
// load, not a silent OOM risk: the broadcast join holds the whole
// reference in RAM, so the cap must fail loudly at load time.
func TestMaxRowsRejectsOversizedReference(t *testing.T) {
	cfg := refCfg(func(c *spec.Enrich) { c.MaxRows = 2 })
	s, err := New([]spec.Enrich{cfg}, evSchema("id", "user_ref", "q"), nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	loader := fakeRows(t, []map[string]any{
		{"id": int64(1), "name": "ana", "tier": "gold"},
		{"id": int64(2), "name": "beto", "tier": "silver"},
		{"id": int64(3), "name": "caio", "tier": "bronze"},
	})
	if uerr := s.UseLoader("users", loader); uerr != nil {
		t.Fatal(uerr)
	}
	s.Start(t.Context())
	t.Cleanup(s.Stop)

	var serr error
	for i := 0; i < 200; i++ {
		if serr = s.refs[0].stickyErr(); serr != nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if serr == nil {
		t.Fatal("reference exceeding maxRows did not fail the load")
	}
	if !strings.Contains(serr.Error(), "maxRows") {
		t.Fatalf("error must cite maxRows: %v", serr)
	}
	if s.refs[0].isHot() {
		t.Fatal("an oversized reference must not go hot")
	}
}

func TestBuildImageEmptyReferenceGoesHot(t *testing.T) {
	cfg := refCfg(nil)
	s, err := New([]spec.Enrich{cfg}, evSchema("id", "user_ref", "q"), nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if uerr := s.UseLoader("users", &fakeLoader{}); uerr != nil { // empty record
		t.Fatal(uerr)
	}
	s.Start(t.Context())
	t.Cleanup(s.Stop)
	for i := 0; i < 200; i++ {
		if s.refs[0].isHot() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !s.refs[0].isHot() {
		t.Fatalf("empty reference must go hot: %v", s.refs[0].stickyErr())
	}
	snap := s.refs[0].snap.Load()
	if snap.refTable != nil {
		t.Fatal("empty reference snapshot should carry no refTable")
	}
	if _, ok := snap.lookup(int64(1)); ok {
		t.Fatal("every lookup against an empty reference must miss")
	}
}

// snapValue reads a dest column's value for the row a key resolves to.
func snapValue(t *testing.T, snap *snapshot, key any, dest string) any {
	t.Helper()
	idx, ok := snap.lookup(key)
	if !ok {
		return nil
	}
	for i, d := range snap.dests {
		if d.as == dest {
			return arrowValueAt(snap.refTable.Column(i+1), int(idx))
		}
	}
	return nil
}

// TestBuildImageJoinTableShape — column 0 is the join key, 1..N the dests.
func TestBuildImageJoinTableShape(t *testing.T) {
	s, _ := newTestStage(t, refCfg(nil), usersRows())
	snap := s.refs[0].snap.Load()
	sch := snap.refTable.Schema()
	if sch.Field(0).Name != "id" {
		t.Fatalf("refTable column 0 = %q, want the join key %q", sch.Field(0).Name, "id")
	}
	if sch.Field(0).Type.ID() != arrow.INT64 {
		t.Fatalf("join key type = %s, want int64", sch.Field(0).Type)
	}
	if snapValue(t, snap, int64(1), "users.name") != "ana" {
		t.Fatalf("lookup(1).users.name = %v", snapValue(t, snap, int64(1), "users.name"))
	}
	if _, ok := snap.lookup(int64(99)); ok {
		t.Fatal("lookup(99) must miss")
	}
}

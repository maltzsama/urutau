package enrich

// E1: the enrich output must be in ARRIVAL order across multiple references
// with cold-start buffering — the downstream last-write-wins collapse
// depends on it. A cold buffer parks events and they drain reference by
// reference; the seq stamp restores arrival order.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/spec"
)

func TestEnrichPreservesArrivalOrderAcrossColdRefs(t *testing.T) {
	users := refCfg(func(c *spec.Enrich) { c.Refresh = "10ms" }) // users, on user_ref -> id
	orders := refCfg(func(c *spec.Enrich) {
		c.Table = "orders"
		c.On = map[string]string{"order_ref": "id"}
		c.Refresh = "10ms"
	})
	s, err := New([]spec.Enrich{users, orders}, []string{"id", "user_ref", "order_ref", "q"}, nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	// Both references fail their first load -> cold.
	flu := &fakeLoader{err: errors.New("down")}
	flo := &fakeLoader{err: errors.New("down")}
	if err := s.UseLoader("users", flu); err != nil {
		t.Fatal(err)
	}
	if err := s.UseLoader("orders", flo); err != nil {
		t.Fatal(err)
	}
	s.Start(context.Background())
	defer s.Stop()

	ev := func(id int64) rowchange.Change {
		return rowchange.Change{Op: rowchange.OpInsert, Key: []any{id},
			After:    map[string]any{"id": id, "user_ref": int64(1), "order_ref": int64(1), "q": "x"},
			IngestTS: time.Now()}
	}
	in := []rowchange.Change{ev(1), ev(2), ev(3), ev(4)}

	// Cold: every event parks (output empty).
	if out, err := s.Enrich(in); err != nil || len(out) != 0 {
		t.Fatalf("cold enrich: out=%d err=%v, want all parked", len(out), err)
	}

	// Both references go hot.
	flu.SetErr(nil)
	flu.SetRows(usersRows())
	flo.SetErr(nil)
	flo.SetRows(usersRows())
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && (!s.refs[0].isHot() || !s.refs[1].isHot()) {
		time.Sleep(time.Millisecond)
	}
	if !s.refs[0].isHot() || !s.refs[1].isHot() {
		t.Fatal("references did not go hot")
	}

	// Drain: the parked events must come out in arrival order.
	out, err := s.Enrich(nil)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(out) != len(in) {
		t.Fatalf("drained %d, want %d", len(out), len(in))
	}
	for i := range out {
		if out[i].Key[0] != int64(i+1) {
			t.Fatalf("order lost at %d: key=%v, want %d", i, out[i].Key[0], i+1)
		}
	}
}

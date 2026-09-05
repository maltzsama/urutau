package mysql

import (
	"fmt"
	"testing"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/schema"

	"github.com/maltzsama/urutau/change"
	"github.com/maltzsama/urutau/internal/position"
)

const readerTestUUID = "3e11fa47-71ca-11e1-9e33-c80aa9429562"

func ordersTable() *schema.Table {
	return &schema.Table{
		Schema: "shop",
		Name:   "orders",
		Columns: []schema.TableColumn{
			{Name: "id"},
			{Name: "v"},
			{Name: "amount"},
		},
	}
}

var ordersRef = TableRef{Source: "shop.orders", Target: "raw.orders", PrimaryKey: []string{"id"}}

func newTestReader(out chan<- change.Change) *Reader {
	return &Reader{out: out, bySrc: map[string]TableRef{"shop.orders": ordersRef}}
}

func TestDecodeInsert(t *testing.T) {
	r := newTestReader(nil)
	tbl := ordersTable()
	row := []any{int64(7), []byte("seven"), 1.5}

	c := r.decode(ordersRef, tbl, change.OpInsert, row, nil, "u:1-3")

	if c.Op != change.OpInsert || c.Table != "raw.orders" || c.Position != "u:1-3" {
		t.Fatalf("change = %+v", c)
	}
	if len(c.Key) != 1 || c.Key[0] != int64(7) {
		t.Fatalf("key = %v, want [7]", c.Key)
	}
	if c.After["v"] != "seven" {
		t.Fatalf("v = %v (%T), want normalized string", c.After["v"], c.After["v"])
	}
	if c.After["amount"] != 1.5 {
		t.Fatalf("amount = %v", c.After["amount"])
	}
	if c.Before != nil {
		t.Fatalf("insert must not carry Before: %+v", c.Before)
	}
}

func TestDecodeDeleteKeepsBeforeOnly(t *testing.T) {
	r := newTestReader(nil)
	tbl := ordersTable()
	row := []any{int64(7), []byte("seven"), 1.5}

	c := r.decode(ordersRef, tbl, change.OpDelete, row, nil, "u:1-4")

	if c.Op != change.OpDelete {
		t.Fatalf("op = %v", c.Op)
	}
	if c.After != nil {
		t.Fatalf("delete must not carry After: %+v", c.After)
	}
	if c.Key[0] != int64(7) {
		t.Fatalf("delete key must come from the old row: %v", c.Key)
	}
}

func TestDecodeUpdateCarriesBeforeAndAfter(t *testing.T) {
	r := newTestReader(nil)
	tbl := ordersTable()
	before := []any{int64(7), []byte("old"), 1.0}
	after := []any{int64(7), []byte("new"), 2.0}

	c := r.decode(ordersRef, tbl, change.OpUpdate, after, before, "u:1-5")

	if c.After["v"] != "new" || c.Before["v"] != "old" {
		t.Fatalf("update before/after = %v / %v", c.Before, c.After)
	}
	if c.After["amount"] != 2.0 || c.Before["amount"] != 1.0 {
		t.Fatalf("update amounts = %v / %v", c.Before["amount"], c.After["amount"])
	}
}

func TestOnRowRoutesDecodeAndPosition(t *testing.T) {
	out := make(chan change.Change, 8)
	r := newTestReader(out)
	r.curGTID = "u:1-9"

	tbl := ordersTable()
	e := &canal.RowsEvent{
		Table:  tbl,
		Action: canal.InsertAction,
		Rows: [][]any{
			{int64(1), []byte("a"), 1.0},
			{int64(2), []byte("b"), 2.0},
		},
	}
	if err := r.OnRow(e); err != nil {
		t.Fatalf("OnRow: %v", err)
	}

	for i, wantID := range []int64{1, 2} {
		c := <-out
		if c.Op != change.OpInsert || c.Table != "raw.orders" {
			t.Fatalf("row %d: %+v", i, c)
		}
		if c.Key[0] != wantID || c.Position != "u:1-9" {
			t.Fatalf("row %d: key %v pos %q", i, c.Key, c.Position)
		}
	}
}

func TestOnRowUpdatePairsAndUnregisteredTable(t *testing.T) {
	out := make(chan change.Change, 8)
	r := newTestReader(out)
	r.curGTID = "u:2-2"

	e := &canal.RowsEvent{
		Table:  ordersTable(),
		Action: canal.UpdateAction,
		Rows: [][]any{
			{int64(1), []byte("old"), 1.0},
			{int64(1), []byte("new"), 2.0},
		},
	}
	if err := r.OnRow(e); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	c := <-out
	if c.Op != change.OpUpdate || c.Before["v"] != "old" || c.After["v"] != "new" {
		t.Fatalf("update change = %+v", c)
	}

	// A table outside the spec must be silently skipped.
	other := &canal.RowsEvent{
		Table:  &schema.Table{Schema: "other", Name: "t", Columns: []schema.TableColumn{{Name: "id"}}},
		Action: canal.InsertAction,
		Rows:   [][]any{{int64(1)}},
	}
	if err := r.OnRow(other); err != nil {
		t.Fatalf("OnRow other: %v", err)
	}
	select {
	case c := <-out:
		t.Fatalf("unregistered table leaked: %+v", c)
	default:
	}
}

func TestOnGTIDAccumulatesCumulativeSet(t *testing.T) {
	r := newTestReader(nil)
	r.mergeGTID(position.MustGTID(readerTestUUID + ":1-2"))
	for i := 3; i <= 5; i++ {
		r.mergeGTID(position.MustGTID(fmt.Sprintf("%s:%d", readerTestUUID, i)))
	}

	want := position.MustGTID(readerTestUUID + ":1-5").String()
	if r.curGTID != want {
		t.Fatalf("curGTID = %q, want cumulative %q", r.curGTID, want)
	}
}

// TestOnRowWindowTagsOnlyPastLowWatermark drives the DBLog window predicate:
// only transactions strictly past the low watermark (the master's executed
// GTID set captured at OpenWindow) are tagged InWindow. A transaction at or
// before the watermark is already reflected in the chunk SELECT and must not
// be tagged, or the snapshot row would be discarded for a stale value.
func TestOnRowWindowTagsOnlyPastLowWatermark(t *testing.T) {
	out := make(chan change.Change, 8)
	r := newTestReader(out)
	r.curGTID = "u:1-9"
	low := position.MustGTID(readerTestUUID + ":1-5")
	r.winMu.Lock()
	r.winOpen = true
	r.winChunk = 0
	r.winLow = low
	r.winMu.Unlock()

	tbl := ordersTable()
	e := &canal.RowsEvent{
		Table:  tbl,
		Action: canal.InsertAction,
		Rows:   [][]any{{int64(1), []byte("a"), 1.0}},
	}

	// A transaction PAST the low watermark is tagged InWindow.
	r.curTxn = position.MustGTID(readerTestUUID + ":1-7")
	if err := r.OnRow(e); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	c := <-out
	if c.Window == nil || !c.Window.InWindow || c.Window.ChunkID != 0 {
		t.Fatalf("past-low event must be InWindow: %+v", c.Window)
	}

	// A transaction AT the low watermark is NOT tagged (its effect is
	// already in the SELECT).
	r.curTxn = position.MustGTID(readerTestUUID + ":1-5")
	if err := r.OnRow(e); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	c = <-out
	if c.Window != nil {
		t.Fatalf("at-low event must NOT be InWindow: %+v", c.Window)
	}

	// A missing watermark (master capture failed) falls back to tagging
	// everything — over-tagging is safe.
	r.winMu.Lock()
	r.winLow = nil
	r.winMu.Unlock()
	r.curTxn = position.MustGTID(readerTestUUID + ":1-1")
	if err := r.OnRow(e); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	c = <-out
	if c.Window == nil || !c.Window.InWindow {
		t.Fatalf("missing watermark must fall back to tagging: %+v", c.Window)
	}

	// Window closed: nothing is tagged.
	r.ClearWindow()
	r.curTxn = position.MustGTID(readerTestUUID + ":1-8")
	if err := r.OnRow(e); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	c = <-out
	if c.Window != nil {
		t.Fatalf("closed window must not tag: %+v", c.Window)
	}
}

package mysql

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/schema"

	"github.com/maltzsama/urutau/internal/change"
	"github.com/maltzsama/urutau/internal/spec"
)

func filteredReader(out chan<- change.Change, f *spec.Filter) *Reader {
	ref := ordersRef
	ref.Filter = f
	return &Reader{out: out, bySrc: map[string]TableRef{"shop.orders": ref}}
}

func neqGone() *spec.Filter {
	return &spec.Filter{Predicate: &spec.Predicate{Column: "v", Op: spec.OpNeq, Value: "gone"}}
}

func TestFilterInsertMembership(t *testing.T) {
	out := make(chan change.Change, 4)
	r := filteredReader(out, neqGone())
	r.curGTID = "u:1-1"

	// A matching insert flows; a non-matching insert is dropped.
	if err := r.OnRow(&canal.RowsEvent{
		Table:  ordersTable(),
		Action: canal.InsertAction,
		Rows: [][]any{
			{int64(1), []byte("kept"), 1.0},
			{int64(2), []byte("gone"), 2.0},
		},
	}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}

	c := <-out
	if c.Op != change.OpInsert || c.Key[0] != int64(1) || c.After["v"] != "kept" {
		t.Fatalf("matching insert: %+v", c)
	}
	select {
	case extra := <-out:
		t.Fatalf("non-matching insert must be dropped, got %+v", extra)
	default:
	}
}

func TestFilterDeleteMembership(t *testing.T) {
	out := make(chan change.Change, 4)
	r := filteredReader(out, neqGone())
	r.curGTID = "u:1-1"

	if err := r.OnRow(&canal.RowsEvent{
		Table:  ordersTable(),
		Action: canal.DeleteAction,
		Rows: [][]any{
			{int64(1), []byte("kept"), 1.0}, // member: delete flows
			{int64(2), []byte("gone"), 2.0}, // non-member: dropped
		},
	}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}

	c := <-out
	if c.Op != change.OpDelete || c.Key[0] != int64(1) {
		t.Fatalf("member delete: %+v", c)
	}
	select {
	case extra := <-out:
		t.Fatalf("non-member delete must be dropped, got %+v", extra)
	default:
	}
}

func TestFilterUpdateTransitions(t *testing.T) {
	out := make(chan change.Change, 8)
	r := filteredReader(out, neqGone())
	r.curGTID = "u:1-1"

	// in→in stays an upsert; in→out becomes a delete; out→in becomes an
	// insert; out→out is dropped.
	if err := r.OnRow(&canal.RowsEvent{
		Table:  ordersTable(),
		Action: canal.UpdateAction,
		Rows: [][]any{
			{int64(1), []byte("a"), 1.0}, {int64(1), []byte("b"), 1.0}, // in→in
			{int64(2), []byte("a"), 2.0}, {int64(2), []byte("gone"), 2.0}, // in→out
			{int64(3), []byte("gone"), 3.0}, {int64(3), []byte("back"), 3.0}, // out→in
			{int64(4), []byte("gone"), 4.0}, {int64(4), []byte("gone"), 5.0}, // out→out
		},
	}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}

	in1 := <-out
	if in1.Op != change.OpUpdate || in1.Key[0] != int64(1) {
		t.Fatalf("in→in: %+v", in1)
	}

	left := <-out
	if left.Op != change.OpDelete || left.Key[0] != int64(2) {
		t.Fatalf("in→out must become a delete keyed by the old row: %+v", left)
	}

	entered := <-out
	if entered.Op != change.OpInsert || entered.Key[0] != int64(3) || entered.After["v"] != "back" {
		t.Fatalf("out→in must become an insert: %+v", entered)
	}

	select {
	case extra := <-out:
		t.Fatalf("out→out must be dropped, got %+v", extra)
	default:
	}
}

func TestFilterUpdateKeyFromBeforeOnExit(t *testing.T) {
	out := make(chan change.Change, 2)
	r := filteredReader(out, neqGone())
	r.curGTID = "u:1-1"

	// The exit delete must be keyed by the BEFORE values, even if the
	// filter column itself changed the value that matters.
	if err := r.OnRow(&canal.RowsEvent{
		Table:  ordersTable(),
		Action: canal.UpdateAction,
		Rows: [][]any{
			{int64(9), []byte("kept"), 1.0},
			{int64(9), []byte("gone"), 9.0},
		},
	}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}

	c := <-out
	if c.Op != change.OpDelete || c.Key[0] != int64(9) {
		t.Fatalf("exit delete: %+v", c)
	}
	if c.Position != "u:1-1" {
		t.Fatalf("converted delete must carry the event position: %q", c.Position)
	}
}

func TestNoFilterPassthrough(t *testing.T) {
	out := make(chan change.Change, 4)
	r := filteredReader(out, nil)
	r.curGTID = "u:1-1"

	if err := r.OnRow(&canal.RowsEvent{
		Table:  ordersTable(),
		Action: canal.UpdateAction,
		Rows: [][]any{
			{int64(1), []byte("gone"), 1.0},
			{int64(1), []byte("still gone"), 2.0},
		},
	}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	c := <-out
	if c.Op != change.OpUpdate || c.After["v"] != "still gone" {
		t.Fatalf("unfiltered table must pass everything through: %+v", c)
	}
}

func TestChunkWhereComposition(t *testing.T) {
	filter := "(`v` <> ?)"
	fargs := []any{"gone"}

	// Bounds only.
	where, args := chunkWhere(Chunk{Low: []any{int64(1)}, High: []any{int64(11)}}, []string{"id"}, "", nil)
	want := " WHERE (`id`) >= (?) AND (`id`) < (?)"
	if where != want || len(args) != 2 {
		t.Fatalf("bounds-only: %q %v, want %q", where, args, want)
	}

	// Bounds AND filter: bound args first, filter args last.
	where, args = chunkWhere(Chunk{Low: []any{int64(1)}, High: []any{int64(11)}}, []string{"id"}, filter, fargs)
	want = " WHERE (`id`) >= (?) AND (`id`) < (?) AND (`v` <> ?)"
	if where != want || len(args) != 3 || args[2] != "gone" {
		t.Fatalf("bounds+filter: %q %v, want %q", where, args, want)
	}

	// Last chunk (open high) with filter only.
	where, args = chunkWhere(Chunk{Low: []any{int64(11)}}, []string{"id"}, filter, fargs)
	want = " WHERE (`id`) >= (?) AND (`v` <> ?)"
	if where != want || len(args) != 2 {
		t.Fatalf("open-high+filter: %q %v, want %q", where, args, want)
	}

	// No bounds, no filter.
	where, args = chunkWhere(Chunk{}, []string{"id"}, "", nil)
	if where != "" || len(args) != 0 {
		t.Fatalf("empty: %q %v", where, args)
	}
}

var _ = schema.TableColumn{} // keep the schema import for ordersTable reuse

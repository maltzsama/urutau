package postgres

import (
	"encoding/json"
	"testing"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/position"
)

// wal2jsonKeyChange renders an UPDATE that moves shop.orders.id from 7 to 8,
// with oldkeys carrying the old key (REPLICA IDENTITY semantics).
func wal2jsonKeyChange() wal2jsonChange {
	return wal2jsonChange{
		Kind:         "update",
		Schema:       "shop",
		Table:        "orders",
		ColumnNames:  []string{"id", "v", "amount", "active"},
		ColumnTypes:  []string{"bigint", "text", "numeric", "boolean"},
		ColumnValues: []any{json.Number("8"), "new", "2.0", true},
		OldKeys: &wal2jsonOldKey{
			KeyNames:  []string{"id"},
			KeyTypes:  []string{"bigint"},
			KeyValues: []any{json.Number("7")},
		},
	}
}

// TestWal2jsonUpdateKeyChangeDeletesOldKeyUpsert pins wal2json parity with the
// pgoutput path (#545): an upsert UPDATE that changes the primary key must
// delete the old key before writing the new one, or the old row survives.
func TestWal2jsonUpdateKeyChangeDeletesOldKeyUpsert(t *testing.T) {
	out := make(chan *dataplane.Batch, 8)
	r, ref := keyChangeReader(true, out)

	if err := r.handleWal2jsonChange(wal2jsonKeyChange()); err != nil {
		t.Fatalf("handleWal2jsonChange: %v", err)
	}
	chs := flushAndDecode(t, r, out, ref, *position.MustLSN("0/40"))
	if len(chs) != 2 {
		t.Fatalf("changes = %d, want a delete of the old key then the update", len(chs))
	}
	if chs[0].Op != rowchange.OpDelete || chs[0].Key[0] != int64(7) {
		t.Fatalf("first change = %+v, want a delete of key 7", chs[0])
	}
	if chs[1].Op != rowchange.OpUpdate || chs[1].Key[0] != int64(8) {
		t.Fatalf("second change = %+v, want the update of key 8", chs[1])
	}
}

// An append target keeps the old row; the key change must not emit a delete.
func TestWal2jsonUpdateKeyChangeAppendKeepsOldKey(t *testing.T) {
	out := make(chan *dataplane.Batch, 8)
	r, ref := keyChangeReader(false, out)

	if err := r.handleWal2jsonChange(wal2jsonKeyChange()); err != nil {
		t.Fatalf("handleWal2jsonChange: %v", err)
	}
	chs := flushAndDecode(t, r, out, ref, *position.MustLSN("0/40"))
	if len(chs) != 1 {
		t.Fatalf("changes = %d, want only the update for an append target", len(chs))
	}
	if chs[0].Op != rowchange.OpUpdate || chs[0].Key[0] != int64(8) {
		t.Fatalf("change = %+v, want the update of key 8", chs[0])
	}
}

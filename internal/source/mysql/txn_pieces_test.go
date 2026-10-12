package mysql

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/schema"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/transport"
)

// lastPos is the position a batch would be committed at: its last row's.
func lastPos(t *testing.T, b *dataplane.Batch) string {
	t.Helper()
	br, err := transport.NewBatchReader(b.Record, nil)
	if err != nil {
		t.Fatal(err)
	}
	return br.Position(br.NumRows() - 1)
}

// A transaction over two tables yields a record per table. Only the last
// record of the transaction carried its position: the other table's rows went
// out with the previous transaction's, complete as they were. A commit at
// that position and a restart later, a replay took them for already
// committed and dropped them (#823).
func TestEachTablesLastRecordCarriesTheTransactionPosition(t *testing.T) {
	out := make(chan *dataplane.Batch, 8)
	r := newTestReader(out)
	itemsRef := TableRef{Source: "shop.items", Target: "raw.items", PrimaryKey: []string{"id"}}
	r.bySrc["shop.items"] = itemsRef
	r.projections["shop.items"] = projection{}
	r.cfg.Projections["shop.items"] = projection{}
	r.cfg.Schemas["raw.items"] = ordersSchema()
	items := &schema.Table{Schema: "shop", Name: "items", Columns: ordersTable().Columns}

	r.SetResume("u:1-8")
	r.curGTID = "u:1-9"
	for _, tbl := range []*schema.Table{ordersTable(), items} {
		if err := r.OnRow(&canal.RowsEvent{Table: tbl, Action: canal.InsertAction, Rows: [][]any{{int64(1), []byte("a"), 1.5}}}); err != nil {
			t.Fatalf("OnRow %s: %v", tbl.Name, err)
		}
	}
	flushTxn(t, r)

	for i := 0; i < 2; i++ {
		b := <-out
		if got := lastPos(t, b); got != "u:1-9" {
			t.Errorf("%s: position %q, want the transaction's u:1-9: the table holds all its rows of it", b.Table, got)
		}
		if b.Partial {
			t.Errorf("%s: marked partial, but no later record holds more of its rows", b.Table)
		}
		b.Record.Release()
	}
}

// A table's rows of one transaction cut into several records: the earlier
// ones carry the previous safe position (#456) and say they are partial, so
// that position is not read as "already committed"; the last one carries the
// transaction's.
func TestCutTransactionMarksItsEarlierRecordsPartial(t *testing.T) {
	out := make(chan *dataplane.Batch, 8)
	r := newTestReader(out)
	r.SetResume("u:1-8")
	r.curGTID = "u:1-9"
	rows := make([][]any, maxDirectRows+10)
	for i := range rows {
		rows[i] = []any{int64(i), []byte("a"), 1.5}
	}
	if err := r.OnRow(&canal.RowsEvent{Table: ordersTable(), Action: canal.InsertAction, Rows: rows}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	flushTxn(t, r)

	first, last := <-out, <-out
	defer first.Record.Release()
	defer last.Record.Release()
	if got := lastPos(t, first); got != "u:1-8" || !first.Partial {
		t.Errorf("first record: position %q partial=%v, want the safe position u:1-8 and partial", got, first.Partial)
	}
	if got := lastPos(t, last); got != "u:1-9" || last.Partial {
		t.Errorf("last record: position %q partial=%v, want the transaction's u:1-9 and not partial", got, last.Partial)
	}
}

package mysql

import (
	"reflect"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/spec"
)

// TestDirectPathMatchesRowEncoding pins that the columnar live path lands
// exactly what the old row path (rowToMap -> RecordFromChanges) landed, cell
// for cell. This is the #455 parity bar: no map on the hot path, byte-for-byte
// the same wire record.
func TestDirectPathMatchesRowEncoding(t *testing.T) {
	tbl := ordersTable()
	cs := ordersSchema()

	out := make(chan *dataplane.Batch, 8)
	r := newTestReader(out)
	r.curGTID = "u:1-9"
	r.curCommitTS = testCommitTS
	rows := [][]any{
		{int64(1), []byte("a"), 1.5},
		{int64(2), []byte("b"), 2.5},
	}
	if err := r.OnRow(&canal.RowsEvent{Table: tbl, Action: canal.InsertAction, Rows: rows}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	flushTxn(t, r)
	direct := drainChanges(t, out)

	var changes []rowchange.Change
	for _, row := range rows {
		changes = append(changes, rowchange.Change{
			Op:       rowchange.OpInsert,
			Table:    "raw.orders",
			Position: "u:1-9",
			CommitTS: testCommitTS,
			After:    rowToMap(tbl, row, time.UTC),
		})
	}
	rec, err := transport.RecordFromChanges(changes, cs, nil)
	if err != nil {
		t.Fatalf("row encode: %v", err)
	}
	defer rec.Release()
	rowPath, err := transport.DecodeBatch(rec, "raw.orders", []string{"id"})
	if err != nil {
		t.Fatalf("row decode: %v", err)
	}

	if len(direct) != len(rowPath) {
		t.Fatalf("direct %d changes, row path %d", len(direct), len(rowPath))
	}
	for i := range direct {
		if direct[i].Op != rowPath[i].Op || direct[i].Position != rowPath[i].Position {
			t.Fatalf("row %d: direct op/pos %v/%q, row %v/%q", i, direct[i].Op, direct[i].Position, rowPath[i].Op, rowPath[i].Position)
		}
		for _, col := range []string{"id", "v", "amount"} {
			if !reflect.DeepEqual(direct[i].After[col], rowPath[i].After[col]) {
				t.Fatalf("row %d col %s: direct %#v (%T), row %#v (%T)", i, col, direct[i].After[col], direct[i].After[col], rowPath[i].After[col], rowPath[i].After[col])
			}
		}
	}
}

// TestDirectPathNoPerRowMap pins the #455 memory bar: decoding binlog rows
// straight into the Arrow builders allocates no per-row map[string]any. It
// measures the direct path against the old row path (rowToMap per row, then
// RecordFromChanges) for the same images; the row path's excess is exactly the
// per-row map plus its interface boxing that #455 removes.
//
// The bar was set before the code existed: on the pinned toolchain the direct
// path measures ~2.6 allocs/row and the row path ~4.6. The direct path must
// stay strictly below the row path AND under a fixed 4.0/row ceiling, so a
// future change that reintroduces a map on the hot path fails here.
func TestDirectPathNoPerRowMap(t *testing.T) {
	tbl := ordersTable()
	cs := ordersSchema()
	const n = 300
	images := make([][]any, n)
	for i := range images {
		images[i] = []any{int64(i), []byte("value"), float64(i) + 0.5}
	}

	direct := testing.AllocsPerRun(20, func() {
		te, err := newTableEncoder("raw.orders", cs, tbl, projection{})
		if err != nil {
			t.Fatal(err)
		}
		defer te.enc.Release()
		for _, row := range images {
			if err := te.appendRow(row, tbl, time.UTC, transport.RowMeta{Op: rowchange.OpInsert}); err != nil {
				t.Fatal(err)
			}
		}
		if rec := te.materialize(); rec != nil {
			rec.Release()
		}
	})

	rowPath := testing.AllocsPerRun(20, func() {
		changes := make([]rowchange.Change, 0, n)
		for _, row := range images {
			changes = append(changes, rowchange.Change{
				Op:    rowchange.OpInsert,
				Table: "raw.orders",
				After: rowToMap(tbl, row, time.UTC),
			})
		}
		rec, err := transport.RecordFromChanges(changes, cs, nil)
		if err != nil {
			t.Fatal(err)
		}
		rec.Release()
	})

	directPerRow, rowPerRow := direct/n, rowPath/n
	t.Logf("direct %.2f allocs/row, row path %.2f allocs/row", directPerRow, rowPerRow)
	if directPerRow >= rowPerRow {
		t.Fatalf("direct path allocates %.2f/row, row path %.2f/row — the per-row map was not removed (#455)", directPerRow, rowPerRow)
	}
	if directPerRow >= 4.0 {
		t.Fatalf("direct path allocates %.2f/row, want < 4.0 (the #455 bar)", directPerRow)
	}
}

// TestDirectPathAppliesProjection pins that a columnFilter narrows the wire
// shape: the encoder writes only the projected columns.
func TestDirectPathAppliesProjection(t *testing.T) {
	tbl := ordersTable()
	out := make(chan *dataplane.Batch, 8)
	r := newTestReader(out)
	r.cfg.Schemas["raw.orders"] = core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "v", Type: core.ColumnType{Kind: core.KindString}},
		},
		PrimaryKey: []string{"id"},
	}
	r.curGTID = "u:1-9"
	if err := r.OnRow(&canal.RowsEvent{Table: tbl, Action: canal.InsertAction, Rows: [][]any{{int64(1), []byte("a"), 1.5}}}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	flushTxn(t, r)

	b := <-out
	defer b.Record.Release()
	br, err := transport.NewBatchReader(b.Record, []string{"id"})
	if err != nil {
		t.Fatal(err)
	}
	if br.HasColumn("amount") {
		t.Fatal("projection leaked the excluded column amount")
	}
	if v, ok := br.Value("v", 0); !ok || v != "a" {
		t.Fatalf("projected v = %#v (%v)", v, ok)
	}
}

// TestDirectPathAppliesFilter pins that the row filter still excludes rows on
// the columnar path (the filter reads only the columns it references, but its
// verdict is unchanged).
func TestDirectPathAppliesFilter(t *testing.T) {
	tbl := ordersTable()
	out := make(chan *dataplane.Batch, 8)
	r := newTestReader(out)
	proj, err := newProjection(&spec.Filter{
		Predicate: &spec.Predicate{Column: "id", Op: spec.OpGt, Value: float64(1)},
	}, tbl)
	if err != nil {
		t.Fatal(err)
	}
	r.cfg.Projections["shop.orders"] = proj
	r.projections["shop.orders"] = proj
	r.curGTID = "u:1-9"
	rows := [][]any{
		{int64(1), []byte("drop"), 1.0},
		{int64(2), []byte("keep"), 2.0},
	}
	if err := r.OnRow(&canal.RowsEvent{Table: tbl, Action: canal.InsertAction, Rows: rows}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	flushTxn(t, r)

	chs := drainChanges(t, out)
	if len(chs) != 1 || chs[0].After["id"] != int64(2) {
		t.Fatalf("filtered path = %+v, want only id=2", chs)
	}
}

// TestDirectPathEnumSetParity pins that ENUM/SET normalization survives the
// columnar path (it reads the column definition, not a map).
func TestDirectPathEnumSetParity(t *testing.T) {
	tbl := enumSetTable()
	out := make(chan *dataplane.Batch, 8)
	r := newTestReader(out)
	r.cfg.Schemas = map[string]core.Schema{"raw.orders": {
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "status", Type: core.ColumnType{Kind: core.KindString}},
			{Name: "tags", Type: core.ColumnType{Kind: core.KindString}},
		},
		PrimaryKey: []string{"id"},
	}}
	r.curGTID = "u:1-9"
	if err := r.OnRow(&canal.RowsEvent{Table: tbl, Action: canal.InsertAction, Rows: [][]any{{int64(7), int64(2), int64(0b101)}}}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	flushTxn(t, r)

	chs := drainChanges(t, out)
	if len(chs) != 1 || chs[0].After["status"] != "paid" || chs[0].After["tags"] != "gift,express" {
		t.Fatalf("enum/set direct = %+v", chs)
	}
}

// BenchmarkDirectInsert measures the per-row cost of the columnar live path.
// The point of #455 is that no map[string]any is materialized per row.
func BenchmarkDirectInsert(b *testing.B) {
	tbl := ordersTable()
	rows := make([][]any, 500)
	for i := range rows {
		rows[i] = []any{int64(i), []byte("value"), float64(i) + 0.5}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out := make(chan *dataplane.Batch, 8)
		r := newTestReader(out)
		r.curGTID = "u:1-9"
		e := &canal.RowsEvent{Table: tbl, Action: canal.InsertAction, Rows: rows}
		if err := r.OnRow(e); err != nil {
			b.Fatal(err)
		}
		if err := r.OnPosSynced(&replication.EventHeader{}, gomysql.Position{}, nil, false); err != nil {
			b.Fatal(err)
		}
		for len(out) > 0 {
			(<-out).Record.Release()
		}
	}
}

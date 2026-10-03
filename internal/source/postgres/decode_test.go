package postgres

import (
	"testing"

	pglogrepl "github.com/jackc/pglogrepl"

	"github.com/maltzsama/urutau/source"
)

// orderState is the introspected shape of shop.orders used by the golden
// tests: id bigint, v text, amount numeric, active boolean.
func orderState() *TableState {
	return &TableState{
		Schema: "shop",
		Name:   "orders",
		Columns: []Column{
			{Name: "id", DataType: "bigint"},
			{Name: "v", DataType: "text"},
			{Name: "amount", DataType: "numeric"},
			{Name: "active", DataType: "boolean"},
		},
		PKColumns: []int{0},
	}
}

// col builds one text-format tuple column.
func col(s string) *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeText, Data: []byte(s)}
}

func nullCol() *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeNull}
}

func toastCol() *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeToast}
}

func tuple(cols ...*pglogrepl.TupleDataColumn) *pglogrepl.TupleData {
	return &pglogrepl.TupleData{Columns: cols}
}

func TestTupleRowScalars(t *testing.T) {
	row, err := tupleRow(orderState(), tuple(
		col("42"),
		col("hello world"),
		col("1.99"),
		col("t"),
	), nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if row[0] != int64(42) {
		t.Errorf("id = %v (%T), want int64(42)", row[0], row[0])
	}
	if row[1] != "hello world" {
		t.Errorf("v = %v, want string", row[1])
	}
	if row[2] != "1.99" {
		t.Errorf("amount = %v (%T), want string \"1.99\"", row[2], row[2])
	}
	if row[3] != true {
		t.Errorf("active = %v, want true", row[3])
	}
}

func TestTupleRowNullsAndMoney(t *testing.T) {
	row, err := tupleRow(orderState(), tuple(
		col("7"),
		nullCol(),
		col("$1,234.50"),
		col("f"),
	), nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if row[1] != nil {
		t.Errorf("v = %v (%T), want nil", row[1], row[1])
	}
	if row[2] != "1234.50" {
		t.Errorf("amount = %v (%T), want string \"1234.50\"", row[2], row[2])
	}
	if row[3] != false {
		t.Errorf("active = %v, want false", row[3])
	}
}

func TestTupleRowToastRecoversFromOldImage(t *testing.T) {
	old := tuple(col("1"), col("the original toast"), col("0.5"), col("t"))
	row, err := tupleRow(orderState(), tuple(
		col("1"),
		toastCol(), // unchanged TOAST — recovered from the old tuple
		col("0.75"),
		toastCol(), // recovered too
	), old)
	if err != nil {
		t.Fatalf("decode with old image: %v", err)
	}
	if row[1] != "the original toast" {
		t.Errorf("v = %v, want recovered old value", row[1])
	}
	if row[3] != true {
		t.Errorf("active = %v, want recovered old value", row[3])
	}
}

func TestTupleRowToastWithoutOldImageIsHardError(t *testing.T) {
	_, err := tupleRow(orderState(), tuple(col("1"), toastCol(), col("0.5"), col("t")), nil)
	if err == nil {
		t.Fatal("unchanged TOAST with no old image must be a hard error")
	}
}

func TestTupleRowBadScalarIsHardError(t *testing.T) {
	if _, err := tupleRow(orderState(), tuple(col("not-a-number"), col("x"), col("1"), col("t")), nil); err == nil {
		t.Fatal("bad bigint must be a hard error")
	}
	if _, err := tupleRow(orderState(), tuple(col("1"), col("x"), col("1"), col("maybe")), nil); err == nil {
		t.Fatal("bad boolean must be a hard error")
	}
}

func TestKeyTupleSpecOrder(t *testing.T) {
	st := &TableState{Schema: "shop", Name: "orders", Columns: []Column{
		{Name: "a"}, {Name: "b"}, {Name: "id"},
	}, PKColumns: []int{2, 0}}
	ref := source.TableRef{Source: "shop.orders", Target: "raw.orders", PrimaryKey: []string{"id", "a"}}

	// Positional row in table column order: a, b, id.
	key := keyTuple(st, ref, []any{"x", "y", int64(9)})
	if len(key) != 2 || key[0] != int64(9) || key[1] != "x" {
		t.Errorf("key = %v, want [9 x] in spec order", key)
	}

	// Missing column degrades to nil, never panics.
	key = keyTuple(st, ref, []any{nil, nil, nil})
	if len(key) != 2 || key[0] != nil {
		t.Errorf("key = %v, want [nil nil]", key)
	}
}

// A key-only old tuple ('K') carries just the identity key columns, in key
// order, not the table's full column order — so it is decoded by name into the
// positional row (issue #500).
func TestKeyTupleRowByName(t *testing.T) {
	// orderState is id, v, amount, active. A key (id, v) whose tuple holds two
	// columns: a positional decode would put v into amount/active.
	st := orderState()

	row, err := keyTupleRow(st, tuple(col("7"), col("x")), []string{"id", "v"})
	if err != nil {
		t.Fatal(err)
	}
	if row[0] != int64(7) || row[1] != "x" {
		t.Fatalf("row = %+v, want id=7 v=x", row)
	}
	if row[2] != nil || row[3] != nil {
		t.Fatalf("non-key columns must be nil, got %+v", row)
	}

	// A key that is not the leading column still maps by name.
	st2 := orderState()
	st2.PKColumns = []int{1}

	row, err = keyTupleRow(st2, tuple(col("hello")), []string{"v"})
	if err != nil {
		t.Fatal(err)
	}
	if row[1] != "hello" {
		t.Fatalf("row = %+v, want v=hello", row)
	}

	// A key tuple whose width does not match the key is an error, not a
	// partial or mis-mapped row.
	if _, err := keyTupleRow(st, tuple(col("1"), col("2"), col("3")), []string{"id", "v"}); err == nil {
		t.Fatal("want an error for a key tuple wider than the primary key")
	}
	if _, err := keyTupleRow(st, tuple(col("1")), []string{"id", "v"}); err == nil {
		t.Fatal("want an error for a key tuple narrower than the primary key")
	}
}

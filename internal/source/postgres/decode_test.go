package postgres

import (
	"testing"

	pglogrepl "github.com/jackc/pglogrepl"

	"github.com/maltzsama/urutau/internal/snapshot"
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

func TestTupleToMapScalars(t *testing.T) {
	row, err := tupleToMap(orderState(), tuple(
		col("42"),
		col("hello world"),
		col("1.99"),
		col("t"),
	), nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if row["id"] != int64(42) {
		t.Errorf("id = %v (%T), want int64(42)", row["id"], row["id"])
	}
	if row["v"] != "hello world" {
		t.Errorf("v = %v, want string", row["v"])
	}
	if row["amount"] != 1.99 {
		t.Errorf("amount = %v (%T), want float64(1.99)", row["amount"], row["amount"])
	}
	if row["active"] != true {
		t.Errorf("active = %v, want true", row["active"])
	}
}

func TestTupleToMapNullsAndMoney(t *testing.T) {
	row, err := tupleToMap(orderState(), tuple(
		col("7"),
		nullCol(),
		col("$1,234.50"),
		col("f"),
	), nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if v, ok := row["v"]; !ok || v != nil {
		t.Errorf("v = %v (%T), want nil, present", v, v)
	}
	if row["amount"] != 1234.50 {
		t.Errorf("amount = %v, want 1234.50", row["amount"])
	}
	if row["active"] != false {
		t.Errorf("active = %v, want false", row["active"])
	}
}

func TestTupleToMapToastRecoversFromOldImage(t *testing.T) {
	old := tuple(col("1"), col("the original toast"), col("0.5"), col("t"))
	row, err := tupleToMap(orderState(), tuple(
		col("1"),
		toastCol(), // unchanged TOAST — recovered from the old tuple
		col("0.75"),
		toastCol(), // recovered too
	), old)
	if err != nil {
		t.Fatalf("decode with old image: %v", err)
	}
	if row["v"] != "the original toast" {
		t.Errorf("v = %v, want recovered old value", row["v"])
	}
	if row["active"] != true {
		t.Errorf("active = %v, want recovered old value", row["active"])
	}
}

func TestTupleToMapToastWithoutOldImageIsHardError(t *testing.T) {
	_, err := tupleToMap(orderState(), tuple(col("1"), toastCol(), col("0.5"), col("t")), nil)
	if err == nil {
		t.Fatal("unchanged TOAST with no old image must be a hard error")
	}
}

func TestTupleToMapBadScalarIsHardError(t *testing.T) {
	if _, err := tupleToMap(orderState(), tuple(col("not-a-number"), col("x"), col("1"), col("t")), nil); err == nil {
		t.Fatal("bad bigint must be a hard error")
	}
	if _, err := tupleToMap(orderState(), tuple(col("1"), col("x"), col("1"), col("maybe")), nil); err == nil {
		t.Fatal("bad boolean must be a hard error")
	}
}

func TestKeyFromSpecOrder(t *testing.T) {
	st := &TableState{Schema: "shop", Name: "orders", Columns: []Column{
		{Name: "a"}, {Name: "b"}, {Name: "id"},
	}, PKColumns: []int{2, 0}}
	ref := snapshot.TableRef{Source: "shop.orders", Target: "raw.orders", PrimaryKey: []string{"id", "a"}}

	key := keyFrom(st, ref, map[string]any{"id": int64(9), "a": "x", "b": "y"})
	if len(key) != 2 || key[0] != int64(9) || key[1] != "x" {
		t.Errorf("key = %v, want [9 x] in spec order", key)
	}

	// Missing column degrades to nil, never panics.
	key = keyFrom(st, ref, map[string]any{"a": "x"})
	if len(key) != 2 || key[0] != nil {
		t.Errorf("key = %v, want [nil x]", key)
	}
}

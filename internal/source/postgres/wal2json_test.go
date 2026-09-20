package postgres

import "testing"

func TestCoerceWal2json(t *testing.T) {
	cases := []struct {
		v    any
		typ  string
		want any
	}{
		{float64(42), "bigint", int64(42)},
		{float64(3), "integer", int64(3)},
		{"7", "bigint", int64(7)},
		{float64(1.5), "double precision", 1.5},
		{"1.5", "real", 1.5},
		{"a", "text", "a"},
		{true, "boolean", true},
		{nil, "bigint", nil},
	}
	for _, c := range cases {
		got, err := coerceWal2json(c.v, c.typ)
		if err != nil {
			t.Fatalf("coerceWal2json(%v, %q): %v", c.v, c.typ, err)
		}
		if got != c.want {
			t.Fatalf("coerceWal2json(%v, %q) = %v (%T), want %v (%T)", c.v, c.typ, got, got, c.want, c.want)
		}
	}
}

func TestWal2jsonRow(t *testing.T) {
	st := &TableState{Columns: []Column{
		{Name: "id", DataType: "bigint"},
		{Name: "v", DataType: "text"},
	}}
	row, err := wal2jsonRow(st, []string{"id", "v"}, []any{float64(1), "a"})
	if err != nil {
		t.Fatal(err)
	}
	if row["id"] != int64(1) || row["v"] != "a" {
		t.Fatalf("row = %+v, want id=1 v=a", row)
	}
	// A column absent from the message stays absent.
	row, err = wal2jsonRow(st, []string{"id"}, []any{float64(2)})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := row["v"]; ok {
		t.Fatalf("row = %+v, want no v", row)
	}
}

func TestParseWal2jsonTime(t *testing.T) {
	if got := parseWal2jsonTime("2024-03-01 12:00:00.123456+00"); got.IsZero() {
		t.Fatal("a valid timestamp must parse")
	}
	if got := parseWal2jsonTime(""); !got.IsZero() {
		t.Fatal("an empty timestamp must be zero")
	}
	if got := parseWal2jsonTime("not a time"); !got.IsZero() {
		t.Fatal("an unparseable timestamp must be zero")
	}
}

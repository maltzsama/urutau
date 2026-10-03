package mysql

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/schema"

	"github.com/maltzsama/urutau/spec"
)

func projectionTable() *schema.Table {
	return &schema.Table{
		Schema: "shop", Name: "orders",
		Columns: []schema.TableColumn{
			{Name: "id", Type: schema.TYPE_NUMBER},
			{Name: "v", Type: schema.TYPE_STRING},
			{Name: "amount", Type: schema.TYPE_DECIMAL},
			{Name: "active", Type: schema.TYPE_NUMBER},
		},
		PKColumns: []int{0},
	}
}

// rowFor builds a positional table row (table column order) from a sparse
// named image; columns not named are NULL.
func rowFor(tbl *schema.Table, vals map[string]any) []any {
	row := make([]any, len(tbl.Columns))
	for name, v := range vals {
		if i := tbl.FindColumn(name); i >= 0 {
			row[i] = v
		}
	}
	return row
}

func TestProjectionKeepNumeric(t *testing.T) {
	tbl := projectionTable()
	p, err := newProjection(nil, &spec.Filter{
		Predicate: &spec.Predicate{Column: "active", Op: spec.OpEq, Value: float64(1)},
	}, tbl)
	if err != nil {
		t.Fatal(err)
	}
	ok, err := p.keep(rowFor(tbl, map[string]any{"active": int64(1)}), tbl, nil)
	if err != nil || !ok {
		t.Fatalf("keep(active=1) = %v, %v; want true", ok, err)
	}
	ok, err = p.keep(rowFor(tbl, map[string]any{"active": int64(0)}), tbl, nil)
	if err != nil || ok {
		t.Fatalf("keep(active=0) = %v, %v; want false", ok, err)
	}
	// NULL never satisfies a comparison.
	ok, err = p.keep(rowFor(tbl, map[string]any{"active": nil}), tbl, nil)
	if err != nil || ok {
		t.Fatalf("keep(active=NULL) = %v, %v; want false", ok, err)
	}
}

func TestProjectionKeepDecimalExact(t *testing.T) {
	// DECIMAL compares exactly: a value one ulp past 100.5 must pass, and the
	// boundary itself must not.
	tbl := projectionTable()
	p, err := newProjection(nil, &spec.Filter{
		Predicate: &spec.Predicate{Column: "amount", Op: spec.OpGt, Value: "100.5"},
	}, tbl)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		amount string
		want   bool
	}{
		{"100.6", true},
		{"100.5000001", true},
		{"100.50", false},
		{"100.4", false},
	} {
		ok, err := p.keep(rowFor(tbl, map[string]any{"amount": tc.amount}), tbl, nil)
		if err != nil || ok != tc.want {
			t.Fatalf("keep(amount=%s) = %v, %v; want %v", tc.amount, ok, err, tc.want)
		}
	}
}

func TestProjectionRejectsUnknownFilterColumn(t *testing.T) {
	_, err := newProjection(nil, &spec.Filter{
		Predicate: &spec.Predicate{Column: "nope", Op: spec.OpEq, Value: 1},
	}, projectionTable())
	if err == nil {
		t.Fatal("a filter on an unknown column must error")
	}
}

func TestProjectionKeepCaseInsensitive(t *testing.T) {
	tbl := &schema.Table{
		Schema: "shop", Name: "orders",
		Columns: []schema.TableColumn{
			{Name: "id", Type: schema.TYPE_NUMBER},
			{Name: "status", Type: schema.TYPE_STRING, Collation: "utf8mb4_0900_ai_ci"},
			{Name: "code", Type: schema.TYPE_STRING, Collation: "utf8mb4_bin"},
		},
	}
	// A _ci column matches case-insensitively, matching the snapshot.
	p, err := newProjection(nil, &spec.Filter{
		Predicate: &spec.Predicate{Column: "status", Op: spec.OpEq, Value: "active"},
	}, tbl)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := p.keep(rowFor(tbl, map[string]any{"status": "ACTIVE"}), tbl, nil); err != nil || !ok {
		t.Fatalf("_ci keep(ACTIVE) = %v, %v; want true", ok, err)
	}
	// A _bin column stays case-sensitive.
	b, err := newProjection(nil, &spec.Filter{
		Predicate: &spec.Predicate{Column: "code", Op: spec.OpEq, Value: "abc"},
	}, tbl)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := b.keep(rowFor(tbl, map[string]any{"code": "ABC"}), tbl, nil); err != nil || ok {
		t.Fatalf("_bin keep(ABC) = %v, %v; want false", ok, err)
	}
}

func TestRequireFullImage(t *testing.T) {
	tbl := ordersTable()
	if err := requireFullImage(tbl, []any{int64(1), "a", 1.0}); err != nil {
		t.Fatalf("a full row must pass: %v", err)
	}
	if err := requireFullImage(tbl, []any{int64(1)}); err == nil {
		t.Fatal("a partial row must fail loud")
	}
}

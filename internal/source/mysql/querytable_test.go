package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/schema"

	"github.com/maltzsama/urutau/core"
)

// fakeInformationSchema is a database/sql driver that answers the two
// introspection queries with canned information_schema rows, so QueryTable
// can be exercised end to end without a MySQL server.
//
// It exists because the package's unit tests all built schema.TableColumn by
// hand, which is how #180 hid: the assertions were right, but they asserted a
// shape queryColumns never produced, and queryColumns itself was at 0%
// coverage. Testing the derivation alone would repeat that mistake one layer
// down — a wrong column name in the SELECT still compiles and still passes.
type fakeInformationSchema struct{}

func (fakeInformationSchema) Open(string) (driver.Conn, error) { return fakeISConn{}, nil }

type fakeISConn struct{}

func (fakeISConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unsupported") }
func (fakeISConn) Close() error                        { return nil }
func (fakeISConn) Begin() (driver.Tx, error)           { return nil, errors.New("unsupported") }

// capturedQueries records every SQL string QueryTable issued, so a test can
// assert the query shape as well as the decoded result.
var capturedQueries []string

func (fakeISConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	capturedQueries = append(capturedQueries, query)

	// Both queries are parameterized by (table_schema, table_name); a
	// hardcoded schema/table here would let a missing bind slip through.
	if len(args) != 2 {
		return nil, errors.New("want 2 bind args")
	}
	if args[0].Value != "shop" || args[1].Value != "orders" {
		return nil, errors.New("unexpected bind args")
	}

	switch {
	case strings.Contains(query, "key_column_usage"):
		return &fakeRows{
			cols: []string{"column_name"},
			data: [][]driver.Value{{"id"}, {"tenant"}},
		}, nil
	case strings.Contains(query, "information_schema.columns"):
		// column_name, data_type, column_type, collation_name,
		// numeric_precision, numeric_scale — in the order the SELECT lists.
		return &fakeRows{
			cols: []string{"column_name", "data_type", "column_type", "collation_name", "numeric_precision", "numeric_scale"},
			data: [][]driver.Value{
				{"id", "bigint", "bigint unsigned", nil, int64(20), int64(0)},
				{"tenant", "int", "int(11)", nil, int64(10), int64(0)},
				{"digest", "binary", "binary(16)", nil, int64(0), int64(0)},
				{"name", "varchar", "varchar(64)", "latin1_swedish_ci", int64(0), int64(0)},
				{"status", "enum", "enum('new','paid')", "utf8mb4_general_ci", int64(0), int64(0)},
				{"flags", "set", "set('a','b','c')", "utf8mb4_general_ci", int64(0), int64(0)},
				{"amount", "decimal", "decimal(20,4)", nil, int64(20), int64(4)},
			},
		}, nil
	}
	return nil, errors.New("unexpected query: " + query)
}

type fakeRows struct {
	cols []string
	data [][]driver.Value
	pos  int
}

func (r *fakeRows) Columns() []string { return r.cols }
func (r *fakeRows) Close() error      { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.pos >= len(r.data) {
		return io.EOF
	}
	copy(dest, r.data[r.pos])
	r.pos++
	return nil
}

func init() { sql.Register("urutau_mysql_fake_is", fakeInformationSchema{}) }

func openFakeIS(t *testing.T) *sql.DB {
	t.Helper()
	capturedQueries = nil
	db, err := sql.Open("urutau_mysql_fake_is", "")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// QueryTable must produce columns carrying every modifier, straight out of
// the SELECT — the end-to-end assertion #180 lacked.
func TestQueryTableDerivesColumnModifiers(t *testing.T) {
	tbl, err := QueryTable(context.Background(), openFakeIS(t), "shop", "orders")
	if err != nil {
		t.Fatalf("QueryTable: %v", err)
	}
	if tbl.Schema != "shop" || tbl.Name != "orders" {
		t.Fatalf("table = %s.%s, want shop.orders", tbl.Schema, tbl.Name)
	}

	byName := map[string]schema.TableColumn{}
	for _, c := range tbl.Columns {
		byName[c.Name] = c
	}
	if len(byName) != 7 {
		t.Fatalf("columns = %d, want 7", len(byName))
	}

	// The bug: unsignedness lives only in column_type.
	if !byName["id"].IsUnsigned {
		t.Error("id: IsUnsigned = false, want true (bigint unsigned)")
	}
	if byName["tenant"].IsUnsigned {
		t.Error("tenant: IsUnsigned = true, want false (int(11))")
	}
	if got := byName["digest"].FixedSize; got != 16 {
		t.Errorf("digest: FixedSize = %d, want 16", got)
	}
	if got := byName["name"].Collation; got != "latin1_swedish_ci" {
		t.Errorf("name: Collation = %q, want latin1_swedish_ci", got)
	}
	if got, want := byName["status"].EnumValues, []string{"new", "paid"}; !equalStrings(got, want) {
		t.Errorf("status: EnumValues = %v, want %v", got, want)
	}
	if got, want := byName["flags"].SetValues, []string{"a", "b", "c"}; !equalStrings(got, want) {
		t.Errorf("flags: SetValues = %v, want %v", got, want)
	}
	if got := byName["amount"]; got.MaxSize != 20 || got.FixedSize != 4 {
		t.Errorf("amount: precision/scale = %d/%d, want 20/4", got.MaxSize, got.FixedSize)
	}

	// A NULL collation_name (every non-text column) must not fail the scan.
	if got := byName["id"].Collation; got != "" {
		t.Errorf("id: Collation = %q, want empty for a NULL collation_name", got)
	}
}

// The composite primary key must map to column indexes in key order.
func TestQueryTablePrimaryKeyIndexes(t *testing.T) {
	tbl, err := QueryTable(context.Background(), openFakeIS(t), "shop", "orders")
	if err != nil {
		t.Fatalf("QueryTable: %v", err)
	}
	if len(tbl.PKColumns) != 2 {
		t.Fatalf("PKColumns = %v, want 2 entries", tbl.PKColumns)
	}
	for i, want := range []string{"id", "tenant"} {
		if got := tbl.Columns[tbl.PKColumns[i]].Name; got != want {
			t.Errorf("PKColumns[%d] = %q, want %q", i, got, want)
		}
	}
}

// The SELECT must actually request collation_name and column_type. Deriving
// the modifiers is worthless if the query never reads the columns they come
// from, and a wrong name here still compiles.
func TestQueryTableSelectsTheModifierColumns(t *testing.T) {
	if _, err := QueryTable(context.Background(), openFakeIS(t), "shop", "orders"); err != nil {
		t.Fatalf("QueryTable: %v", err)
	}
	if len(capturedQueries) != 2 {
		t.Fatalf("queries = %d, want 2 (columns, pk)", len(capturedQueries))
	}
	cols := capturedQueries[0]
	for _, want := range []string{"column_type", "collation_name", "numeric_precision", "numeric_scale", "ordinal_position"} {
		if !strings.Contains(cols, want) {
			t.Errorf("columns query does not select %q:\n%s", want, cols)
		}
	}
	if !strings.Contains(capturedQueries[1], "constraint_name = 'PRIMARY'") {
		t.Errorf("pk query does not filter on the PRIMARY constraint:\n%s", capturedQueries[1])
	}
}

// The end the bug actually reached: an unsigned column must arrive at the
// canonical schema as KindUnknown, so a declared cast is required instead of
// an int64 that wraps negative above 2^63.
func TestQueryTableToCanonicalSchema(t *testing.T) {
	tbl, err := QueryTable(context.Background(), openFakeIS(t), "shop", "orders")
	if err != nil {
		t.Fatalf("QueryTable: %v", err)
	}
	cs, err := CanonicalSchema(tbl)
	if err != nil {
		t.Fatalf("CanonicalSchema: %v", err)
	}

	id, ok := cs.Column("id")
	if !ok || id.Type.Kind != core.KindUnknown {
		t.Errorf("id = %+v, want KindUnknown (bigint unsigned needs a cast)", id.Type)
	}
	tenant, ok := cs.Column("tenant")
	if !ok || tenant.Type.Kind != core.KindInt64 {
		t.Errorf("tenant = %+v, want KindInt64", tenant.Type)
	}
	digest, ok := cs.Column("digest")
	if !ok || digest.Type.Kind != core.KindFixedBinary || digest.Type.FixedSize != 16 {
		t.Errorf("digest = %+v, want fixed(16)", digest.Type)
	}
	amount, ok := cs.Column("amount")
	if !ok || amount.Type.Kind != core.KindDecimal || amount.Type.Precision != 20 || amount.Type.Scale != 4 {
		t.Errorf("amount = %+v, want decimal(20,4)", amount.Type)
	}
	if got, want := cs.PrimaryKey, []string{"id", "tenant"}; !equalStrings(got, want) {
		t.Errorf("PrimaryKey = %v, want %v", got, want)
	}
}

// A failing scan must surface, not read as an empty column list — an empty
// table would silently declare a schema with no columns.
func TestQueryColumnsPropagatesScanError(t *testing.T) {
	db, err := sql.Open("urutau_mysql_fake_is", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// Wrong table name: the fake rejects the bind args.
	if _, err := QueryTable(context.Background(), db, "shop", "nope"); err == nil {
		t.Fatal("QueryTable must surface the query error")
	}
}

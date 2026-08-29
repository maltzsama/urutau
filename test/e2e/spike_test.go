package e2e

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	_ "github.com/trinodb/trino-go-client/trino"

	urutauiceberg "github.com/maltzsama/urutau/internal/sink/iceberg"
)

// Spike: prove append/equality-delete semantics by reading the
// result back through Trino, trusting the read and not the commit return.
//
// Finding on iceberg-go v0.6.0: a transaction mixing AppendTable and a
// RowDelta of equality deletes does NOT produce a single snapshot. It stages
// two snapshots (append, then delete); the delete gets the higher sequence
// number and therefore applies to the freshly appended data file as well. So
// a correct upsert must be delete-then-append (separate commits), never
// append-then-delete in one commit. Run with URUTAU_E2E=1 against the stack in
// docker-compose.yml.

const (
	nsName    = "raw"
	tableName = "spike"
)

var (
	spikeArrowSchema = arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "v", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	deleteArrowSchema = arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	spikeIcebergSchema = iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "v", Type: iceberg.PrimitiveTypes.String},
	)
)

func requireE2E(t *testing.T) {
	t.Helper()
	if os.Getenv("URUTAU_E2E") == "" {
		t.Skip("URUTAU_E2E not set; skipping the spike")
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func TestSpike(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cfg := urutauiceberg.Config{
		URI:          env("URUTAU_E2E_CATALOG", "http://localhost:8181/api/catalog"),
		Warehouse:    env("URUTAU_E2E_WAREHOUSE", "quickstart_catalog"),
		ClientID:     env("URUTAU_E2E_CLIENT_ID", "root"),
		ClientSecret: env("URUTAU_E2E_CLIENT_SECRET", "s3cr3t"),
		Scope:        "PRINCIPAL_ROLE:ALL",
	}

	cat, err := urutauiceberg.NewCatalog(ctx, cfg)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	if err := urutauiceberg.EnsureNamespace(ctx, cat, table.Identifier{nsName}); err != nil {
		t.Fatalf("namespace: %v", err)
	}
	ident := table.Identifier{nsName, tableName}
	_ = cat.DropTable(ctx, ident) // table may not exist yet
	tbl, err := cat.CreateTable(ctx, ident, spikeIcebergSchema,
		catalog.WithProperties(iceberg.Properties{"format-version": "2"}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// ---- Proof 1: UPSERT (correct pattern in iceberg-go: delete-then-append).
	// c1: insert id=1 v=a.
	must(t, urutauiceberg.Append(ctx, tbl, spikeRecord(1, "a"), props("p1")))
	tbl = reload(t, ctx, cat, ident)
	// c2: equality-delete id=1 (kills the p1 version).
	must(t, urutauiceberg.DeleteOnly(ctx, tbl, []int{1}, deleteRecord(1), props("p2")))
	tbl = reload(t, ctx, cat, ident)
	// c3: insert id=1 v=b — after the delete, so it survives.
	must(t, urutauiceberg.Append(ctx, tbl, spikeRecord(1, "b"), props("p3")))
	tbl = reload(t, ctx, cat, ident)

	// ---- Proof 2: DELETE.
	// c4: insert id=2 v=x, then c5: equality-delete id=2.
	must(t, urutauiceberg.Append(ctx, tbl, spikeRecord(2, "x"), props("p4")))
	tbl = reload(t, ctx, cat, ident)
	must(t, urutauiceberg.DeleteOnly(ctx, tbl, []int{1}, deleteRecord(2), props("p5")))
	tbl = reload(t, ctx, cat, ident)

	// ---- Proof 3: the gotcha, documented. A single transaction that appends
	// a row AND equality-deletes the same row commits two snapshots; the
	// delete (higher sequence number) applies to the appended file too, so the
	// row is removed — the append is wasted. Naive append+delete-in-one-commit
	// is a data-loss trap in iceberg-go.
	must(t, urutauiceberg.AppendAndDelete(ctx, tbl, spikeRecord(3, "y"), []int{1}, deleteRecord(3), props("p6")))
	tbl = reload(t, ctx, cat, ident)

	assertPositions(t, tbl)

	rows := trinoRows(t, ctx, `SELECT id, v FROM spike ORDER BY id`)
	want := [][2]any{{int64(1), "b"}}
	if len(rows) != len(want) {
		t.Fatalf("proofs: got rows %v, want %v", rows, want)
	}
	for i := range want {
		if rows[i][0] != want[i][0] || rows[i][1] != want[i][1] {
			t.Fatalf("proofs: row %d = %v, want %v", i, rows[i], want[i])
		}
	}
	t.Log("proofs ok: upsert(id=1)=b; delete(id=2) gone; gotcha(id=3) gone (append was killed by co-committed delete)")
}

// assertPositions proves the position mechanisms: the current snapshot carries
// cdc.position in its summary, the table property holds the latest one, and a
// reader (Trino) can see both.
func assertPositions(t *testing.T, tbl *table.Table) {
	t.Helper()
	if got := tbl.Properties()["cdc.position"]; got != "p6" {
		t.Fatalf("cdc.position table property = %q, want p6", got)
	}
	if got := tbl.CurrentSnapshot().Summary.Properties["cdc.position"]; got != "p6" {
		t.Fatalf("cdc.position current snapshot summary = %q, want p6", got)
	}

	props := trinoRows(t, context.Background(), `SELECT key, value FROM "spike$properties"`)
	found := false
	for _, r := range props {
		if r[0] == "cdc.position" && r[1] == "p6" {
			found = true
		}
	}
	if !found {
		t.Fatalf("cdc.position=p6 not visible in Trino $properties: %v", props)
	}

	sums := trinoRows(t, context.Background(),
		`SELECT summary['cdc.position'] FROM "spike$snapshots" ORDER BY committed_at`)
	t.Logf("snapshots: %v", sums)
	if len(sums) != 7 {
		t.Fatalf("expected 7 snapshots in Trino, got %d", len(sums))
	}
	for i, want := range []string{"p1", "p2", "p3", "p4", "p5", "p6", "p6"} {
		if sums[i][0] != want {
			t.Fatalf("snapshot %d cdc.position = %v, want %v", i, sums[i][0], want)
		}
	}
	t.Log("proof 4 ok: cdc.position in snapshot summaries + table property, readable by Trino")
}

func props(pos string) iceberg.Properties {
	return iceberg.Properties{"cdc.position": pos}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func reload(t *testing.T, ctx context.Context, cat *rest.Catalog, ident table.Identifier) *table.Table {
	t.Helper()
	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	return tbl
}

func spikeRecord(id int64, v string) arrow.RecordBatch {
	b := array.NewRecordBuilder(memory.DefaultAllocator, spikeArrowSchema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(id)
	b.Field(1).(*array.StringBuilder).Append(v)
	return b.NewRecordBatch()
}

func deleteRecord(id int64) arrow.RecordBatch {
	b := array.NewRecordBuilder(memory.DefaultAllocator, deleteArrowSchema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(id)
	return b.NewRecordBatch()
}

func trinoRows(t *testing.T, ctx context.Context, query string) [][]any {
	t.Helper()
	dsn := "http://user@" + env("URUTAU_E2E_TRINO", "127.0.0.1:8080") + "?catalog=iceberg&schema=" + nsName
	db, err := sql.Open("trino", dsn)
	if err != nil {
		t.Fatalf("trino open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		t.Fatalf("trino query %q: %v", query, err)
	}
	t.Cleanup(func() { _ = rows.Close() })

	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("trino columns: %v", err)
	}
	var out [][]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("trino scan: %v", err)
		}
		out = append(out, vals)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("trino rows: %v", err)
	}
	return out
}

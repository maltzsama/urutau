package pods

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"

	"github.com/maltzsama/urutau/core"
	icebergsink "github.com/maltzsama/urutau/internal/sink/iceberg"
)

// trinoIDs reads the id column of a sink table through Trino, sorted.
func trinoIDs(t *testing.T, trino *sql.DB, target string) []int64 {
	t.Helper()
	rows, err := trino.Query("SELECT id FROM " + target)
	if err != nil {
		t.Fatalf("trino select %s: %v", target, err)
	}
	defer func() { _ = rows.Close() }()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("trino scan: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("trino rows: %v", err)
	}
	slices.Sort(ids)
	return ids
}

// TestTrinoReadsDeletionVectorsWrittenByIcebergGo is the reader half of the
// format-v3 work: a v3 table whose row-level deletes are deletion vectors
// written by apache/iceberg-go, in the catalog and object store the pipelines
// use, must read back through Trino with exactly the surviving rows —
// including after a second delete replaces a data file's vector with a merged
// one.
func TestTrinoReadsDeletionVectorsWrittenByIcebergGo(t *testing.T) {
	requirePods(t)
	ctx := context.Background()
	t.Setenv("AWS_ACCESS_KEY_ID", "urutau")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "urutau_dev_secret")
	t.Setenv("AWS_REGION", "us-east-1")
	portForward(t, dataNS, "svc/trino", localTrinoPort, 8080)
	trino := openTrino(t, localTrinoPort)

	cat, err := icebergsink.NewCatalog(ctx, icebergsink.Config{
		URI:          "http://polaris.e2e.svc.cluster.local:8181/api/catalog",
		Warehouse:    "quickstart_catalog",
		ClientID:     "root",
		ClientSecret: "s3cr3t",
		Scope:        "PRINCIPAL_ROLE:ALL",
	})
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	if err := icebergsink.EnsureNamespace(ctx, cat, table.Identifier{"raw"}); err != nil {
		t.Fatalf("namespace: %v", err)
	}

	target := uniqueTarget("v3_deletion_vectors")
	ident := table.Identifier{"raw", target}
	schema, err := icebergsink.FromCanonical(core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "v", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
	}})
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	if err := icebergsink.EnsureTable(ctx, cat, ident, schema, nil, []string{"id"}, core.CastPolicy{}, false); err != nil {
		t.Fatalf("EnsureTable: %v", err)
	}
	t.Cleanup(func() { _ = cat.DropTable(context.Background(), ident) })

	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	if v := tbl.Metadata().Version(); v != 3 {
		t.Fatalf("format version = %d, want 3", v)
	}
	arrowSchema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, true, false)
	if err != nil {
		t.Fatalf("arrow schema: %v", err)
	}
	appendIDs := func(from, n int) {
		t.Helper()
		b := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
		defer b.Release()
		for i := range n {
			b.Field(0).(*array.Int64Builder).Append(int64(from + i))
			b.Field(1).(*array.StringBuilder).Append("x")
		}
		rec := b.NewRecordBatch()
		defer rec.Release()
		cur, err := cat.LoadTable(ctx, ident)
		if err != nil {
			t.Fatalf("LoadTable: %v", err)
		}
		txn := cur.NewTransaction()
		if err := txn.AppendTable(ctx, array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{rec}), -1, nil); err != nil {
			t.Fatalf("append: %v", err)
		}
		if _, err := txn.Commit(ctx); err != nil {
			t.Fatalf("commit append: %v", err)
		}
	}
	deleteID := func(id int64) {
		t.Helper()
		cur, err := cat.LoadTable(ctx, ident)
		if err != nil {
			t.Fatalf("LoadTable: %v", err)
		}
		txn := cur.NewTransaction()
		if err := txn.SetProperties(iceberg.Properties{table.WriteDeleteModeKey: table.WriteModeMergeOnRead}); err != nil {
			t.Fatalf("SetProperties: %v", err)
		}
		if err := txn.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), id), nil); err != nil {
			t.Fatalf("delete id=%d: %v", id, err)
		}
		if _, err := txn.Commit(ctx); err != nil {
			t.Fatalf("commit delete id=%d: %v", id, err)
		}
	}
	vectors := func() (total, maxPerFile int) {
		t.Helper()
		cur, err := cat.LoadTable(ctx, ident)
		if err != nil {
			t.Fatalf("LoadTable: %v", err)
		}
		tasks, err := cur.Scan().PlanFiles(ctx)
		if err != nil {
			t.Fatalf("PlanFiles: %v", err)
		}
		seen := map[string]bool{}
		for _, task := range tasks {
			for _, f := range task.DeletionVectorFiles {
				seen[f.FilePath()] = true
			}
			maxPerFile = max(maxPerFile, len(task.DeletionVectorFiles))
		}
		return len(seen), maxPerFile
	}
	expect := func(step string, want []int64) {
		t.Helper()
		if got := trinoIDs(t, trino, target); !slices.Equal(got, want) {
			t.Fatalf("%s: Trino reads ids %v, want %v", step, got, want)
		}
	}

	appendIDs(1, 4) // one data file: 1..4
	appendIDs(5, 4) // another: 5..8
	expect("after the appends", []int64{1, 2, 3, 4, 5, 6, 7, 8})

	deleteID(2)
	if n, _ := vectors(); n != 1 {
		t.Fatalf("after the first delete the plan has %d deletion vectors, want 1", n)
	}
	expect("after the first deletion vector", []int64{1, 3, 4, 5, 6, 7, 8})

	deleteID(3) // same data file: the vector is replaced by a merged one
	deleteID(6) // the other data file gets its own
	if n, per := vectors(); n != 2 || per != 1 {
		t.Fatalf("after three deletes the plan has %d deletion vectors, up to %d per data file; want 2 and 1", n, per)
	}
	expect("after a merged vector and a second file's vector", []int64{1, 4, 5, 7, 8})

	appendIDs(9, 2)
	expect("after an append on top of the vectors", []int64{1, 4, 5, 7, 8, 9, 10})

	var count int64
	if err := trino.QueryRow(fmt.Sprintf("SELECT count(*) FROM %s", target)).Scan(&count); err != nil {
		t.Fatalf("trino count: %v", err)
	}
	if count != 7 {
		t.Fatalf("Trino count(*) = %d, want 7", count)
	}
}

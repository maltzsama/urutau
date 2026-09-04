package e2e

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"

	"github.com/maltzsama/urutau/internal/change"
	"github.com/maltzsama/urutau/internal/core"
	urutauiceberg "github.com/maltzsama/urutau/internal/sink/iceberg"
	"github.com/maltzsama/urutau/internal/spec"
	"github.com/maltzsama/urutau/internal/worker"
)

// TestWorkerEndToEnd drives the collapsed worker with synthetic changes
// loaded from the inline YAML authoring format and proves the final state by
// reading it back through Trino.
func TestWorkerEndToEnd(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Isolated from other e2e tests sharing the stack: drop the target table.
	dropIcebergTable(t, ctx)

	yamlSpec := `
pipeline: e2e
source:
  kind: mysql
  uri: mysql://repl@mysql:3306/e2e
sink:
  uri: polaris://localhost:8181/api/catalog
  namespace: raw
tables:
  - source: e2e.orders
    target: raw.orders
    primaryKey: [id]
    createIfNotExists: true
`
	s, err := spec.LoadYAML(strings.NewReader(yamlSpec))
	if err != nil {
		t.Fatalf("load yaml: %v", err)
	}
	if err := s.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

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

	ident := table.Identifier{"raw", "orders"}
	// Schema derivation from source types arrives with the source plugin;
	// the e2e defines the schema explicitly.
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "v", Type: iceberg.PrimitiveTypes.String},
	)
	if err := urutauiceberg.EnsureNamespace(ctx, cat, table.Identifier{"raw"}); err != nil {
		t.Fatalf("namespace: %v", err)
	}
	if err := urutauiceberg.EnsureTable(ctx, cat, ident, schema, nil, core.CastPolicy{}); err != nil {
		t.Fatalf("ensure table: %v", err)
	}

	wr, err := urutauiceberg.NewTableWriter(ctx, cat, ident, []string{"id"}, core.CastPolicy{}, nil, "")
	if err != nil {
		t.Fatalf("writer: %v", err)
	}

	var committed []change.Batch
	w := worker.New(worker.Config{MaxRows: 8, MaxInterval: 200 * time.Millisecond})
	w.Register("raw.orders", wr, change.UpsertMode)
	w.OnCommit(func(b change.Batch, _ int) { committed = append(committed, b) })

	ingest := make(chan change.Change, 32)
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx, ingest) }()

	// The story: id=1 lives through two updates; id=2 is born and deleted;
	// id=3 is inserted and deleted within the same batch — collapse must keep
	// it out of the table entirely.
	feed := []change.Change{
		{Op: change.OpInsert, Table: "raw.orders", Key: []any{int64(1)}, After: row(1, "a"), Position: "p1"},
		{Op: change.OpUpdate, Table: "raw.orders", Key: []any{int64(1)}, After: row(1, "b"), Position: "p2"},
		{Op: change.OpInsert, Table: "raw.orders", Key: []any{int64(2)}, After: row(2, "x"), Position: "p3"},
		{Op: change.OpDelete, Table: "raw.orders", Key: []any{int64(2)}, Position: "p4"},
		{Op: change.OpInsert, Table: "raw.orders", Key: []any{int64(3)}, After: row(3, "y"), Position: "p5"},
		{Op: change.OpDelete, Table: "raw.orders", Key: []any{int64(3)}, Position: "p6"},
		{Op: change.OpUpdate, Table: "raw.orders", Key: []any{int64(1)}, After: row(1, "c"), Position: "p7"},
	}
	for _, c := range feed {
		ingest <- c
	}
	close(ingest)

	if err := <-done; err != nil {
		t.Fatalf("worker run: %v", err)
	}
	if len(committed) == 0 {
		t.Fatal("no batch was committed")
	}

	rows := trinoRows(t, ctx, `SELECT id, v FROM orders ORDER BY id`)
	want := [][2]any{{int64(1), "c"}}
	if len(rows) != len(want) {
		t.Fatalf("got rows %v, want %v", rows, want)
	}
	if rows[0][0] != want[0][0] || rows[0][1] != want[0][1] {
		t.Fatalf("row = %v, want %v", rows[0], want[0])
	}

	// The position must have advanced to the last change, in the table
	// property and visible to readers.
	last := committed[len(committed)-1].Position
	if last != "p7" {
		t.Fatalf("last committed position = %q, want p7", last)
	}
	props := trinoRows(t, ctx, `SELECT value FROM "orders$properties" WHERE key = 'cdc.position'`)
	if len(props) != 1 || props[0][0] != last {
		t.Fatalf("cdc.position in Trino = %v, want %q", props, last)
	}

	t.Logf("e2e ok: id=1=%v, id=2 gone, id=3 collapsed away, cdc.position=%s", rows[0][1], last)
}

func row(id int64, v string) map[string]any {
	return map[string]any{"id": id, "v": v}
}

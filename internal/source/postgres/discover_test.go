package postgres

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

// TestDiscoverSQL locks the discovery query's shape without a real database
// (the captureTxDriver records the last query text). The three clauses that
// are easy to "simplify" and wrong:
//
//   - relkind IN ('r','p'): matview/foreign tables fail EnsureSetup's
//     REPLICA IDENTITY FULL and CREATE PUBLICATION;
//   - NOT relispartition: a leaf partition has relkind 'r', so without it the
//     parent AND its leaves are discovered and every row replicates twice;
//   - pg\_%: the underscore must be escaped or a user schema like pgx_meta is
//     silently excluded.
func TestDiscoverSQL(t *testing.T) {
	db, err := sql.Open("urutau_capture_tx", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	a := Source{db: db}
	if _, err := a.Discover(context.Background(), nil); err != nil {
		t.Fatalf("Discover: %v", err)
	}
	for _, want := range []string{
		"c.relkind IN ('r','p')",
		"NOT c.relispartition",
		`n.nspname NOT LIKE 'pg\_%'`,
		"n.nspname <> 'information_schema'",
		"has_table_privilege(c.oid, 'SELECT')",
		"has_schema_privilege(current_user, n.nspname, 'USAGE')",
	} {
		if !strings.Contains(capturedChunkQuery, want) {
			t.Fatalf("discover SQL missing %q:\n%s", want, capturedChunkQuery)
		}
	}
	if strings.Contains(capturedChunkQuery, "ANY($1)") {
		t.Fatalf("no schemas must not add the ANY filter:\n%s", capturedChunkQuery)
	}

	// A schema filter adds the ANY clause.
	if _, err := a.Discover(context.Background(), []string{"public"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(capturedChunkQuery, "n.nspname = ANY($1)") {
		t.Fatalf("a schema filter must add ANY($1):\n%s", capturedChunkQuery)
	}
}

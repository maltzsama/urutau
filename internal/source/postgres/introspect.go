// Package source.postgres implements the PostgreSQL replication source:
// a single logical-decoding reader (pgoutput) that decodes row changes
// into change.Change, positions them at their commit LSN, and exposes the
// synced and confirmed positions for the DBLog watermark logic. The
// snapshot path reuses the source-agnostic DBLog orchestrator.
package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/maltzsama/urutau/internal/core"
)

// Column is one introspected source column.
type Column struct {
	Name     string
	DataType string // pg_type native name ("int8", "text", "timestamptz", …)
}

// TableState is the introspection result for one source table: the ordered
// column list and the primary key column indexes, so row decoding and
// Iceberg schema derivation share one path.
type TableState struct {
	Schema    string
	Name      string
	Columns   []Column
	PKColumns []int
}

// FindColumn returns the column index by name, or -1.
func (t *TableState) FindColumn(name string) int {
	for i, c := range t.Columns {
		if c.Name == name {
			return i
		}
	}
	return -1
}

// QueryTable introspects one table via pg_catalog, producing the column
// order the pgoutput relation messages follow.
func QueryTable(ctx context.Context, db *sql.DB, schemaName, tableName string) (*TableState, error) {
	cols, err := queryColumns(ctx, db, schemaName, tableName)
	if err != nil {
		return nil, err
	}
	pkCols, err := queryPK(ctx, db, schemaName, tableName)
	if err != nil {
		return nil, err
	}

	t := &TableState{Schema: schemaName, Name: tableName, Columns: cols}
	for _, pk := range pkCols {
		for i, c := range cols {
			if c.Name == pk {
				t.PKColumns = append(t.PKColumns, i)
			}
		}
	}
	return t, nil
}

func queryColumns(ctx context.Context, db *sql.DB, s, t string) ([]Column, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod)
		FROM pg_catalog.pg_attribute a
		JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2
		  AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY a.attnum`, s, t)
	if err != nil {
		return nil, fmt.Errorf("postgres: columns: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []Column
	for rows.Next() {
		var name, dataType string
		if err := rows.Scan(&name, &dataType); err != nil {
			return nil, err
		}
		out = append(out, Column{Name: name, DataType: dataType})
	}
	return out, rows.Err()
}

func queryPK(ctx context.Context, db *sql.DB, s, t string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT a.attname
		FROM pg_catalog.pg_index i
		JOIN pg_catalog.pg_class c ON c.oid = i.indrelid
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		JOIN unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
		JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum
		WHERE n.nspname = $1 AND c.relname = $2 AND i.indisprimary
		ORDER BY k.ord`, s, t)
	if err != nil {
		return nil, fmt.Errorf("postgres: pk: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

// CanonicalSchema derives the canonical core.Schema from the introspected
// table, mapping the scalar subset the pipeline supports (int64, float64,
// string, bool). Anything outside the subset is a hard error — silent
// coercion is how type bugs are born. The source knows nothing about any
// sink (CR-012).
func CanonicalSchema(tbl *TableState) (core.Schema, error) {
	cols := make([]core.Column, 0, len(tbl.Columns))
	for _, col := range tbl.Columns {
		ct, err := mapColumnType(col)
		if err != nil {
			return core.Schema{}, fmt.Errorf("postgres: %s.%s column %q: %w", tbl.Schema, tbl.Name, col.Name, err)
		}
		cols = append(cols, core.Column{Name: col.Name, Type: ct})
	}
	var pk []string
	for _, idx := range tbl.PKColumns {
		if idx >= 0 && idx < len(tbl.Columns) {
			pk = append(pk, tbl.Columns[idx].Name)
		}
	}
	return core.Schema{Columns: cols, PrimaryKey: pk}, nil
}

// mapColumnType maps a PostgreSQL type to a canonical type, restricted to
// the scalar subset the pipeline implements. Temporal types stay as strings
// in this milestone — the source text representation round-trips losslessly
// and the MySQL source behaves the same way.
func mapColumnType(col Column) (core.ColumnType, error) {
	switch strings.ToLower(col.DataType) {
	case "smallint", "integer", "bigint":
		return core.ColumnType{Kind: core.KindInt64}, nil
	case "real", "double precision", "numeric", "money":
		return core.ColumnType{Kind: core.KindFloat64}, nil
	case "boolean":
		return core.ColumnType{Kind: core.KindBool}, nil
	case "character varying", "character", "text", "citext",
		"date", "timestamp without time zone", "timestamp with time zone",
		"time without time zone", "time with time zone",
		"uuid", "json", "jsonb", "xml",
		"inet", "cidr", "macaddr", "macaddr8", "interval", "bytea":
		return core.ColumnType{Kind: core.KindString}, nil
	default:
		return core.ColumnType{}, fmt.Errorf("unsupported postgres type %q", col.DataType)
	}
}

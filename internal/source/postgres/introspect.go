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

	"github.com/apache/iceberg-go"
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

// IcebergSchema derives an Iceberg schema from the introspected table,
// mapping the scalar subset the writer supports (int64, float64, string,
// bool). Anything outside the subset is a hard error — silent coercion is
// how type bugs are born.
func IcebergSchema(tbl *TableState) (*iceberg.Schema, error) {
	fields := make([]iceberg.NestedField, 0, len(tbl.Columns))
	for id, col := range tbl.Columns {
		itype, err := mapType(col)
		if err != nil {
			return nil, fmt.Errorf("postgres: %s.%s column %q: %w", tbl.Schema, tbl.Name, col.Name, err)
		}
		fields = append(fields, iceberg.NestedField{
			ID:       id + 1, // Iceberg field ids start at 1
			Name:     col.Name,
			Type:     itype,
			Required: false,
		})
	}
	return iceberg.NewSchema(0, fields...), nil
}

// mapType maps a PostgreSQL type to an Iceberg type, restricted to the
// scalar subset the writer implements. Temporal types stay as text in this
// milestone — the source text representation round-trips losslessly and
// the MySQL source (canal with ParseTime=false) behaves the same way.
func mapType(col Column) (iceberg.Type, error) {
	switch strings.ToLower(col.DataType) {
	case "smallint", "integer", "bigint":
		return iceberg.PrimitiveTypes.Int64, nil
	case "real", "double precision", "numeric", "money":
		return iceberg.PrimitiveTypes.Float64, nil
	case "boolean":
		return iceberg.PrimitiveTypes.Bool, nil
	case "character varying", "character", "text", "citext",
		"date", "timestamp without time zone", "timestamp with time zone",
		"time without time zone", "time with time zone",
		"uuid", "json", "jsonb", "xml",
		"inet", "cidr", "macaddr", "macaddr8", "interval", "bytea":
		return iceberg.PrimitiveTypes.String, nil
	default:
		return nil, fmt.Errorf("unsupported postgres type %q", col.DataType)
	}
}

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
// table. Mappable columns map to their canonical kind; unmappable ones are
// carried as KindUnknown so a declared cast can land them explicitly — never
// silent coercion. The source knows nothing about any sink.
func CanonicalSchema(tbl *TableState) (core.Schema, error) {
	cols := make([]core.Column, 0, len(tbl.Columns))
	for _, col := range tbl.Columns {
		cols = append(cols, core.Column{Name: col.Name, Type: mapColumnType(col)})
	}
	var pk []string
	for _, idx := range tbl.PKColumns {
		if idx >= 0 && idx < len(tbl.Columns) {
			pk = append(pk, tbl.Columns[idx].Name)
		}
	}
	return core.Schema{Columns: cols, PrimaryKey: pk}, nil
}

// mapColumnType maps a PostgreSQL type to a canonical type. Integer widths
// map to int64; temporal, uuid and json keep their text representation as
// the canonical value (the pgoutput text form round-trips losslessly, and
// the snapshot chunker produces the same text). Unmappable types become
// KindUnknown so a declared cast is the only way to land them.
func mapColumnType(col Column) core.ColumnType {
	switch strings.ToLower(col.DataType) {
	case "smallint", "integer", "bigint":
		return core.ColumnType{Kind: core.KindInt64}
	case "real", "double precision", "money":
		return core.ColumnType{Kind: core.KindFloat64}
	case "numeric":
		return core.ColumnType{Kind: core.KindDecimal}
	case "boolean":
		return core.ColumnType{Kind: core.KindBool}
	case "character varying", "character", "text", "citext":
		return core.ColumnType{Kind: core.KindString}
	case "date":
		return core.ColumnType{Kind: core.KindDate}
	case "time without time zone", "time with time zone":
		return core.ColumnType{Kind: core.KindTime}
	case "timestamp without time zone":
		return core.ColumnType{Kind: core.KindTimestamp}
	case "timestamp with time zone":
		return core.ColumnType{Kind: core.KindTimestampTZ}
	case "uuid":
		return core.ColumnType{Kind: core.KindUUID}
	case "json", "jsonb":
		return core.ColumnType{Kind: core.KindJSON}
	case "bytea":
		return core.ColumnType{Kind: core.KindBinary}
	default:
		// xml, inet, cidr, macaddr, interval, extensions, … — no canonical
		// form; a cast is the only way to land these columns.
		return core.ColumnType{Kind: core.KindUnknown}
	}
}

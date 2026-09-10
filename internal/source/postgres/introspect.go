// Package source.postgres implements the PostgreSQL replication source:
// a single logical-decoding reader (pgoutput) that decodes row changes
// into rowchange.Change, positions them at their commit LSN, and exposes the
// synced and confirmed positions for the DBLog watermark logic. The
// snapshot path reuses the source-agnostic DBLog orchestrator.
package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/maltzsama/urutau/core"
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
		ct, err := mapColumnType(col.DataType, col.DataType)
		if err != nil {
			return core.Schema{}, fmt.Errorf("postgres: column %q: %w", col.Name, err)
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

// mapColumnType maps a PostgreSQL type to a canonical type. Integer widths
// map to int64; temporal, uuid and json keep their text representation as
// the canonical value (the pgoutput text form round-trips losslessly, and
// the snapshot chunker produces the same text). Unmappable types become
// KindUnknown so a declared cast is the only way to land them.
func mapColumnType(dataType, rawType string) (core.ColumnType, error) {
	switch {
	case dataType == "smallint", dataType == "integer", dataType == "bigint":
		return core.ColumnType{Kind: core.KindInt64}, nil
	case dataType == "real", dataType == "double precision", dataType == "money":
		return core.ColumnType{Kind: core.KindFloat64}, nil
	case strings.HasPrefix(dataType, "numeric"):
		precision, scale, err := parseNumericPrecision(rawType)
		if err != nil {
			return core.ColumnType{}, err
		}
		return core.ColumnType{Kind: core.KindDecimal, Precision: precision, Scale: scale}, nil
	case dataType == "boolean":
		return core.ColumnType{Kind: core.KindBool}, nil
	case dataType == "character varying", dataType == "character", dataType == "text", dataType == "citext":
		return core.ColumnType{Kind: core.KindString}, nil
	case dataType == "date":
		return core.ColumnType{Kind: core.KindDate}, nil
	case dataType == "time without time zone", dataType == "time with time zone":
		return core.ColumnType{Kind: core.KindTime}, nil
	case dataType == "timestamp without time zone":
		return core.ColumnType{Kind: core.KindTimestamp}, nil
	case dataType == "timestamp with time zone":
		return core.ColumnType{Kind: core.KindTimestampTZ}, nil
	case dataType == "uuid":
		return core.ColumnType{Kind: core.KindUUID}, nil
	case dataType == "json", dataType == "jsonb":
		return core.ColumnType{Kind: core.KindJSON}, nil
	case dataType == "bytea":
		return core.ColumnType{Kind: core.KindBinary}, nil
	case dataType == "interval":
		// interval is text-encoded by pgoutput; map to String so the
		// sink stores it verbatim (no canonical numeric form).
		return core.ColumnType{Kind: core.KindString}, nil
	default:
		// xml, inet, cidr, macaddr, interval, extensions, … — no canonical
		// form; a cast is the only way to land them. The type name is
		// carried so the validation error says what the valve is holding.
		return core.ColumnType{Kind: core.KindUnknown,
			Opaque: &core.OpaqueOrigin{TypeName: rawType, VendorName: "postgres"}}, nil
	}
}

// parseNumericPrecision extracts precision and scale from a PostgreSQL
// numeric type string like "numeric(10,2)". Absent (plain "numeric") is
// 0,0; a present but unparseable or malformed value is an error — a silent 0
// would look like a legitimate default and hide broken introspection.
func parseNumericPrecision(s string) (precision, scale int, err error) {
	s = strings.TrimPrefix(s, "numeric")
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "(") {
		return 0, 0, nil
	}
	s = strings.TrimPrefix(s, "(")
	s = strings.TrimSuffix(s, ")")
	parts := strings.Split(s, ",")
	if len(parts) == 1 {
		if _, err := fmt.Sscanf(parts[0], "%d", &precision); err != nil {
			return 0, 0, fmt.Errorf("numeric precision %q: %w", parts[0], err)
		}
		return precision, 0, nil
	}
	if len(parts) == 2 {
		if _, err := fmt.Sscanf(parts[0], "%d", &precision); err != nil {
			return 0, 0, fmt.Errorf("numeric precision %q: %w", parts[0], err)
		}
		if _, err := fmt.Sscanf(parts[1], "%d", &scale); err != nil {
			return 0, 0, fmt.Errorf("numeric scale %q: %w", parts[1], err)
		}
		return precision, scale, nil
	}
	return 0, 0, fmt.Errorf("numeric type %q: expected at most precision,scale", s)
}

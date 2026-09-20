package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/go-mysql-org/go-mysql/schema"
)

// QueryTable introspects one table via information_schema, producing the
// same schema.Table the canal decoder would fetch — so row decoding and
// Iceberg schema derivation share one path.
func QueryTable(ctx context.Context, db *sql.DB, schemaName, tableName string) (*schema.Table, error) {
	cols, err := queryColumns(ctx, db, schemaName, tableName)
	if err != nil {
		return nil, err
	}
	pkCols, err := queryPK(ctx, db, schemaName, tableName)
	if err != nil {
		return nil, err
	}

	t := &schema.Table{Schema: schemaName, Name: tableName, Columns: cols}
	for _, pk := range pkCols {
		for i, c := range cols {
			if c.Name == pk {
				t.PKColumns = append(t.PKColumns, i)
			}
		}
	}
	return t, nil
}

func queryColumns(ctx context.Context, db *sql.DB, s, t string) ([]schema.TableColumn, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT column_name, data_type, column_type, collation_name,
		       COALESCE(numeric_precision, 0), COALESCE(numeric_scale, 0)
		FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ?
		ORDER BY ordinal_position`, s, t)
	if err != nil {
		return nil, fmt.Errorf("mysql: columns: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []schema.TableColumn
	for rows.Next() {
		var name, dataType, colType string
		var collation sql.NullString
		var precision, scale int
		if err := rows.Scan(&name, &dataType, &colType, &collation, &precision, &scale); err != nil {
			return nil, err
		}
		out = append(out, buildColumn(name, dataType, colType, collation.String, precision, scale))
	}
	return out, rows.Err()
}

// buildColumn assembles one schema.TableColumn from an information_schema
// row. The classification comes from data_type, but every modifier —
// unsignedness, fixed size, ENUM/SET members — lives only in column_type, so
// both are needed.
//
// Deriving these here is what makes the mapColumnType and normalizeCol
// branches that read them reachable at all: the canal path gets them from
// go-mysql's own AddColumn, and before #180 the introspection path silently
// left them at their zero values. A BIGINT UNSIGNED was declared KindInt64
// and wrapped negative above 2^63; binary(16) lost its fixed size; a
// non-UTF-8 column lost the collation its charset decoder keys on.
func buildColumn(name, dataType, columnType, collation string, precision, scale int) schema.TableColumn {
	col := schema.TableColumn{
		Name:      name,
		RawType:   columnType,
		Type:      mapTypeByName(dataType),
		Collation: collation,
	}

	// "unsigned" and "zerofill" both imply unsignedness, matching go-mysql
	// (schema.AddColumn): a zerofill column is unsigned by definition.
	lower := strings.ToLower(columnType)
	col.IsUnsigned = strings.Contains(lower, "unsigned") || strings.Contains(lower, "zerofill")

	switch col.Type {
	case schema.TYPE_ENUM:
		col.EnumValues = parseMemberList(columnType, "enum")
	case schema.TYPE_SET:
		col.SetValues = parseMemberList(columnType, "set")
	case schema.TYPE_BINARY:
		// Only BINARY is fixed-width; VARBINARY's size is a maximum.
		if strings.HasPrefix(lower, "binary") {
			col.FixedSize = parseSize(columnType)
		}
		col.MaxSize = parseSize(columnType)
	case schema.TYPE_DECIMAL:
		// Precision and scale come from the dedicated information_schema
		// columns when present; RawType is the fallback so the canal path
		// (which has no information_schema row) resolves the same values.
		if precision == 0 && scale == 0 {
			precision, scale = parseDecimalSpec(columnType)
		}
		col.MaxSize = uint(precision)
		col.FixedSize = uint(scale)
	case schema.TYPE_STRING:
		if strings.HasPrefix(lower, "char") {
			col.FixedSize = parseSize(columnType)
		}
		col.MaxSize = parseSize(columnType)
	}
	return col
}

// parseSize reads the first parenthesised number of a column_type, e.g.
// binary(16) or varchar(64). Absent or malformed parentheses give 0.
func parseSize(columnType string) uint {
	open := strings.Index(columnType, "(")
	closeIdx := strings.Index(columnType, ")")
	if open < 0 || closeIdx < 0 || open > closeIdx {
		return 0
	}
	// decimal(20,4): the size is the part before the comma.
	inner := columnType[open+1 : closeIdx]
	if comma := strings.Index(inner, ","); comma >= 0 {
		inner = inner[:comma]
	}
	n, err := strconv.ParseUint(strings.TrimSpace(inner), 10, 32)
	if err != nil {
		return 0
	}
	return uint(n)
}

// parseDecimalSpec reads precision and scale from a decimal(p,s) column_type.
// A bare "decimal" yields 0,0 — MySQL's own defaults are 10,0, but inventing
// them here would hide a column the introspection failed to describe.
func parseDecimalSpec(columnType string) (precision, scale int) {
	open := strings.Index(columnType, "(")
	closeIdx := strings.Index(columnType, ")")
	if open < 0 || closeIdx < 0 || open > closeIdx {
		return 0, 0
	}
	parts := strings.SplitN(columnType[open+1:closeIdx], ",", 2)
	if p, err := strconv.Atoi(strings.TrimSpace(parts[0])); err == nil {
		precision = p
	}
	if len(parts) == 2 {
		if s, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
			scale = s
		}
	}
	return precision, scale
}

// parseMemberList splits the member list of an ENUM or SET column_type, e.g.
// enum('a','b'). MySQL escapes an embedded quote by doubling it: two single
// quotes inside a member are one literal quote and must not end the member.
func parseMemberList(columnType, prefix string) []string {
	lower := strings.ToLower(columnType)
	if !strings.HasPrefix(lower, prefix+"(") || !strings.HasSuffix(columnType, ")") {
		return nil
	}
	body := columnType[len(prefix)+1 : len(columnType)-1]

	var (
		out     []string
		cur     strings.Builder
		inQuote bool
	)
	for i := 0; i < len(body); i++ {
		c := body[i]
		switch {
		case c == '\'' && inQuote && i+1 < len(body) && body[i+1] == '\'':
			cur.WriteByte('\'') // doubled quote: one literal quote
			i++
		case c == '\'':
			inQuote = !inQuote
			if !inQuote {
				out = append(out, cur.String())
				cur.Reset()
			}
		case inQuote:
			cur.WriteByte(c)
		}
	}
	return out
}

func queryPK(ctx context.Context, db *sql.DB, s, t string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT column_name FROM information_schema.key_column_usage
		WHERE table_schema = ? AND table_name = ? AND constraint_name = 'PRIMARY'
		ORDER BY ordinal_position`, s, t)
	if err != nil {
		return nil, fmt.Errorf("mysql: pk: %w", err)
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

// mapTypeByName maps a MySQL data_type string to the canal schema.Type
// constants, so the canonical mapping reuses the same classification as the
// canal runtime path (schema.Table.AddColumn).
func mapTypeByName(dataType string) int {
	t := strings.ToLower(dataType)
	switch {
	case strings.HasPrefix(t, "float"), strings.HasPrefix(t, "double"):
		return schema.TYPE_FLOAT
	case strings.HasPrefix(t, "decimal"), t == "numeric", t == "real":
		return schema.TYPE_DECIMAL
	case strings.HasPrefix(t, "enum"):
		return schema.TYPE_ENUM
	case strings.HasPrefix(t, "set"):
		return schema.TYPE_SET
	case strings.HasPrefix(t, "binary"), strings.HasPrefix(t, "varbinary"):
		return schema.TYPE_BINARY
	case strings.HasPrefix(t, "datetime"):
		return schema.TYPE_DATETIME
	case strings.HasPrefix(t, "timestamp"):
		return schema.TYPE_TIMESTAMP
	case strings.HasPrefix(t, "time"):
		return schema.TYPE_TIME
	case t == "date":
		return schema.TYPE_DATE
	case strings.HasPrefix(t, "bit"):
		return schema.TYPE_BIT
	case strings.HasPrefix(t, "json"):
		return schema.TYPE_JSON
	case strings.HasPrefix(t, "mediumint"):
		return schema.TYPE_MEDIUM_INT
	case strings.HasPrefix(t, "int"), strings.HasPrefix(t, "smallint"),
		strings.HasPrefix(t, "tinyint"), strings.HasPrefix(t, "bigint"),
		strings.HasPrefix(t, "year"):
		return schema.TYPE_NUMBER
	default:
		// char, varchar, text, blob and everything else — the canal maps
		// the residual to TYPE_STRING.
		return schema.TYPE_STRING
	}
}

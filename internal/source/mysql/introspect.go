package mysql

import (
	"context"
	"database/sql"
	"fmt"
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
		SELECT column_name, data_type, column_type
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
		if err := rows.Scan(&name, &dataType, &colType); err != nil {
			return nil, err
		}
		out = append(out, schema.TableColumn{Name: name, RawType: colType, Type: mapTypeByName(dataType)})
	}
	return out, rows.Err()
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
// constants, so IcebergSchema can reuse the same mapping for both the canal
// path and the information_schema path.
func mapTypeByName(dataType string) int {
	switch strings.ToLower(dataType) {
	case "bigint", "int", "smallint", "tinyint", "mediumint", "year":
		return schema.TYPE_NUMBER
	case "double", "float", "decimal", "numeric", "real":
		return schema.TYPE_FLOAT
	case "char", "varchar", "text", "tinytext", "mediumtext", "longtext", "enum", "set", "json":
		return schema.TYPE_STRING
	default:
		return 0 // schema.TYPE_NUMBER = iota + 1, so 0 marks unknown
	}
}

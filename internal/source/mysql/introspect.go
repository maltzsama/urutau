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

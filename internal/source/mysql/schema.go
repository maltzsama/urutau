package mysql

import (
	"fmt"

	"github.com/apache/iceberg-go"
	"github.com/go-mysql-org/go-mysql/schema"
)

// IcebergSchema derives an Iceberg schema from the canal table metadata,
// mapping the scalar subset the writer supports. Anything outside the subset
// is a hard error — silent coercion is how type bugs are born.
func IcebergSchema(tbl *schema.Table) (*iceberg.Schema, error) {
	fields := make([]iceberg.NestedField, 0, len(tbl.Columns))
	for id, col := range tbl.Columns {
		itype, err := mapType(col)
		if err != nil {
			return nil, fmt.Errorf("mysql: %s.%s column %q: %w", tbl.Schema, tbl.Name, col.Name, err)
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

// mapType maps a MySQL column to an Iceberg type, restricted to the scalar
// subset the writer implements (int64, string, float64, bool).
func mapType(col schema.TableColumn) (iceberg.Type, error) {
	switch col.Type {
	case schema.TYPE_NUMBER:
		if col.IsUnsigned {
			// BIGINT UNSIGNED overflows int64 — out of the scalar subset.
			return nil, fmt.Errorf("unsigned numeric (%s): unsupported", col.RawType)
		}
		return iceberg.PrimitiveTypes.Int64, nil
	case schema.TYPE_FLOAT:
		return iceberg.PrimitiveTypes.Float64, nil
	case schema.TYPE_STRING, schema.TYPE_ENUM:
		return iceberg.PrimitiveTypes.String, nil
	default:
		return nil, fmt.Errorf("unsupported mysql type %d (%s)", col.Type, col.RawType)
	}
}

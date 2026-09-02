package mysql

import (
	"fmt"

	"github.com/go-mysql-org/go-mysql/schema"

	"github.com/maltzsama/urutau/internal/core"
)

// CanonicalSchema derives the canonical core.Schema from the canal table
// metadata, mapping the scalar subset the pipeline supports. Anything
// outside the subset is a hard error — silent coercion is how type bugs are
// born. The source knows nothing about any sink: this is the source side of
// the canonical type system (CR-012).
func CanonicalSchema(tbl *schema.Table) (core.Schema, error) {
	cols := make([]core.Column, 0, len(tbl.Columns))
	for _, col := range tbl.Columns {
		ct, err := mapColumnType(col)
		if err != nil {
			return core.Schema{}, fmt.Errorf("mysql: %s.%s column %q: %w", tbl.Schema, tbl.Name, col.Name, err)
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

// mapColumnType maps a MySQL column to a canonical type, restricted to the
// scalar subset the pipeline implements (int64, string, float64, bool).
func mapColumnType(col schema.TableColumn) (core.ColumnType, error) {
	switch col.Type {
	case schema.TYPE_NUMBER:
		if col.IsUnsigned {
			// BIGINT UNSIGNED overflows int64 — out of the scalar subset.
			return core.ColumnType{}, fmt.Errorf("unsigned numeric (%s): unsupported", col.RawType)
		}
		return core.ColumnType{Kind: core.KindInt64}, nil
	case schema.TYPE_FLOAT:
		return core.ColumnType{Kind: core.KindFloat64}, nil
	case schema.TYPE_STRING, schema.TYPE_ENUM:
		return core.ColumnType{Kind: core.KindString}, nil
	default:
		return core.ColumnType{}, fmt.Errorf("unsupported mysql type %d (%s)", col.Type, col.RawType)
	}
}
package mysql

import (
	"fmt"

	"github.com/go-mysql-org/go-mysql/schema"

	"github.com/maltzsama/urutau/core"
)

// CanonicalSchema derives the canonical core.Schema from the canal table
// metadata. Mappable columns map to their canonical kind; unmappable ones
// (unsigned numerics, geometry, bit, …) are carried as KindUnknown so a
// declared cast can land them explicitly — never silent coercion. The source
// knows nothing about any sink: this is the source side of the canonical
// type system.
func CanonicalSchema(tbl *schema.Table) (core.Schema, error) {
	cols := make([]core.Column, 0, len(tbl.Columns))
	for _, col := range tbl.Columns {
		ct, err := mapColumnType(col)
		if err != nil {
			return core.Schema{}, fmt.Errorf("mysql: column %q: %w", col.Name, err)
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

// mapColumnType maps a MySQL column to a canonical type. Integer widths map
// to int64; the canonical value space carries ints as int64. Unmappable
// columns become KindUnknown so the declared cast is the only way to land
// them — the pipeline never coerces silently.
func mapColumnType(col schema.TableColumn) (core.ColumnType, error) {
	switch col.Type {
	case schema.TYPE_NUMBER, schema.TYPE_MEDIUM_INT:
		if col.IsUnsigned {
			// BIGINT UNSIGNED overflows int64; a declared cast (e.g. to
			// string) is the explicit way to land it.
			return core.ColumnType{Kind: core.KindUnknown,
				Opaque: &core.OpaqueOrigin{TypeName: col.RawType, VendorName: "mysql"}}, nil
		}
		return core.ColumnType{Kind: core.KindInt64}, nil
	case schema.TYPE_FLOAT:
		return core.ColumnType{Kind: core.KindFloat64}, nil
	case schema.TYPE_DECIMAL:
		// Precision and scale are carried via EnumValues[0] as "precision,scale"
		// by the introspection path (queryColumns). When absent (canal runtime),
		// both default to 0 — the sink infers its own defaults. A PRESENT but
		// unparseable value is an error: silently returning 0 would look like a
		// legitimate default and hide a broken introspection.
		precision, scale := 0, 0
		if len(col.EnumValues) == 1 {
			if _, err := fmt.Sscanf(col.EnumValues[0], "%d,%d", &precision, &scale); err != nil {
				return core.ColumnType{}, fmt.Errorf("decimal precision/scale %q: %w", col.EnumValues[0], err)
			}
		}
		return core.ColumnType{Kind: core.KindDecimal, Precision: precision, Scale: scale}, nil
	case schema.TYPE_STRING, schema.TYPE_ENUM, schema.TYPE_SET:
		return core.ColumnType{Kind: core.KindString}, nil
	case schema.TYPE_DATE:
		return core.ColumnType{Kind: core.KindDate}, nil
	case schema.TYPE_DATETIME:
		return core.ColumnType{Kind: core.KindTimestamp}, nil
	case schema.TYPE_TIMESTAMP:
		return core.ColumnType{Kind: core.KindTimestampTZ}, nil
	case schema.TYPE_TIME:
		return core.ColumnType{Kind: core.KindTime}, nil
	case schema.TYPE_JSON:
		return core.ColumnType{Kind: core.KindJSON}, nil
	case schema.TYPE_BINARY:
		// go-mysql maps both binary(n) and varbinary(n) to TYPE_BINARY; the
		// fixed-size flag distinguishes them. A fixed byte sequence keeps
		// its declared length (Iceberg fixed(L)) instead of degrading to
		// variable binary.
		if col.FixedSize > 0 {
			return core.ColumnType{Kind: core.KindFixedBinary, FixedSize: int(col.FixedSize)}, nil
		}
		return core.ColumnType{Kind: core.KindBinary}, nil
	default:
		// TYPE_BIT, TYPE_POINT, geometry, unknown — no canonical form; a
		// cast is the only way to land these columns. The raw type name is
		// carried so the error says what the valve is holding.
		return core.ColumnType{Kind: core.KindUnknown,
			Opaque: &core.OpaqueOrigin{TypeName: col.RawType, VendorName: "mysql"}}, nil
	}
}

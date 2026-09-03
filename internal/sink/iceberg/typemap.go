package iceberg

import (
	"fmt"

	"github.com/apache/iceberg-go"

	"github.com/maltzsama/urutau/internal/core"
)

// FromCanonical maps a canonical core.Schema into an Iceberg schema. This is
// the sink side of the canonical type system: sources map into core.Kind,
// sinks map out of it. Field IDs are assigned 1..N in order.
func FromCanonical(cs core.Schema) (*iceberg.Schema, error) {
	fields := make([]iceberg.NestedField, 0, len(cs.Columns))
	for i, col := range cs.Columns {
		itype, err := mapCanonicalType(col.Type)
		if err != nil {
			return nil, fmt.Errorf("iceberg: column %q: %w", col.Name, err)
		}
		fields = append(fields, iceberg.NestedField{
			ID:       i + 1, // Iceberg field ids start at 1
			Name:     col.Name,
			Type:     itype,
			Required: false,
		})
	}
	return iceberg.NewSchema(0, fields...), nil
}

// mapCanonicalType maps a canonical kind to an Iceberg primitive. Anything
// outside the supported set is a hard error — silent coercion is how type
// bugs are born. A sink may be more restricted than the canonical set
// without contaminating the source. JSON has no Iceberg primitive, so it
// lands as a string.
func mapCanonicalType(t core.ColumnType) (iceberg.Type, error) {
	switch t.Kind {
	case core.KindBool:
		return iceberg.PrimitiveTypes.Bool, nil
	case core.KindInt32:
		return iceberg.PrimitiveTypes.Int32, nil
	case core.KindInt64:
		return iceberg.PrimitiveTypes.Int64, nil
	case core.KindFloat32:
		return iceberg.PrimitiveTypes.Float32, nil
	case core.KindFloat64:
		return iceberg.PrimitiveTypes.Float64, nil
	case core.KindDecimal:
		if t.Precision <= 0 {
			return nil, fmt.Errorf("decimal requires precision > 0")
		}
		return iceberg.DecimalTypeOf(t.Precision, t.Scale), nil
	case core.KindString, core.KindJSON:
		return iceberg.PrimitiveTypes.String, nil
	case core.KindBinary:
		return iceberg.PrimitiveTypes.Binary, nil
	case core.KindDate:
		return iceberg.PrimitiveTypes.Date, nil
	case core.KindTime:
		return iceberg.PrimitiveTypes.Time, nil
	case core.KindTimestamp:
		return iceberg.PrimitiveTypes.Timestamp, nil
	case core.KindTimestampTZ:
		return iceberg.PrimitiveTypes.TimestampTz, nil
	case core.KindUUID:
		return iceberg.PrimitiveTypes.UUID, nil
	default:
		return nil, fmt.Errorf("iceberg: unsupported canonical kind %s", t.Kind)
	}
}
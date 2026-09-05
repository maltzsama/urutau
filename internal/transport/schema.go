// Typed wire schema derivation: maps the canonical core.Schema into a typed
// Arrow schema for the Flight data plane. Each column travels as its native
// Arrow type — no JSON blobs, no lossy round-trips.
package transport

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/maltzsama/urutau/core"
)

// Extension metadata carried on wire fields. UUID and fixed(16) are both
// FixedSizeBinary in Arrow; the extension distinguishes them so decode
// reconstructs the right canonical Kind. JSON is physically a string; the
// extension labels it for consumers reading the RecordBatch directly.
const (
	extNameKey = "ARROW:extension:name"
	extUUID    = "arrow.uuid"
	extJSON    = "arrow.json"
)

// fieldMetadata returns the extension metadata a canonical kind carries on
// the wire, or empty metadata when it carries none.
func fieldMetadata(ct core.ColumnType) arrow.Metadata {
	switch ct.Kind {
	case core.KindUUID:
		return arrow.NewMetadata([]string{extNameKey}, []string{extUUID})
	case core.KindJSON:
		return arrow.NewMetadata([]string{extNameKey}, []string{extJSON})
	default:
		return arrow.Metadata{}
	}
}

// fieldTypeToCore reconstructs the canonical type of a wire field, honoring
// extension metadata (uuid, json) before falling back to the raw Arrow type.
func fieldTypeToCore(f arrow.Field) core.ColumnType {
	if v, ok := f.Metadata.GetValue(extNameKey); ok {
		switch v {
		case extUUID:
			return core.ColumnType{Kind: core.KindUUID}
		case extJSON:
			return core.ColumnType{Kind: core.KindJSON}
		}
	}
	return arrowTypeToCore(f.Type)
}

// CoreSchemaToArrow maps a canonical core.Schema into a typed Arrow schema.
// Data columns appear in schema order, followed by the fixed metadata
// columns (__op, __pos, __commit_ts, __ingest_ts, __snapshot). This is
// the wire schema used by EncodeBatch/DecodeBatch.
func CoreSchemaToArrow(cs core.Schema) (*arrow.Schema, error) {
	fields := make([]arrow.Field, 0, len(cs.Columns)+8)

	// Data columns: typed per core.Kind.
	for _, col := range cs.Columns {
		af, err := columnToArrowField(col)
		if err != nil {
			return nil, fmt.Errorf("transport: column %q: %w", col.Name, err)
		}
		fields = append(fields, af)
	}

	// Metadata columns: always nullable, appended at the end.
	fields = append(fields,
		arrow.Field{Name: "__op", Type: arrow.PrimitiveTypes.Uint8, Nullable: false},
		arrow.Field{Name: "__pos", Type: arrow.BinaryTypes.String, Nullable: false},
		arrow.Field{Name: "__commit_ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		arrow.Field{Name: "__ingest_ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		arrow.Field{Name: "__snapshot", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
	)

	return arrow.NewSchema(fields, nil), nil
}

// columnToArrowField builds the arrow.Field for one canonical column,
// recursing into composite kinds so nested struct fields carry their own
// nullability and extension metadata (uuid/json labels survive inside a
// struct, not just at the top level).
func columnToArrowField(col core.Column) (arrow.Field, error) {
	at, err := kindToArrow(col.Type)
	if err != nil {
		return arrow.Field{}, err
	}
	return arrow.Field{
		Name:     col.Name,
		Type:     at,
		Nullable: col.Type.Nullable,
		Metadata: fieldMetadata(col.Type),
	}, nil
}

// kindToArrow maps a canonical ColumnType to its Arrow type. Scalars map to
// primitives; composite kinds recurse. Element and value nullability are
// carried on the child field where Arrow supports it.
func kindToArrow(ct core.ColumnType) (arrow.DataType, error) {
	switch ct.Kind {
	case core.KindStruct:
		if len(ct.Fields) == 0 {
			return nil, fmt.Errorf("struct requires fields")
		}
		fields := make([]arrow.Field, len(ct.Fields))
		for i, f := range ct.Fields {
			af, err := columnToArrowField(f)
			if err != nil {
				return nil, fmt.Errorf("struct field %q: %w", f.Name, err)
			}
			fields[i] = af
		}
		return arrow.StructOf(fields...), nil
	case core.KindList:
		if ct.Elem == nil {
			return nil, fmt.Errorf("list requires an element type")
		}
		at, err := kindToArrow(*ct.Elem)
		if err != nil {
			return nil, err
		}
		return arrow.ListOfField(arrow.Field{
			Name:     "item",
			Type:     at,
			Nullable: ct.Elem.Nullable,
			Metadata: fieldMetadata(*ct.Elem),
		}), nil
	case core.KindMap:
		if ct.KeyType == nil || ct.ValueType == nil {
			return nil, fmt.Errorf("map requires key and value types")
		}
		kt, err := kindToArrow(*ct.KeyType)
		if err != nil {
			return nil, err
		}
		vt, err := kindToArrow(*ct.ValueType)
		if err != nil {
			return nil, err
		}
		// Arrow map keys are never nullable; Iceberg matches this.
		return arrow.MapOfFields(
			arrow.Field{Name: "key", Type: kt, Nullable: false, Metadata: fieldMetadata(*ct.KeyType)},
			arrow.Field{Name: "value", Type: vt, Nullable: ct.ValueType.Nullable, Metadata: fieldMetadata(*ct.ValueType)},
		), nil
	default:
		return kindToArrowScalar(ct)
	}
}

// kindToArrowScalar maps a canonical scalar ColumnType to its Arrow
// primitive. Composite kinds are handled by kindToArrow.
func kindToArrowScalar(ct core.ColumnType) (arrow.DataType, error) {
	switch ct.Kind {
	case core.KindBool:
		return arrow.FixedWidthTypes.Boolean, nil
	case core.KindInt32:
		return arrow.PrimitiveTypes.Int32, nil
	case core.KindInt64:
		return arrow.PrimitiveTypes.Int64, nil
	case core.KindFloat32:
		return arrow.PrimitiveTypes.Float32, nil
	case core.KindFloat64:
		return arrow.PrimitiveTypes.Float64, nil
	case core.KindDecimal:
		if ct.Precision <= 0 {
			return nil, fmt.Errorf("decimal requires precision > 0")
		}
		return &arrow.Decimal128Type{Precision: int32(ct.Precision), Scale: int32(ct.Scale)}, nil
	case core.KindString, core.KindJSON:
		return arrow.BinaryTypes.String, nil
	case core.KindBinary:
		return arrow.BinaryTypes.Binary, nil
	case core.KindFixedBinary:
		if ct.FixedSize <= 0 {
			return nil, fmt.Errorf("fixed requires size > 0")
		}
		return &arrow.FixedSizeBinaryType{ByteWidth: ct.FixedSize}, nil
	case core.KindDate:
		return arrow.PrimitiveTypes.Int32, nil // days since epoch, stored as int32
	case core.KindTime:
		return arrow.PrimitiveTypes.Int64, nil // micros since midnight
	case core.KindTimestamp:
		return &arrow.TimestampType{Unit: arrow.Microsecond}, nil
	case core.KindTimestampTZ:
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, nil
	case core.KindUUID:
		return &arrow.FixedSizeBinaryType{ByteWidth: 16}, nil
	default:
		return nil, fmt.Errorf("unsupported canonical kind %s", ct.Kind)
	}
}

// IsMetadataColumn returns true for the fixed metadata column names.
func IsMetadataColumn(name string) bool {
	switch name {
	case "__op", "__pos", "__commit_ts", "__ingest_ts", "__snapshot":
		return true
	}
	return false
}

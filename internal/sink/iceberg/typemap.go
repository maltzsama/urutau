package iceberg

import (
	"fmt"

	"github.com/apache/iceberg-go"

	"github.com/maltzsama/urutau/internal/core"
)

// FromCanonical maps a canonical core.Schema into an Iceberg schema. This is
// the sink side of the canonical type system: sources map into core.Kind,
// sinks map out of it. Field IDs are allocated sequentially through the
// whole tree — a nested struct's fields continue the counter — so they are
// deterministic and stable: adding a field inside a nested struct is additive
// schema evolution that does not invalidate older readers' field IDs.
func FromCanonical(cs core.Schema) (*iceberg.Schema, error) {
	nextID := 1
	fields := make([]iceberg.NestedField, 0, len(cs.Columns))
	for _, col := range cs.Columns {
		nf, err := nestedFieldFromColumn(col, &nextID)
		if err != nil {
			return nil, fmt.Errorf("iceberg: column %q: %w", col.Name, err)
		}
		fields = append(fields, nf)
	}
	return iceberg.NewSchema(0, fields...), nil
}

// nestedFieldFromColumn builds one Iceberg NestedField for a canonical
// column. Its own ID is allocated BEFORE its type recurses, so top-level
// fields take the first ids in order and nested children continue the
// counter — the Iceberg field-ID convention that keeps schema evolution
// additive.
func nestedFieldFromColumn(col core.Column, nextID *int) (iceberg.NestedField, error) {
	id := nextFieldID(nextID)
	itype, err := icebergFromCanonicalType(col.Type, nextID)
	if err != nil {
		return iceberg.NestedField{}, err
	}
	return iceberg.NestedField{
		ID:       id,
		Name:     col.Name,
		Type:     itype,
		Required: !col.Type.Nullable,
	}, nil
}

// icebergFromCanonicalType maps a canonical column type to an Iceberg type.
// Scalars map to primitives; composite kinds recurse, with each nested
// field's own ID allocated before its children.
func icebergFromCanonicalType(t core.ColumnType, nextID *int) (iceberg.Type, error) {
	switch t.Kind {
	case core.KindStruct:
		fields := make([]iceberg.NestedField, 0, len(t.Fields))
		for _, f := range t.Fields {
			nf, err := nestedFieldFromColumn(f, nextID)
			if err != nil {
				return nil, fmt.Errorf("struct field %q: %w", f.Name, err)
			}
			fields = append(fields, nf)
		}
		return &iceberg.StructType{FieldList: fields}, nil
	case core.KindList:
		if t.Elem == nil {
			return nil, fmt.Errorf("list requires an element type")
		}
		elemID := nextFieldID(nextID)
		et, err := icebergFromCanonicalType(*t.Elem, nextID)
		if err != nil {
			return nil, err
		}
		return &iceberg.ListType{
			ElementID:       elemID,
			Element:         et,
			ElementRequired: !t.Elem.Nullable,
		}, nil
	case core.KindMap:
		if t.KeyType == nil || t.ValueType == nil {
			return nil, fmt.Errorf("map requires key and value types")
		}
		keyID := nextFieldID(nextID)
		kt, err := icebergFromCanonicalType(*t.KeyType, nextID)
		if err != nil {
			return nil, err
		}
		valID := nextFieldID(nextID)
		vt, err := icebergFromCanonicalType(*t.ValueType, nextID)
		if err != nil {
			return nil, err
		}
		return &iceberg.MapType{
			KeyID:         keyID,
			KeyType:       kt,
			ValueID:       valID,
			ValueType:     vt,
			ValueRequired: !t.ValueType.Nullable,
		}, nil
	default:
		return mapCanonicalType(t)
	}
}

// nextFieldID hands out the next sequential field ID.
func nextFieldID(nextID *int) int {
	id := *nextID
	*nextID++
	return id
}

// mapCanonicalType maps a canonical scalar kind to an Iceberg primitive.
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
	case core.KindFixedBinary:
		if t.FixedSize <= 0 {
			return nil, fmt.Errorf("fixed requires size > 0")
		}
		return iceberg.FixedTypeOf(t.FixedSize), nil
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

package decoder

import (
	"fmt"

	"github.com/hamba/avro/v2"

	"github.com/maltzsama/urutau/internal/core"
)

// avroSchemaToCanonical maps an Avro schema to a canonical ColumnType. It is
// the schema half of the Avro decoder: records → struct, arrays → list, maps
// → map<string, value>. Logical types arrive typed — decimal, date,
// time/timestamp and uuid map to their canonical kinds instead of degrading
// to a bare number/string, so the int64-over-float64 corruption class has no
// path through Avro. Union [null, T] (the optional-field case) collapses to
// T nullable; a genuine union between concrete types has no canonical
// representation and lands on the escape valve (KindUnknown + opaque), never
// a synthetic struct.
func avroSchemaToCanonical(schema avro.Schema) (core.ColumnType, error) {
	switch s := schema.(type) {
	case *avro.RecordSchema:
		fields := make([]core.Column, 0, len(s.Fields()))
		for _, f := range s.Fields() {
			ft, err := avroSchemaToCanonical(f.Type())
			if err != nil {
				return core.ColumnType{}, fmt.Errorf("field %q: %w", f.Name(), err)
			}
			if u, ok := f.Type().(*avro.UnionSchema); ok && u.Nullable() {
				ft.Nullable = true
			}
			fields = append(fields, core.Column{Name: f.Name(), Type: ft})
		}
		return core.ColumnType{Kind: core.KindStruct, Fields: fields}, nil
	case *avro.ArraySchema:
		et, err := avroSchemaToCanonical(s.Items())
		if err != nil {
			return core.ColumnType{}, err
		}
		return core.ColumnType{Kind: core.KindList, Elem: &et}, nil
	case *avro.MapSchema:
		vt, err := avroSchemaToCanonical(s.Values())
		if err != nil {
			return core.ColumnType{}, err
		}
		return core.ColumnType{Kind: core.KindMap,
			KeyType:   &core.ColumnType{Kind: core.KindString},
			ValueType: &vt}, nil
	case *avro.UnionSchema:
		if s.Nullable() {
			_, typ := s.Indices()
			branches := s.Types()
			inner, err := avroSchemaToCanonical(branches[typ])
			if err != nil {
				return core.ColumnType{}, err
			}
			inner.Nullable = true
			return inner, nil
		}
		// Genuine union between concrete types: no canonical Kind (CR-033).
		return core.ColumnType{Kind: core.KindUnknown, Nullable: true,
			Opaque: &core.OpaqueOrigin{TypeName: "avro-union", VendorName: "avro"}}, nil
	case *avro.FixedSchema:
		if dec, ok := decimalOf(s); ok {
			return core.ColumnType{Kind: core.KindDecimal, Precision: dec.Precision(), Scale: dec.Scale()}, nil
		}
		return core.ColumnType{Kind: core.KindFixedBinary, FixedSize: s.Size()}, nil
	case *avro.EnumSchema:
		return core.ColumnType{Kind: core.KindString}, nil // Avro enums are strings
	case *avro.RefSchema:
		// A named-type reference (record/enum/fixed defined once, referenced
		// elsewhere): resolve to the underlying schema and translate it.
		return avroSchemaToCanonical(s.Schema())
	case *avro.PrimitiveSchema:
		return avroPrimitiveToCanonical(s)
	default:
		return core.ColumnType{}, fmt.Errorf("avro: unsupported schema type %T", schema)
	}
}

// avroPrimitiveToCanonical maps an Avro primitive (with its logical type) to
// a canonical kind.
func avroPrimitiveToCanonical(s *avro.PrimitiveSchema) (core.ColumnType, error) {
	if dec, ok := decimalOf(s); ok {
		return core.ColumnType{Kind: core.KindDecimal, Precision: dec.Precision(), Scale: dec.Scale()}, nil
	}
	if lg := s.Logical(); lg != nil {
		switch lg.Type() {
		case avro.UUID:
			return core.ColumnType{Kind: core.KindUUID}, nil
		case avro.Date:
			return core.ColumnType{Kind: core.KindDate}, nil
		case avro.TimeMillis, avro.TimeMicros:
			return core.ColumnType{Kind: core.KindTime}, nil
		case avro.TimestampMillis, avro.TimestampMicros:
			return core.ColumnType{Kind: core.KindTimestampTZ}, nil // Avro timestamps are UTC
		case avro.LocalTimestampMillis, avro.LocalTimestampMicros:
			return core.ColumnType{Kind: core.KindTimestamp}, nil
		}
	}
	switch s.Type() {
	case avro.Boolean:
		return core.ColumnType{Kind: core.KindBool}, nil
	case avro.Int:
		return core.ColumnType{Kind: core.KindInt32}, nil
	case avro.Long:
		return core.ColumnType{Kind: core.KindInt64}, nil
	case avro.Float:
		return core.ColumnType{Kind: core.KindFloat32}, nil
	case avro.Double:
		return core.ColumnType{Kind: core.KindFloat64}, nil
	case avro.String:
		return core.ColumnType{Kind: core.KindString}, nil
	case avro.Bytes:
		return core.ColumnType{Kind: core.KindBinary}, nil
	case avro.Null:
		return core.ColumnType{Kind: core.KindUnknown, Nullable: true}, nil
	default:
		return core.ColumnType{}, fmt.Errorf("avro: unsupported primitive %q", s.Type())
	}
}

// decimalOf returns the decimal logical schema when the schema carries one
// (avro decimal sits on bytes or fixed).
func decimalOf(s interface{ Logical() avro.LogicalSchema }) (*avro.DecimalLogicalSchema, bool) {
	lg := s.Logical()
	if lg == nil || lg.Type() != avro.Decimal {
		return nil, false
	}
	if d, ok := lg.(*avro.DecimalLogicalSchema); ok {
		return d, true
	}
	return nil, false
}

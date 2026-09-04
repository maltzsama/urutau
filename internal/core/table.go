// Package core holds the canonical, sink-agnostic type system that crosses
// the source↔sink boundary. Sources map their native types into
// core.Kind; sinks map out of it. Neither side knows the other — that is
// what makes N sources × M sinks cost N+M mappings instead of N×M.
package core

import "fmt"

// Kind is the canonical scalar type.
//
// The system is intentionally scalar-only today. Composite types are
// deferred debt: Iceberg supports struct/list/map natively, and the first
// real Avro/Protobuf-with-schema-registry landing will need them (headers
// as a native map is the current workaround — JSON in a string). A
// non-scalar Kind is not a new enum value but a new dimension of the model:
// it touches the cast matrix, the Arrow mapping, Iceberg schema generation,
// the wire format, and schema comparison. Reopen only on a real demand for
// nested typed landing, not on convenience.
type Kind uint8

const (
	KindUnknown Kind = iota
	KindBool
	KindInt32
	KindInt64
	KindFloat32
	KindFloat64
	KindDecimal // uses Precision, Scale
	KindString
	KindBinary
	KindDate        // days since epoch
	KindTime        // micros since midnight
	KindTimestamp   // naive wall clock, NO timezone (MySQL DATETIME)
	KindTimestampTZ // UTC instant (MySQL TIMESTAMP)
	KindUUID
	KindJSON // semantically JSON; physically a string in most sinks
)

// String renders the kind name for errors and diagnostics.
func (k Kind) String() string {
	switch k {
	case KindBool:
		return "bool"
	case KindInt32:
		return "int32"
	case KindInt64:
		return "int64"
	case KindFloat32:
		return "float32"
	case KindFloat64:
		return "float64"
	case KindDecimal:
		return "decimal"
	case KindString:
		return "string"
	case KindBinary:
		return "binary"
	case KindDate:
		return "date"
	case KindTime:
		return "time"
	case KindTimestamp:
		return "timestamp"
	case KindTimestampTZ:
		return "timestamptz"
	case KindUUID:
		return "uuid"
	case KindJSON:
		return "json"
	default:
		return "unknown"
	}
}

// ColumnType is a canonical column type. Precision/Scale are only
// meaningful for KindDecimal.
type ColumnType struct {
	Kind      Kind
	Precision int
	Scale     int
	Nullable  bool
}

func (t ColumnType) String() string {
	if t.Kind == KindDecimal {
		return fmt.Sprintf("decimal(%d,%d)", t.Precision, t.Scale)
	}
	return t.Kind.String()
}

// Column is one named canonical column.
type Column struct {
	Name string
	Type ColumnType
}

// Schema is the canonical table shape — the ONLY schema type that crosses
// the source↔sink boundary.
type Schema struct {
	Columns    []Column
	PrimaryKey []string // column names, in key order
}

// Column returns the column by name, if present.
func (s Schema) Column(name string) (Column, bool) {
	for _, c := range s.Columns {
		if c.Name == name {
			return c, true
		}
	}
	return Column{}, false
}

// KeyIndexes resolves PrimaryKey into column positions, in key order.
func (s Schema) KeyIndexes() ([]int, error) {
	idx := make(map[string]int, len(s.Columns))
	for i, c := range s.Columns {
		idx[c.Name] = i
	}
	out := make([]int, 0, len(s.PrimaryKey))
	for _, name := range s.PrimaryKey {
		i, ok := idx[name]
		if !ok {
			return nil, fmt.Errorf("core: primary key column %q not in schema", name)
		}
		out = append(out, i)
	}
	return out, nil
}

// TableRef identifies one replicated table on both sides. This is the
// pipeline-wide table identity (replaces the per-source TableRef).
type TableRef struct {
	Source     string   // source-side identifier, e.g. "shop.orders"
	Target     string   // sink-side identifier, e.g. "raw.orders"
	PrimaryKey []string // equality key; empty means "derive from source"
}

// ParseColumnType parses a textual canonical type string into a
// ColumnType. The syntax is the same as ParseCastTarget: "string",
// "int64", "decimal(20,4)", etc.
func ParseColumnType(s string) (ColumnType, error) {
	ct, err := ParseCastTarget(s)
	if err != nil {
		return ColumnType{}, err
	}
	return ct.Type, nil
}

// Row is a decoded source row: column name → value, using only the Go types
// that ColumnType.Kind implies.
type Row map[string]any

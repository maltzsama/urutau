// Package core holds the canonical, sink-agnostic type system that crosses
// the source↔sink boundary. Sources map their native types into
// core.Kind; sinks map out of it. Neither side knows the other — that is
// what makes N sources × M sinks cost N+M mappings instead of N×M.
package core

import "fmt"

// Kind is the canonical type.
//
// The admission criterion is deliberate: a value enters the Kind set only if
// a supported source produces it natively AND a supported sink has a real
// representation for it. Physical encodings of the same logical type
// (dictionary, *_view, large_*) never become distinct Kinds; polymorphic
// types with no source or destination (union, extension) never enter; and
// the escape valve (KindUnknown + an explicit cast) is the deliberate
// alternative to accepting anything silently. Composite types
// (struct/list/map) are a separate dimension of the model — they touch the
// cast matrix, the Arrow mapping, Iceberg schema generation, the wire format
// and schema comparison — and land as a dedicated effort, not an enum
// addition.
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
	KindFixedBinary // Iceberg fixed(L) — fixed-size byte sequence, distinct from variable binary
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
	case KindFixedBinary:
		return "fixed"
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
// meaningful for KindDecimal; FixedSize only for KindFixedBinary; Opaque
// only when Kind is KindUnknown.
type ColumnType struct {
	Kind      Kind
	Precision int
	Scale     int
	FixedSize int
	Nullable  bool
	// Opaque records the provenance of an unmappable source type: what it
	// was and where it came from, so the validation error and any operator
	// looking at it know what the escape valve is carrying. Non-nil only
	// when Kind == KindUnknown.
	Opaque *OpaqueOrigin
}

// OpaqueOrigin names an unmappable source type and its vendor. It is the
// provenance half of the escape valve: KindUnknown says "this has no
// canonical form and needs an explicit cast", Opaque says what it was
// ("geometry" from "postgres", "point" from "mysql") instead of discarding
// that knowledge at the mapping boundary.
type OpaqueOrigin struct {
	TypeName   string
	VendorName string
}

func (o *OpaqueOrigin) String() string {
	if o == nil {
		return ""
	}
	if o.VendorName != "" {
		return o.VendorName + " " + o.TypeName
	}
	return o.TypeName
}

func (t ColumnType) String() string {
	switch t.Kind {
	case KindDecimal:
		return fmt.Sprintf("decimal(%d,%d)", t.Precision, t.Scale)
	case KindFixedBinary:
		return fmt.Sprintf("fixed(%d)", t.FixedSize)
	default:
		return t.Kind.String()
	}
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

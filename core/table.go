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

// Kind values are explicit (not iota) because Kind crosses the wire in the
// Arrow extension metadata and in serialized schemas: inserting a value must
// never silently renumber the existing ones.
const (
	KindUnknown Kind = 0
	KindBool    Kind = 1
	KindInt32   Kind = 2
	KindInt64   Kind = 3
	// KindUInt64 is an unsigned 64-bit integer; sources that natively emit
	// uint64 keep their exact width instead of silently widening to int64.
	KindUInt64  Kind = 4
	KindFloat32 Kind = 5
	KindFloat64 Kind = 6
	// KindDecimal uses Precision and Scale.
	KindDecimal Kind = 7
	KindString  Kind = 8
	KindBinary  Kind = 9
	// KindFixedBinary is Iceberg fixed(L) — a fixed-size byte sequence,
	// distinct from variable binary.
	KindFixedBinary Kind = 10
	// KindDate is days since epoch.
	KindDate Kind = 11
	// KindTime is micros since midnight.
	KindTime Kind = 12
	// KindTimestamp is a naive wall clock, NO timezone (MySQL DATETIME).
	KindTimestamp Kind = 13
	// KindTimestampTZ is a UTC instant (MySQL TIMESTAMP).
	KindTimestampTZ Kind = 14
	KindUUID        Kind = 15
	// KindJSON is semantically JSON; physically a string in most sinks.
	KindJSON Kind = 16

	// Composite kinds — a separate dimension of the model, entered only
	// because both sides of the boundary have a real representation for
	// them: message-log sources (Avro/Protobuf via schema registry) produce
	// nested values and Iceberg holds struct/list/map natively.
	KindStruct Kind = 17
	KindList   Kind = 18
	KindMap    Kind = 19
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
	case KindUInt64:
		return "uint64"
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
	case KindStruct:
		return "struct"
	case KindList:
		return "list"
	case KindMap:
		return "map"
	default:
		return "unknown"
	}
}

// ColumnType is a canonical column type. Precision/Scale are only
// meaningful for KindDecimal; FixedSize only for KindFixedBinary; Opaque
// only when Kind is KindUnknown; the composite fields only when Kind is
// KindStruct (Fields), KindList (Elem) or KindMap (KeyType/ValueType). The
// composite fields are pointers so a Struct can nest a Struct, or a List
// hold a Struct, without a parallel type system.
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

	// Fields holds the ordered fields of a KindStruct column.
	Fields []Column
	// Elem is the element type of a KindList column.
	Elem *ColumnType
	// KeyType and ValueType are the key/value types of a KindMap column.
	// Message-log map keys are almost always strings.
	KeyType   *ColumnType
	ValueType *ColumnType
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
	case KindUnknown:
		// Carry the provenance into diagnostics instead of dropping it.
		if t.Opaque != nil {
			return "unknown (" + t.Opaque.String() + ")"
		}
		return "unknown"
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

// Validate reports whether the schema is internally coherent: every column
// is named, names are unique, and the primary key names existing columns
// without repetition. An empty schema is valid.
func (s Schema) Validate() error {
	seen := make(map[string]bool, len(s.Columns))
	for _, c := range s.Columns {
		if c.Name == "" {
			return fmt.Errorf("core: schema has a column with an empty name")
		}
		if seen[c.Name] {
			return fmt.Errorf("core: schema has duplicate column %q", c.Name)
		}
		seen[c.Name] = true
	}
	key := make(map[string]bool, len(s.PrimaryKey))
	for _, name := range s.PrimaryKey {
		if !seen[name] {
			return fmt.Errorf("core: primary key column %q not in schema", name)
		}
		if key[name] {
			return fmt.Errorf("core: primary key lists %q twice", name)
		}
		key[name] = true
	}
	return nil
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

// Row is a decoded source row: column name → value. The Go type per Kind is
// the canonical contract both cast kernels (core.Convert and the columnar
// encoder) rely on:
//
//	KindBool        bool
//	KindInt32       int32
//	KindInt64       int64
//	KindUInt64      uint64
//	KindFloat32     float32
//	KindFloat64     float64
//	KindDecimal     string (decimal text)
//	KindString      string
//	KindBinary      []byte
//	KindFixedBinary []byte
//	KindDate        time.Time, int32 days, or "2006-01-02" text
//	KindTime        time.Time, int64 micros, or "15:04:05" text
//	KindTimestamp   time.Time or naive timestamp text
//	KindTimestampTZ time.Time or RFC3339 text
//	KindUUID        string or 16 raw bytes
//	KindJSON        string, []byte, or a Go composite (map/slice)
//
// A Kind may arrive in more than one representation — the DBLog snapshot and
// the live stream decode differently — so both kernels accept the union.
type Row map[string]any

package mysql

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/schema"

	"github.com/maltzsama/urutau/core"
)

// columnFromIntrospect builds a column the way queryColumns does: the
// information_schema data_type drives the classification, the column_type
// carries the modifiers (unsigned, size, member lists). These tests exercise
// that derivation directly, because the database round trip is what the
// package has no fixture for — and its absence is why #180 went unnoticed.
func columnFromIntrospect(t *testing.T, name, dataType, columnType, collation string, precision, scale int) schema.TableColumn {
	t.Helper()
	return buildColumn(name, dataType, columnType, collation, precision, scale)
}

// #180: the unsigned safety valve in mapColumnType is unreachable unless
// queryColumns derives IsUnsigned from column_type. Before the fix this
// declared KindInt64 and values above 2^63 wrapped negative.
func TestIntrospectUnsignedReachesSafetyValve(t *testing.T) {
	for _, tc := range []struct{ dataType, columnType string }{
		{"bigint", "bigint unsigned"},
		{"int", "int unsigned"},
		{"smallint", "smallint unsigned"},
		{"tinyint", "tinyint unsigned"},
		{"mediumint", "mediumint unsigned"},
		{"int", "int(10) unsigned zerofill"},
	} {
		col := columnFromIntrospect(t, "n", tc.dataType, tc.columnType, "", 0, 0)
		if !col.IsUnsigned {
			t.Errorf("%s: IsUnsigned = false, want true", tc.columnType)
		}
		ct, err := mapColumnType(col)
		if err != nil {
			t.Fatalf("%s: %v", tc.columnType, err)
		}
		if ct.Kind != core.KindUnknown {
			t.Errorf("%s: Kind = %v, want KindUnknown (declared cast required)", tc.columnType, ct.Kind)
		}
	}
}

// A signed integer must NOT take the unsigned branch — the valve has to stay
// narrow or every int needs a cast.
func TestIntrospectSignedStaysInt64(t *testing.T) {
	for _, tc := range []struct{ dataType, columnType string }{
		{"bigint", "bigint"},
		{"int", "int(11)"},
		{"mediumint", "mediumint"},
		{"year", "year(4)"},
	} {
		col := columnFromIntrospect(t, "n", tc.dataType, tc.columnType, "", 0, 0)
		if col.IsUnsigned {
			t.Errorf("%s: IsUnsigned = true, want false", tc.columnType)
		}
		ct, err := mapColumnType(col)
		if err != nil {
			t.Fatalf("%s: %v", tc.columnType, err)
		}
		if ct.Kind != core.KindInt64 {
			t.Errorf("%s: Kind = %v, want KindInt64", tc.columnType, ct.Kind)
		}
	}
}

// #180: FixedSize must come from column_type, or binary(16) degrades to
// KindBinary. schema_test.go hand-built FixedSize and so asserted a shape
// production never produced.
func TestIntrospectFixedSize(t *testing.T) {
	col := columnFromIntrospect(t, "digest", "binary", "binary(16)", "", 0, 0)
	if col.FixedSize != 16 {
		t.Fatalf("FixedSize = %d, want 16", col.FixedSize)
	}
	ct, err := mapColumnType(col)
	if err != nil {
		t.Fatalf("mapColumnType: %v", err)
	}
	if ct.Kind != core.KindFixedBinary || ct.FixedSize != 16 {
		t.Errorf("binary(16) = %v/%d, want KindFixedBinary/16", ct.Kind, ct.FixedSize)
	}

	// VARBINARY is variable: FixedSize stays 0 and the kind is KindBinary.
	vb := columnFromIntrospect(t, "blob", "varbinary", "varbinary(255)", "", 0, 0)
	if vb.FixedSize != 0 {
		t.Errorf("varbinary FixedSize = %d, want 0", vb.FixedSize)
	}
	vct, err := mapColumnType(vb)
	if err != nil {
		t.Fatalf("mapColumnType: %v", err)
	}
	if vct.Kind != core.KindBinary {
		t.Errorf("varbinary(255) = %v, want KindBinary", vct.Kind)
	}
}

// #180: Collation must survive introspection — decodeString keys the charset
// decoder on it, so an empty collation silently falls through to passthrough.
func TestIntrospectCollation(t *testing.T) {
	col := columnFromIntrospect(t, "name", "varchar", "varchar(64)", "latin1_swedish_ci", 0, 0)
	if col.Collation != "latin1_swedish_ci" {
		t.Errorf("Collation = %q, want latin1_swedish_ci", col.Collation)
	}
}

// ENUM and SET member lists come from column_type. decodeEnum/decodeSet
// (reader.go) read EnumValues/SetValues and fall back to the raw ordinal or
// bitmask when they are empty — so without this the snapshot and the CDC
// disagree on what an ENUM column holds.
func TestIntrospectEnumSetMembers(t *testing.T) {
	e := columnFromIntrospect(t, "status", "enum", "enum('a','b','c')", "utf8mb4_general_ci", 0, 0)
	if got, want := e.EnumValues, []string{"a", "b", "c"}; !equalStrings(got, want) {
		t.Errorf("EnumValues = %v, want %v", got, want)
	}
	s := columnFromIntrospect(t, "flags", "set", "set('x','y','z')", "utf8mb4_general_ci", 0, 0)
	if got, want := s.SetValues, []string{"x", "y", "z"}; !equalStrings(got, want) {
		t.Errorf("SetValues = %v, want %v", got, want)
	}

	// The decoders must now resolve a member rather than echoing the ordinal.
	if got := decodeEnum(e, int64(2)); got != "b" {
		t.Errorf("decodeEnum(2) = %v, want b", got)
	}
	if got := decodeSet(s, int64(0b101)); got != "x,z" {
		t.Errorf("decodeSet(0b101) = %v, want x,z", got)
	}
}

// DECIMAL precision/scale must still resolve after moving off EnumValues,
// which now carries real ENUM members.
func TestIntrospectDecimalPrecisionScale(t *testing.T) {
	col := columnFromIntrospect(t, "amount", "decimal", "decimal(20,4)", "", 20, 4)
	ct, err := mapColumnType(col)
	if err != nil {
		t.Fatalf("mapColumnType: %v", err)
	}
	if ct.Kind != core.KindDecimal || ct.Precision != 20 || ct.Scale != 4 {
		t.Errorf("decimal(20,4) = %v/%d/%d, want KindDecimal/20/4", ct.Kind, ct.Precision, ct.Scale)
	}
	if len(col.EnumValues) != 0 {
		t.Errorf("EnumValues = %v, want empty (decimal must not overload it)", col.EnumValues)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

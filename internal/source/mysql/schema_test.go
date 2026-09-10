package mysql

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/schema"

	"github.com/maltzsama/urutau/core"
)

func TestMapColumnTypeBinaryFixedVsVariable(t *testing.T) {
	// BINARY(16): fixed-size byte sequence keeps its declared length.
	col := schema.TableColumn{Type: schema.TYPE_BINARY, RawType: "binary(16)", FixedSize: 16}
	ct, err := mapColumnType(col)
	if err != nil {
		t.Fatal(err)
	}
	if ct.Kind != core.KindFixedBinary || ct.FixedSize != 16 {
		t.Fatalf("binary(16) mapped to %+v, want fixed(16)", ct)
	}

	// VARBINARY(255): variable binary, FixedSize stays 0 in go-mysql.
	col = schema.TableColumn{Type: schema.TYPE_BINARY, RawType: "varbinary(255)", FixedSize: 0}
	ct, err = mapColumnType(col)
	if err != nil {
		t.Fatal(err)
	}
	if ct.Kind != core.KindBinary {
		t.Fatalf("varbinary mapped to %+v, want KindBinary", ct)
	}
}

func TestMapColumnTypeOpaqueProvenance(t *testing.T) {
	// An unmappable type carries its provenance so the error can name it.
	col := schema.TableColumn{Type: schema.TYPE_POINT, RawType: "point"}
	ct, err := mapColumnType(col)
	if err != nil {
		t.Fatal(err)
	}
	if ct.Kind != core.KindUnknown || ct.Opaque == nil {
		t.Fatalf("point mapped to %+v, want KindUnknown with provenance", ct)
	}
	if ct.Opaque.TypeName != "point" || ct.Opaque.VendorName != "mysql" {
		t.Fatalf("opaque = %+v, want point/mysql", ct.Opaque)
	}
}

// fixed binary survives the full canonical derivation.
func TestCanonicalSchemaFixedBinary(t *testing.T) {
	tbl := &schema.Table{
		Columns: []schema.TableColumn{
			{Name: "id", Type: schema.TYPE_NUMBER},
			{Name: "digest", Type: schema.TYPE_BINARY, RawType: "binary(16)", FixedSize: 16},
		},
	}
	cs, err := CanonicalSchema(tbl)
	if err != nil {
		t.Fatalf("canonical: %v", err)
	}
	digest, ok := cs.Column("digest")
	if !ok || digest.Type.Kind != core.KindFixedBinary || digest.Type.FixedSize != 16 {
		t.Fatalf("digest = %+v, want fixed(16)", digest)
	}
}

// A present but unparseable decimal precision/scale is an error, not a silent
// 0 (which would look like a legitimate default and hide broken introspection).
func TestMapColumnTypeDecimalBadPrecisionErrors(t *testing.T) {
	col := schema.TableColumn{Type: schema.TYPE_DECIMAL, RawType: "decimal", EnumValues: []string{"garbage"}}
	if _, err := mapColumnType(col); err == nil {
		t.Fatal("unparseable decimal precision/scale must error")
	}
	// A valid "precision,scale" parses.
	col = schema.TableColumn{Type: schema.TYPE_DECIMAL, RawType: "decimal(10,2)", EnumValues: []string{"10,2"}}
	ct, err := mapColumnType(col)
	if err != nil || ct.Precision != 10 || ct.Scale != 2 {
		t.Fatalf("decimal(10,2) = %+v err=%v", ct, err)
	}
}

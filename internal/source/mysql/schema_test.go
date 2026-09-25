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
	cs, err := CanonicalSchema(&Table{Table: tbl})
	if err != nil {
		t.Fatalf("canonical: %v", err)
	}
	digest, ok := cs.Column("digest")
	if !ok || digest.Type.Kind != core.KindFixedBinary || digest.Type.FixedSize != 16 {
		t.Fatalf("digest = %+v, want fixed(16)", digest)
	}
}

// Decimal precision/scale ride MaxSize/FixedSize since #180 — EnumValues now
// carries the real ENUM member list. The old "precision,scale" string and its
// parse error are gone; buildColumn parses the spec, so mapColumnType only
// ever sees numbers.
func TestMapColumnTypeDecimalPrecisionScale(t *testing.T) {
	col := schema.TableColumn{Type: schema.TYPE_DECIMAL, RawType: "decimal(10,2)", MaxSize: 10, FixedSize: 2}
	ct, err := mapColumnType(col)
	if err != nil || ct.Kind != core.KindDecimal || ct.Precision != 10 || ct.Scale != 2 {
		t.Fatalf("decimal(10,2) = %+v err=%v", ct, err)
	}

	// Unsized: the canal runtime path, where go-mysql's AddColumn does not
	// size a decimal column. 0/0 lets the sink infer its own defaults rather
	// than this layer inventing MySQL's 10,0.
	bare := schema.TableColumn{Type: schema.TYPE_DECIMAL, RawType: "decimal"}
	ct, err = mapColumnType(bare)
	if err != nil || ct.Kind != core.KindDecimal || ct.Precision != 0 || ct.Scale != 0 {
		t.Fatalf("bare decimal = %+v err=%v", ct, err)
	}

	// A malformed column_type cannot produce a bogus precision: buildColumn
	// leaves 0 rather than guessing, so nothing downstream sees a spec that
	// the source never actually read.
	garbage := buildColumn("amount", "decimal", "decimal(garbage)", "", 0, 0)
	if garbage.MaxSize != 0 || garbage.FixedSize != 0 {
		t.Fatalf("garbage decimal spec = %d,%d, want 0,0", garbage.MaxSize, garbage.FixedSize)
	}
}

package decoder

import (
	"testing"

	"github.com/hamba/avro/v2"

	"github.com/maltzsama/urutau/internal/core"
)

func mustParse(t *testing.T, json string) avro.Schema {
	t.Helper()
	s, err := avro.Parse(json)
	if err != nil {
		t.Fatalf("parse avro: %v", err)
	}
	return s
}

// A record with a nested struct, a list of structs, a map, optional and
// logical fields maps to the canonical composite tree.
func TestAvroSchemaToCanonicalComposite(t *testing.T) {
	s := mustParse(t, `{
		"type":"record","name":"order","fields":[
			{"name":"id","type":"long"},
			{"name":"cust","type":{"type":"record","name":"cust","fields":[
				{"name":"name","type":"string"},
				{"name":"age","type":"long"}
			]}},
			{"name":"lineItems","type":{"type":"array","items":{"type":"record","name":"item","fields":[
				{"name":"sku","type":"string"}
			]}}},
			{"name":"note","type":["null","string"]},
			{"name":"attrs","type":{"type":"map","values":"long"}}
		]
	}`)
	ct, err := avroSchemaToCanonical(s)
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if ct.Kind != core.KindStruct || len(ct.Fields) != 5 {
		t.Fatalf("root = %v, want struct with 5 fields", ct.Kind)
	}
	cust := ct.Fields[1].Type
	if cust.Kind != core.KindStruct || len(cust.Fields) != 2 {
		t.Fatalf("cust = %v", cust.Kind)
	}
	items := ct.Fields[2].Type
	if items.Kind != core.KindList || items.Elem.Kind != core.KindStruct {
		t.Fatalf("lineItems = %v, want list of struct", items.Kind)
	}
	// [null,string] collapses to nullable string.
	note := ct.Fields[3].Type
	if note.Kind != core.KindString || !note.Nullable {
		t.Fatalf("note = %v, want nullable string", note.Kind)
	}
	attrs := ct.Fields[4].Type
	if attrs.Kind != core.KindMap || attrs.KeyType.Kind != core.KindString || attrs.ValueType.Kind != core.KindInt64 {
		t.Fatalf("attrs = %v, want map<string,long>", attrs.Kind)
	}
}

// Logical types arrive typed, not as bare numbers/strings.
func TestAvroSchemaLogicalTypes(t *testing.T) {
	s := mustParse(t, `{
		"type":"record","name":"t","fields":[
			{"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}},
			{"name":"born","type":{"type":"long","logicalType":"timestamp-millis"}},
			{"name":"day","type":{"type":"int","logicalType":"date"}},
			{"name":"uid","type":{"type":"string","logicalType":"uuid"}},
			{"name":"digest","type":{"type":"fixed","name":"d","size":16}}
		]
	}`)
	ct, err := avroSchemaToCanonical(s)
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	dec := ct.Fields[0].Type
	if dec.Kind != core.KindDecimal || dec.Precision != 10 || dec.Scale != 2 {
		t.Fatalf("amount = %+v, want decimal(10,2)", dec)
	}
	born := ct.Fields[1].Type
	if born.Kind != core.KindTimestampTZ {
		t.Fatalf("born = %v, want timestamptz", born.Kind)
	}
	day := ct.Fields[2].Type
	if day.Kind != core.KindDate {
		t.Fatalf("day = %v, want date", day.Kind)
	}
	uid := ct.Fields[3].Type
	if uid.Kind != core.KindUUID {
		t.Fatalf("uid = %v, want uuid", uid.Kind)
	}
	digest := ct.Fields[4].Type
	if digest.Kind != core.KindFixedBinary || digest.FixedSize != 16 {
		t.Fatalf("digest = %+v, want fixed(16)", digest)
	}
}

// A genuine union between concrete types falls to the escape valve with
// provenance — never a synthetic struct.
func TestAvroSchemaGenuineUnionEscapeValve(t *testing.T) {
	s := mustParse(t, `["int","string"]`)
	ct, err := avroSchemaToCanonical(s)
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if ct.Kind != core.KindUnknown || ct.Opaque == nil || ct.Opaque.TypeName != "avro-union" {
		t.Fatalf("union = %+v, want KindUnknown with avro-union provenance", ct)
	}
}

package spec

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/maltzsama/urutau/core"
	"gopkg.in/yaml.v3"
)

// decodeColumnDecl round-trips text through the same YAML→JSON path
// LoadYAML uses, so these tests exercise the real wire contract.
func decodeColumnDecl(t *testing.T, text string) ColumnDecl {
	t.Helper()
	var raw any
	if err := yaml.Unmarshal([]byte(text), &raw); err != nil {
		t.Fatalf("yaml: %v", err)
	}
	b, err := json.Marshal(raw)
	if err != nil {
		t.Fatalf("json marshal: %v", err)
	}
	var d ColumnDecl
	if err := json.Unmarshal(b, &d); err != nil {
		t.Fatalf("json unmarshal: %v", err)
	}
	return d
}

func TestColumnDeclScalar(t *testing.T) {
	d := decodeColumnDecl(t, `int64`)
	if d.Scalar != "int64" || d.Struct != nil || d.List != nil || d.Map != nil {
		t.Fatalf("scalar decl = %+v", d)
	}
	ct, err := d.Resolve()
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if ct.Kind != core.KindInt64 {
		t.Fatalf("resolved kind = %v, want int64", ct.Kind)
	}
}

func TestColumnDeclStruct(t *testing.T) {
	d := decodeColumnDecl(t, `
struct:
  street: string
  zip: int64
`)
	ct, err := d.Resolve()
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if ct.Kind != core.KindStruct {
		t.Fatalf("kind = %v, want struct", ct.Kind)
	}
	if len(ct.Fields) != 2 {
		t.Fatalf("fields = %d, want 2", len(ct.Fields))
	}
	// Resolve sorts fields by name for a deterministic schema.
	if ct.Fields[0].Name != "street" || ct.Fields[0].Type.Kind != core.KindString {
		t.Fatalf("field 0 = %+v", ct.Fields[0])
	}
	if ct.Fields[1].Name != "zip" || ct.Fields[1].Type.Kind != core.KindInt64 {
		t.Fatalf("field 1 = %+v", ct.Fields[1])
	}
}

func TestColumnDeclList(t *testing.T) {
	d := decodeColumnDecl(t, `list: string`)
	ct, err := d.Resolve()
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if ct.Kind != core.KindList {
		t.Fatalf("kind = %v, want list", ct.Kind)
	}
	if ct.Elem == nil || ct.Elem.Kind != core.KindString {
		t.Fatalf("elem = %+v", ct.Elem)
	}
}

func TestColumnDeclMap(t *testing.T) {
	d := decodeColumnDecl(t, `
map:
  key: string
  value: int64
`)
	ct, err := d.Resolve()
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if ct.Kind != core.KindMap {
		t.Fatalf("kind = %v, want map", ct.Kind)
	}
	if ct.KeyType == nil || ct.KeyType.Kind != core.KindString {
		t.Fatalf("key = %+v", ct.KeyType)
	}
	if ct.ValueType == nil || ct.ValueType.Kind != core.KindInt64 {
		t.Fatalf("value = %+v", ct.ValueType)
	}
}

func TestColumnDeclNestedDepth(t *testing.T) {
	// list<struct<ts:timestamp, note:string>>
	d := decodeColumnDecl(t, `
list:
  struct:
    ts: timestamp
    note: string
`)
	ct, err := d.Resolve()
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if ct.Kind != core.KindList || ct.Elem.Kind != core.KindStruct {
		t.Fatalf("resolved = %+v", ct)
	}
	if len(ct.Elem.Fields) != 2 {
		t.Fatalf("nested fields = %d, want 2", len(ct.Elem.Fields))
	}
}

func TestColumnDeclRejectsMultipleKeys(t *testing.T) {
	var d ColumnDecl
	err := json.Unmarshal([]byte(`{"struct":{"a":"string"},"list":"string"}`), &d)
	if err == nil {
		t.Fatal("want error: struct and list both set")
	}
}

func TestColumnDeclRejectsEmptyObject(t *testing.T) {
	var d ColumnDecl
	err := json.Unmarshal([]byte(`{}`), &d)
	if err == nil {
		t.Fatal("want error: empty object is neither scalar nor composite")
	}
}

func TestColumnDeclRejectsUnknownKeyGrammar(t *testing.T) {
	var d ColumnDecl
	err := json.Unmarshal([]byte(`{"tuple":"string"}`), &d)
	if err == nil {
		t.Fatal("want error: unknown composite key")
	}
}

func TestColumnDeclRejectsBadScalarType(t *testing.T) {
	d := decodeColumnDecl(t, `not-a-real-type`)
	if _, err := d.Resolve(); err == nil {
		t.Fatal("want error: unknown scalar type")
	}
}

func TestColumnDeclMarshalRoundTrip(t *testing.T) {
	scalar := ColumnDecl{Scalar: "int64"}
	b, err := json.Marshal(scalar)
	if err != nil {
		t.Fatalf("marshal scalar: %v", err)
	}
	if string(b) != `"int64"` {
		t.Fatalf("scalar json = %s", b)
	}

	nested := ColumnDecl{Struct: map[string]ColumnDecl{"a": {Scalar: "string"}}}
	b, err = json.Marshal(nested)
	if err != nil {
		t.Fatalf("marshal struct: %v", err)
	}
	var back ColumnDecl
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.Struct == nil || back.Struct["a"].Scalar != "string" {
		t.Fatalf("round-trip = %+v", back)
	}
}

func TestValidateColumnsGrammar(t *testing.T) {
	s := validSpec()
	s.Source.Kind = "kafka"
	s.Tables[0].WriteMode = WriteModeAppend
	s.Tables[0].PrimaryKey = nil
	s.Tables[0].OnDelete = OnDeleteSkip
	s.Tables[0].Columns = map[string]ColumnDecl{
		"address": {Struct: map[string]ColumnDecl{"city": {Scalar: "string"}}},
	}
	if err := s.Validate(); err != nil {
		t.Fatalf("valid nested column must validate: %v", err)
	}

	s.Tables[0].Columns = map[string]ColumnDecl{
		"address": {Struct: map[string]ColumnDecl{"city": {Scalar: "not-a-type"}}},
	}
	if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "columns") {
		t.Fatalf("want columns grammar problem, got %v", err)
	}
}

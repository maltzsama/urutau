package decoder

import (
	"errors"
	"testing"
)

func TestExtractSimpleFields(t *testing.T) {
	doc := map[string]any{"id": 1.0, "customer_id": 9.0}
	out, err := Extract(doc, []Field{{Name: "id"}, {Name: "customer_id"}}, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if out["id"] != 1.0 || out["customer_id"] != 9.0 {
		t.Fatalf("Extract = %v", out)
	}
}

func TestExtractNestedPath(t *testing.T) {
	doc := map[string]any{"totals": map[string]any{"grand_total": 42.5}}
	out, err := Extract(doc, []Field{{Name: "order_total", Path: "totals.grand_total"}}, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if out["order_total"] != 42.5 {
		t.Fatalf("order_total = %v, want 42.5", out["order_total"])
	}
}

func TestExtractMissingOptionalFieldIsNilAndCounted(t *testing.T) {
	doc := map[string]any{"id": 1.0}
	var missed []string
	out, err := Extract(doc, []Field{{Name: "id"}, {Name: "coupon_code"}}, func(col string) {
		missed = append(missed, col)
	})
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if v, ok := out["coupon_code"]; !ok || v != nil {
		t.Errorf("coupon_code = %v, want present and nil", v)
	}
	if len(missed) != 1 || missed[0] != "coupon_code" {
		t.Errorf("missed = %v, want [coupon_code]", missed)
	}
}

func TestExtractMissingRequiredFieldFails(t *testing.T) {
	doc := map[string]any{}
	_, err := Extract(doc, []Field{{Name: "id", Required: true}}, nil)
	if err == nil {
		t.Fatal("required field missing must fail")
	}
	var missing *ErrFieldMissing
	if !errors.As(err, &missing) || missing.Column != "id" {
		t.Errorf("err = %v, want *ErrFieldMissing for column id", err)
	}
}

// A path that runs into a scalar before it ends is absent, the same as a
// field that is not there at all — the producer changed shape either way.
func TestExtractPathThroughNonDocumentIsAbsent(t *testing.T) {
	doc := map[string]any{"totals": "not-a-document"}
	out, err := Extract(doc, []Field{{Name: "order_total", Path: "totals.grand_total"}}, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if v, ok := out["order_total"]; !ok || v != nil {
		t.Errorf("order_total = %v, want present and nil", v)
	}
}

// Every declared column lands in the result, present or not, so a batch
// always encodes against the same shape.
func TestExtractAlwaysReturnsEveryDeclaredColumn(t *testing.T) {
	doc := map[string]any{}
	out, err := Extract(doc, []Field{{Name: "a"}, {Name: "b"}}, nil)
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("out = %v, want 2 keys", out)
	}
}

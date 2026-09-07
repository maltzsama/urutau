package contract_test

import (
	"encoding/json"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/maltzsama/urutau/internal/plugin/contract"
)

func TestCDCRecordSchema(t *testing.T) {
	tableFields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "qty", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}
	schema := contract.CDCRecordSchema(tableFields)

	if schema.NumFields() != 5 {
		t.Fatalf("expected 5 fields, got %d", schema.NumFields())
	}

	// Check field names
	expectedNames := []string{"op", "before", "after", "offset", "ts_source"}
	for i, want := range expectedNames {
		if got := schema.Field(i).Name; got != want {
			t.Errorf("field %d: want name %q, got %q", i, want, got)
		}
	}

	// Check op is string
	if schema.Field(0).Type.ID() != arrow.STRING {
		t.Errorf("op field: want STRING type, got %v", schema.Field(0).Type)
	}

	// Check offset is binary
	if schema.Field(3).Type.ID() != arrow.BINARY {
		t.Errorf("offset field: want BINARY type, got %v", schema.Field(3).Type)
	}

	// Check before/after are structs with 2 fields
	beforeType, ok := schema.Field(1).Type.(*arrow.StructType)
	if !ok {
		t.Fatalf("before field: want StructType, got %T", schema.Field(1).Type)
	}
	if beforeType.NumFields() != 2 {
		t.Errorf("before struct: want 2 fields, got %d", beforeType.NumFields())
	}
}

func TestValidateOp(t *testing.T) {
	valid := []string{"c", "u", "d"}
	for _, op := range valid {
		if err := contract.ValidateOp(op); err != nil {
			t.Errorf("ValidateOp(%q) unexpected error: %v", op, err)
		}
	}

	invalid := []string{"x", "", "C", "U", "D", "insert", "update", "delete"}
	for _, op := range invalid {
		if err := contract.ValidateOp(op); err == nil {
			t.Errorf("ValidateOp(%q) expected error, got nil", op)
		}
	}
}

func TestGetFlightInfoRequestJSON(t *testing.T) {
	snapshot := `{"table":"orders","mode":"snapshot"}`
	var req contract.GetFlightInfoRequest
	if err := json.Unmarshal([]byte(snapshot), &req); err != nil {
		t.Fatal(err)
	}
	if req.Table != "orders" || req.Mode != "snapshot" || req.FromOffset != "" {
		t.Errorf("unexpected: %+v", req)
	}

	changes := `{"table":"orders","mode":"changes","fromOffset":"YmluLWFsZ28="}`
	if err := json.Unmarshal([]byte(changes), &req); err != nil {
		t.Fatal(err)
	}
	if req.FromOffset != "YmluLWFsZ28=" {
		t.Errorf("unexpected fromOffset: %s", req.FromOffset)
	}
}

func TestStatusResponseJSON(t *testing.T) {
	lag := int64(42)
	resp := contract.StatusResponse{
		State:     "streaming",
		UptimeSec: 184,
		Tables: map[string]contract.TableStatus{
			"orders": {Offset: "Ymlu...", LagEvents: &lag},
		},
	}
	b, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	var roundtrip contract.StatusResponse
	if err := json.Unmarshal(b, &roundtrip); err != nil {
		t.Fatal(err)
	}
	if roundtrip.State != "streaming" || roundtrip.UptimeSec != 184 {
		t.Errorf("unexpected: %+v", roundtrip)
	}
	if *roundtrip.Tables["orders"].LagEvents != 42 {
		t.Errorf("lagEvents mismatch")
	}
}

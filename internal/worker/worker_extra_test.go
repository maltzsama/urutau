package worker

import (
	"testing"

	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

func TestEnrichSpecsEmpty(t *testing.T) {
	got := enrichSpecs(nil)
	if len(got) != 0 {
		t.Errorf("enrichSpecs(nil) = %v, want empty", got)
	}
}

func TestEnrichSpecsMapping(t *testing.T) {
	refs := []*pb.EnrichRef{
		{
			Table:       "orders",
			SourceUri:   "http://enrich:8080",
			SourceQuery: "SELECT id, name FROM customers",
			On:          map[string]string{"customer_id": "id"},
			Select:      []string{"name"},
			As:          map[string]string{"name": "customer_name"},
			JoinType:    "left",
			Refresh:     "5m",
			OnColdStart: "buffer",
		},
	}
	got := enrichSpecs(refs)
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	e := got[0]
	if e.Table != "orders" {
		t.Errorf("Table = %q", e.Table)
	}
	if e.Source.URI != "http://enrich:8080" {
		t.Errorf("Source.URI = %q", e.Source.URI)
	}
	if e.On["customer_id"] != "id" {
		t.Errorf("On = %v", e.On)
	}
	if len(e.Select) != 1 || e.Select[0] != "name" {
		t.Errorf("Select = %v", e.Select)
	}
	if e.As["name"] != "customer_name" {
		t.Errorf("As = %v", e.As)
	}
	if e.JoinType != "left" {
		t.Errorf("JoinType = %q", e.JoinType)
	}
	if e.Refresh != "5m" {
		t.Errorf("Refresh = %q", e.Refresh)
	}
	if e.OnColdStart != "buffer" {
		t.Errorf("OnColdStart = %q", e.OnColdStart)
	}
}

func TestDecodeCastPolicyNil(t *testing.T) {
	ta := &pb.TableAssignment{CastPolicy: nil}
	got, err := decodeCastPolicy(ta)
	if err != nil {
		t.Fatalf("decodeCastPolicy(nil policy): %v", err)
	}
	if len(got.Columns) != 0 {
		t.Errorf("empty CastPolicy.Columns = %v, want empty", got.Columns)
	}
}

func TestDecodeCastPolicyWithData(t *testing.T) {
	ta := &pb.TableAssignment{
		CastPolicy: []byte(`{"Columns":{"amount":{"Type":{"Kind":5},"AssumeUTC":false}}}`),
	}
	got, err := decodeCastPolicy(ta)
	if err != nil {
		t.Fatalf("decodeCastPolicy: %v", err)
	}
	if len(got.Columns) != 1 {
		t.Fatalf("Columns len = %d, want 1", len(got.Columns))
	}
}

func TestDecodeMetadataNil(t *testing.T) {
	ta := &pb.TableAssignment{Metadata: nil}
	got, err := decodeMetadata(ta)
	if err != nil {
		t.Fatalf("decodeMetadata(nil): %v", err)
	}
	if len(got) != 0 {
		t.Errorf("empty = %v, want empty", got)
	}
}

func TestDecodeMetadataWithData(t *testing.T) {
	ta := &pb.TableAssignment{
		Metadata: []byte(`[{"from": "op", "as": "operation"}]`),
	}
	got, err := decodeMetadata(ta)
	if err != nil {
		t.Fatalf("decodeMetadata: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
}

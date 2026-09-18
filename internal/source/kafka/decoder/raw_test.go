package decoder

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Raw lands the payload verbatim — even content that is not valid JSON —
// because the whole point is to not understand the message.
func TestRawDecoderPassthrough(t *testing.T) {
	d := &Raw{}
	payload := []byte(`not-json-at-all-{unbalanced`)
	rec := &kgo.Record{Value: payload}
	changes, err := d.Decode(rec)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("changes = %d, want 1", len(changes))
	}
	c := changes[0]
	if c.Op.String() != "insert" {
		t.Fatalf("op = %q, want insert (raw landing has no update/delete)", c.Op)
	}
	if c.After["payload"] != "not-json-at-all-{unbalanced" {
		t.Fatalf("payload = %v, want the verbatim value", c.After["payload"])
	}
}

// A tombstone (nil value) becomes a row with a NULL payload, never an
// empty string — the absence of a payload is a fact.
func TestRawDecoderTombstoneNilPayload(t *testing.T) {
	d := &Raw{}
	changes, err := d.Decode(&kgo.Record{})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if v, ok := changes[0].After["payload"]; !ok || v != nil {
		t.Fatalf("payload = %v (present %v), want NULL", v, ok)
	}
}

// Declaring ByTopic[topic] extracts named JSON fields as columns instead of
// landing the opaque blob, for that topic only.
func TestRawDecoderExtractsDeclaredFields(t *testing.T) {
	d := &Raw{ByTopic: map[string]TopicExtraction{
		"orders": {Fields: []Field{{Name: "id"}, {Name: "customer_id"}}},
	}}
	rec := &kgo.Record{Topic: "orders", Value: []byte(`{"id": 1, "customer_id": 9, "extra": "dropped"}`)}
	changes, err := d.Decode(rec)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	after := changes[0].After
	if _, ok := after["payload"]; ok {
		t.Error("payload must not be present when extraction is declared without KeepPayload")
	}
	if _, ok := after["extra"]; ok {
		t.Error("a field not declared must not land as a column")
	}
	if _, ok := after["id"]; !ok {
		t.Fatal("id must be extracted")
	}
}

// A topic absent from ByTopic stays fully opaque even when other topics on
// the same decoder extract fields.
func TestRawDecoderUndeclaredTopicStaysOpaque(t *testing.T) {
	d := &Raw{ByTopic: map[string]TopicExtraction{
		"orders": {Fields: []Field{{Name: "id"}}},
	}}
	rec := &kgo.Record{Topic: "clicks", Value: []byte(`not-json-at-all`)}
	changes, err := d.Decode(rec)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if changes[0].After["payload"] != "not-json-at-all" {
		t.Errorf("payload = %v, want the verbatim value (clicks is not declared)", changes[0].After["payload"])
	}
}

// KeepPayload lands the full JSON alongside the extracted columns — the
// full-fidelity bronze guarantee, opted into.
func TestRawDecoderKeepPayloadAlongsideExtraction(t *testing.T) {
	d := &Raw{ByTopic: map[string]TopicExtraction{
		"orders": {Fields: []Field{{Name: "id"}}, KeepPayload: true},
	}}
	rec := &kgo.Record{Topic: "orders", Value: []byte(`{"id": 1}`)}
	changes, err := d.Decode(rec)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	after := changes[0].After
	if after["payload"] != `{"id": 1}` {
		t.Errorf("payload = %v, want the verbatim JSON", after["payload"])
	}
	if _, ok := after["id"]; !ok {
		t.Error("id must still be extracted")
	}
}

// Nested paths reach into the document.
func TestRawDecoderExtractsNestedPath(t *testing.T) {
	d := &Raw{ByTopic: map[string]TopicExtraction{
		"orders": {Fields: []Field{{Name: "order_total", Path: "totals.grand_total"}}},
	}}
	rec := &kgo.Record{Topic: "orders", Value: []byte(`{"totals": {"grand_total": 42.5}}`)}
	changes, err := d.Decode(rec)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if changes[0].After["order_total"].(json.Number) != "42.5" {
		t.Errorf("order_total = %v, want 42.5", changes[0].After["order_total"])
	}
}

// A payload that does not parse as JSON is a hard failure once extraction is
// declared — the spec asserted this topic is JSON.
func TestRawDecoderExtractionRequiresJSON(t *testing.T) {
	d := &Raw{ByTopic: map[string]TopicExtraction{
		"orders": {Fields: []Field{{Name: "id"}}},
	}}
	rec := &kgo.Record{Topic: "orders", Value: []byte(`not-json-at-all-{unbalanced`)}
	_, err := d.Decode(rec)
	var notJSON *ErrNotJSON
	if !errors.As(err, &notJSON) {
		t.Fatalf("err = %v, want *ErrNotJSON", err)
	}
}

// A tombstone with extraction declared lands every field NULL rather than
// failing — "no payload" is the same absence as "no fields".
func TestRawDecoderExtractionTombstoneAllNull(t *testing.T) {
	d := &Raw{ByTopic: map[string]TopicExtraction{
		"orders": {Fields: []Field{{Name: "id"}, {Name: "customer_id"}}},
	}}
	changes, err := d.Decode(&kgo.Record{Topic: "orders"})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	after := changes[0].After
	if after["id"] != nil || after["customer_id"] != nil {
		t.Errorf("after = %v, want every field NULL", after)
	}
}

// A required field absent from the payload fails the record.
func TestRawDecoderRequiredFieldMissingFails(t *testing.T) {
	d := &Raw{ByTopic: map[string]TopicExtraction{
		"orders": {Fields: []Field{{Name: "id", Required: true}}},
	}}
	rec := &kgo.Record{Topic: "orders", Value: []byte(`{}`)}
	_, err := d.Decode(rec)
	var missing *ErrFieldMissing
	if !errors.As(err, &missing) {
		t.Fatalf("err = %v, want *ErrFieldMissing", err)
	}
}

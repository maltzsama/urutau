package decoder

import (
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

package iceberg

import (
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/change"
	"github.com/maltzsama/urutau/internal/core"
)

// Transport metadata projects the message-queue envelope for a kafka event.
func TestMetaValueTransport(t *testing.T) {
	ts := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	c := change.Change{
		Position: "kafka:orders@0=42",
		Transport: &change.Transport{
			Stream:  "orders",
			Shard:   "0",
			Seq:     "42",
			MsgTS:   ts,
			MsgKey:  "k-1",
			Headers: `{"x":"1"}`,
		},
	}

	cases := []struct {
		key  core.MetadataKey
		want any
	}{
		{core.MetaStream, "orders"},
		{core.MetaShard, "0"},
		{core.MetaSeq, "42"},
		{core.MetaMsgKey, "k-1"},
		{core.MetaHeaders, `{"x":"1"}`},
	}
	for _, tc := range cases {
		got, err := metaValue(tc.key, c, "orders")
		if err != nil {
			t.Fatalf("%s: %v", tc.key, err)
		}
		if got != tc.want {
			t.Fatalf("%s = %v, want %v", tc.key, got, tc.want)
		}
	}
	got, err := metaValue(core.MetaMsgTS, c, "")
	if err != nil {
		t.Fatalf("msg_ts: %v", err)
	}
	if !got.(time.Time).Equal(ts) {
		t.Fatalf("msg_ts = %v, want %v", got, ts)
	}
}

// For a CDC source (no Transport), stream is the source table and sequence
// is the event coordinate; the transport-only keys are NULL.
func TestMetaValueTransportDerivedForCDC(t *testing.T) {
	c := change.Change{Position: "0/1A"}
	if v, _ := metaValue(core.MetaStream, c, "shop.orders"); v != "shop.orders" {
		t.Fatalf("stream = %v, want shop.orders (the source table)", v)
	}
	if v, _ := metaValue(core.MetaSeq, c, "shop.orders"); v != "0/1A" {
		t.Fatalf("sequence = %v, want 0/1A (the event coordinate)", v)
	}
	for _, key := range []core.MetadataKey{core.MetaShard, core.MetaMsgTS, core.MetaMsgKey, core.MetaHeaders} {
		if v, _ := metaValue(key, c, "shop.orders"); v != nil {
			t.Fatalf("%s = %v, want NULL for CDC", key, v)
		}
	}
}

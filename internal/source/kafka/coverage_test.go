package kafka

// Coverage for the Kafka source's pure surface: capabilities, position
// parsing, topic→target mapping, the record→transport projection, and the
// no-op reader methods.

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/sourcepull"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

func TestCapabilities(t *testing.T) {
	c := capabilities()
	if !c.Stream || c.Snapshot || c.ChunkQuery || c.BeforeImage {
		t.Fatalf("capabilities = %+v", c)
	}
	if len(c.Modes) != 1 || c.Modes[0] != source.ModeCDC {
		t.Fatalf("modes = %v", c.Modes)
	}
}

func TestTopicToTarget(t *testing.T) {
	got := topicToTarget([]source.TableRef{
		{Source: "topic-a", Target: "raw.a"},
		{Source: "topic-b", Target: "raw.b"},
	})
	if got["topic-a"] != "raw.a" || got["topic-b"] != "raw.b" {
		t.Fatalf("topicToTarget = %v", got)
	}
}

func TestParsePosition(t *testing.T) {
	s := Source{}
	pos, err := s.ParsePosition("t:p0=5,p1=9")
	if err != nil {
		t.Fatalf("ParsePosition: %v", err)
	}
	off, ok := pos.(*position.Offsets)
	if !ok || off.Topics["t"][0] != 5 || off.Topics["t"][1] != 9 {
		t.Fatalf("ParsePosition = %#v", pos)
	}
	if _, err := s.ParsePosition("not-an-offset"); err == nil {
		t.Fatal("a malformed position must be rejected")
	}
}

func TestInitialPositionIsEmptyOffsets(t *testing.T) {
	s := Source{}
	pos, err := s.InitialPosition(context.Background())
	if err != nil {
		t.Fatalf("InitialPosition: %v", err)
	}
	if _, ok := pos.(*position.Offsets); !ok {
		t.Fatalf("InitialPosition = %T, want *position.Offsets", pos)
	}
}

func TestReaderPositionAndNoops(t *testing.T) {
	off := position.NewOffsets("t", map[int32]int64{0: 3})
	r := &Reader{synced: off}
	// Synced/Master return a copy, not the live maps — the consume loop keeps
	// mutating r.synced — so compare by value.
	if got := r.Synced(); got.String() != off.String() {
		t.Fatalf("Synced = %s, want %s", got, off)
	}
	if got, err := r.Master(context.Background()); err != nil || got.String() != off.String() {
		t.Fatalf("Master = %v, %v", got, err)
	}
	// No-op lifecycle methods must not panic.
	r.OpenWindow(context.Background(), 1)
	r.ClearWindow()
	r.SetConfirmed(func() position.Position { return nil })
	r.Close() // nil client
}

func TestSetSourceSchemas(t *testing.T) {
	r := &Reader{puller: sourcepull.New(make(chan rowchange.Change))}
	r.SetSourceSchemas(map[string]core.Schema{
		"t": {Columns: []core.Column{{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}}}},
	})
}

func TestTransportOfHeadersAndFields(t *testing.T) {
	rec := &kgo.Record{
		Topic:     "topic-a",
		Partition: 2,
		Offset:    42,
		Timestamp: time.Unix(1, 0),
		Key:       []byte("k"),
		Headers:   []kgo.RecordHeader{{Key: "h1", Value: []byte("v1")}},
	}
	tr := transportOf(rec)
	if tr.Stream != "topic-a" || tr.Shard != "2" || tr.Seq != "42" {
		t.Fatalf("transport = %+v", tr)
	}
	if tr.MsgKey != "k" || !tr.MsgTS.Equal(time.Unix(1, 0)) {
		t.Fatalf("transport key/ts = %q, %v", tr.MsgKey, tr.MsgTS)
	}
	var headers map[string]string
	if err := json.Unmarshal([]byte(tr.Headers), &headers); err != nil {
		t.Fatalf("headers not JSON: %v", err)
	}
	if headers["h1"] != "v1" {
		t.Fatalf("headers = %v", headers)
	}
}

func TestKgoLogger(t *testing.T) {
	l := newKgoLogger(nil)
	if l.l == nil {
		t.Fatal("nil logger must default to slog.Default()")
	}
	if l.Level() != kgo.LogLevelInfo {
		t.Fatalf("Level = %v", l.Level())
	}
	l.Log(kgo.LogLevelInfo, "hello", "k", "v") // must not panic
}

// extractionByTopic builds one entry per table that declares columns, and
// separates the payload-column flag from the extracted fields.
func TestExtractionByTopic(t *testing.T) {
	sp := &spec.Spec{Tables: []spec.Table{
		{
			Source: "orders",
			Columns: map[string]spec.ColumnDecl{
				"payload":     {Scalar: "string"},
				"id":          {Scalar: "int64"},
				"customer_id": {Scalar: "int64", Required: true},
				"order_total": {Scalar: "decimal(20,4)", From: "totals.grand_total"},
			},
		},
		{Source: "clicks"}, // no Columns: stays opaque
	}}

	byTopic := extractionByTopic(sp, "payload")
	orders, ok := byTopic["orders"]
	if !ok {
		t.Fatal("orders must have an extraction entry")
	}
	if !orders.KeepPayload {
		t.Error("the payload column must set KeepPayload rather than becoming an extracted field")
	}
	if len(orders.Fields) != 3 {
		t.Fatalf("orders.Fields = %v, want 3 (id, customer_id, order_total)", orders.Fields)
	}
	var gotRequired, gotPath bool
	for _, f := range orders.Fields {
		if f.Name == "customer_id" && f.Required {
			gotRequired = true
		}
		if f.Name == "order_total" && f.Path == "totals.grand_total" {
			gotPath = true
		}
		if f.Name == "payload" {
			t.Error("the payload column must not also appear as an extracted field")
		}
	}
	if !gotRequired {
		t.Error("customer_id must carry Required")
	}
	if !gotPath {
		t.Error("order_total must carry its declared From path")
	}

	if _, ok := byTopic["clicks"]; ok {
		t.Error("a table with no declared columns must not get an extraction entry (stays opaque)")
	}
}

// avro has no payload column, so passing "" never treats any declared
// column as the raw-blob flag.
func TestExtractionByTopicNoPayloadColumnForAvro(t *testing.T) {
	sp := &spec.Spec{Tables: []spec.Table{
		{Source: "orders", Columns: map[string]spec.ColumnDecl{"id": {Scalar: "int64"}}},
	}}
	byTopic := extractionByTopic(sp, "")
	if byTopic["orders"].KeepPayload {
		t.Error("KeepPayload must never be set when there is no payload column name")
	}
}

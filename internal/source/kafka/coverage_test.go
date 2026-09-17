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
	if !ok || off.Topic != "t" || off.Parts[0] != 5 || off.Parts[1] != 9 {
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
	off := &position.Offsets{Topic: "t", Parts: map[int32]int64{0: 3}}
	r := &Reader{synced: off}
	if r.Synced() != off {
		t.Fatal("Synced must return the tracked offsets")
	}
	if got, err := r.Master(context.Background()); err != nil || got != off {
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

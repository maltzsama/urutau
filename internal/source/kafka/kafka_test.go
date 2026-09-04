package kafka

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestTransportOf(t *testing.T) {
	rec := &kgo.Record{
		Topic: "orders", Partition: 2, Offset: 99,
		Key:       []byte("k-7"),
		Timestamp: time.Unix(1700000000, 0),
		Headers: []kgo.RecordHeader{
			{Key: "trace", Value: []byte("abc")},
		},
	}
	tp := transportOf(rec)
	if tp.Stream != "orders" || tp.Shard != "2" || tp.Seq != "99" || tp.MsgKey != "k-7" {
		t.Fatalf("transport = %+v, want orders/2/99/k-7", tp)
	}
	if !tp.MsgTS.Equal(rec.Timestamp) {
		t.Fatalf("msg_ts = %v, want record timestamp", tp.MsgTS)
	}
	if tp.Headers != `{"trace":"abc"}` {
		t.Fatalf("headers = %q, want JSON map", tp.Headers)
	}
}

func TestTransportOfNoHeaders(t *testing.T) {
	tp := transportOf(&kgo.Record{Topic: "t", Partition: 0, Offset: 1})
	if tp.Headers != "" {
		t.Fatalf("headers = %q, want empty when none", tp.Headers)
	}
}

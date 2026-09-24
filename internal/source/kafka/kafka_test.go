package kafka

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/maltzsama/urutau/position"
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

func TestConsumePartitionsFromResumesAfterCommitted(t *testing.T) {
	resume := position.NewOffsets("orders", map[int32]int64{0: 5, 1: 12})
	got := consumePartitionsFrom(resume)
	want := map[int32]kgo.Offset{0: kgo.NewOffset().At(6), 1: kgo.NewOffset().At(13)}
	if len(got["orders"]) != len(want) {
		t.Fatalf("partitions = %d, want %d", len(got["orders"]), len(want))
	}
	for p, w := range want {
		g, ok := got["orders"][p]
		if !ok {
			t.Fatalf("partition %d missing from result", p)
		}
		// kgo.Offset has no exported comparison; compare through EpochOffset,
		// which At() sets deterministically (relative=false, epoch=-1).
		if g.EpochOffset() != w.EpochOffset() {
			t.Fatalf("partition %d offset = %+v, want %+v", p, g.EpochOffset(), w.EpochOffset())
		}
	}
}

func TestConsumePartitionsFromNilOrWrongType(t *testing.T) {
	if got := consumePartitionsFrom(nil); got != nil {
		t.Fatalf("nil resume: got %v, want nil", got)
	}
	if got := consumePartitionsFrom(&position.GTID{}); got != nil {
		t.Fatalf("wrong position type: got %v, want nil", got)
	}
}

func TestConsumePartitionsFromEmptyTopicSkipped(t *testing.T) {
	resume := &position.Offsets{Topics: map[string]map[int32]int64{"orders": {}}}
	if got := consumePartitionsFrom(resume); len(got) != 0 {
		t.Fatalf("empty-partition topic: got %v, want no entries", got)
	}
}

// TestSplitConsumeOptsNeverOverlaps guards the exact regression this fix
// caught in the pod e2e: kgo.NewClient refuses a topic present in BOTH
// ConsumePartitions and ConsumeTopics, so a coordinator restart resuming one
// topic while a second, fresh topic has no committed position yet must split
// them into disjoint sets, never leave the resumed topic in wholeTopics too.
func TestSplitConsumeOptsNeverOverlaps(t *testing.T) {
	resume := position.NewOffsets("resumed", map[int32]int64{0: 9})
	parts, wholeTopics := splitConsumeOpts([]string{"resumed", "fresh"}, resume)

	if _, ok := parts["resumed"]; !ok {
		t.Fatalf("parts = %v, want \"resumed\" present", parts)
	}
	for _, topic := range wholeTopics {
		if _, ok := parts[topic]; ok {
			t.Fatalf("topic %q in both parts and wholeTopics: %v / %v", topic, parts, wholeTopics)
		}
	}
	if len(wholeTopics) != 1 || wholeTopics[0] != "fresh" {
		t.Fatalf("wholeTopics = %v, want [fresh]", wholeTopics)
	}
}

func TestSplitConsumeOptsNoResumeGoesWholeTopic(t *testing.T) {
	parts, wholeTopics := splitConsumeOpts([]string{"a", "b"}, nil)
	if len(parts) != 0 {
		t.Fatalf("parts = %v, want none (no resume position)", parts)
	}
	if len(wholeTopics) != 2 {
		t.Fatalf("wholeTopics = %v, want [a b]", wholeTopics)
	}
}

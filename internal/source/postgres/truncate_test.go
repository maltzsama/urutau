package postgres

import (
	"encoding/binary"
	"log/slog"
	"strings"
	"testing"

	"github.com/maltzsama/urutau/source"
)

// truncatePayload builds a pgoutput Truncate message body: RelationNum,
// Option, then RelationNum relation ids.
func truncatePayload(ids ...uint32) []byte {
	b := make([]byte, 0, 9+4*len(ids))
	b = binary.BigEndian.AppendUint32(b, uint32(len(ids)))
	b = append(b, 0)
	for _, id := range ids {
		b = binary.BigEndian.AppendUint32(b, id)
	}
	return b
}

// A TRUNCATE is reported (metric + event) and, under the default policy,
// ignored: the stream carries on and the sink's divergence is visible.
func TestHandleTruncateReportsAndIgnores(t *testing.T) {
	var got []source.DestructiveDDL
	r := &Reader{
		cfg: Config{
			Logger:           slog.New(slog.DiscardHandler),
			OnTruncate:       "ignore",
			OnDestructiveDDL: func(d source.DestructiveDDL) { got = append(got, d) },
		},
		relByID: map[uint32]relEntry{7: {ref: source.TableRef{Source: "public.orders"}}},
	}
	if err := r.handleTruncate(truncatePayload(7, 9)); err != nil {
		t.Fatalf("handleTruncate: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("reports = %d, want 2", len(got))
	}
	if got[0].Source != "postgres" || got[0].Kind != "truncate" || got[0].Table != "public.orders" {
		t.Fatalf("report[0] = %+v", got[0])
	}
	// A relation not yet announced by a Relation message is named by OID, not
	// dropped.
	if got[1].Table != "relation#9" {
		t.Fatalf("unknown-relation report = %+v", got[1])
	}
}

// onTruncate: fail ends the stream with an error naming the table.
func TestHandleTruncateFails(t *testing.T) {
	r := &Reader{
		cfg:     Config{Logger: slog.New(slog.DiscardHandler), OnTruncate: "fail"},
		relByID: map[uint32]relEntry{7: {ref: source.TableRef{Source: "public.orders"}}},
	}
	err := r.handleTruncate(truncatePayload(7))
	if err == nil || !strings.Contains(err.Error(), "public.orders") {
		t.Fatalf("err = %v, want a failure naming the table", err)
	}
}

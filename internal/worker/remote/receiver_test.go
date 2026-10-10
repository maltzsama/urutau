package remote

import (
	"context"
	"log/slog"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/flight"

	"github.com/maltzsama/urutau/internal/transport"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/internal/worker"
	"github.com/maltzsama/urutau/position"
)

// The receiver turns a snapshot-done marker into a SnapshotDone ingest,
// carrying the marker's position and, on a staged table, its cycle. It is
// never skipped as covered: it carries no high position.
func TestReceiverRoutesTheSnapshotDoneMarker(t *testing.T) {
	ingest := make(chan worker.Ingest, 1)
	recv := &batchReceiver{
		ctx:       context.Background(),
		ingest:    ingest,
		committed: map[string]position.Position{"raw.orders": position.MustLSN("0/99")},
		parsePos:  parsePosition("postgres"),
		log:       slog.New(slog.DiscardHandler),
	}
	meta := &pb.BatchMeta{Table: "raw.orders", LowPos: "0/10", BatchId: 43, Staged: true, Window: &pb.WindowTag{SnapshotDone: true}}
	body, metaBytes, err := transport.EncodeBatch(nil, testSchema(), meta, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := recv.apply(&flight.FlightData{DataBody: body, AppMetadata: metaBytes}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	select {
	case ing := <-ingest:
		if !ing.SnapshotDone || ing.Position != "0/10" || ing.Seq != 43 || !ing.Staged || ing.Batch != nil {
			t.Fatalf("ingest %+v, want the done marker at 0/10 as cycle 43", ing)
		}
	default:
		t.Fatal("the done marker was not ingested")
	}
}

// The receiver carries a Closes marker's pending chunks onto its ingest.
func TestReceiverCarriesTheClosesMarkersPending(t *testing.T) {
	ingest := make(chan worker.Ingest, 1)
	recv := &batchReceiver{
		ctx:       context.Background(),
		ingest:    ingest,
		committed: map[string]position.Position{},
		parsePos:  parsePosition("postgres"),
		log:       slog.New(slog.DiscardHandler),
	}
	meta := &pb.BatchMeta{Table: "raw.orders", LowPos: "0/10", Window: &pb.WindowTag{Closes: true, WindowId: 7, SnapshotPending: []uint32{3, 4}}}
	body, metaBytes, err := transport.EncodeBatch(nil, testSchema(), meta, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := recv.apply(&flight.FlightData{DataBody: body, AppMetadata: metaBytes}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	select {
	case ing := <-ingest:
		if ing.Win == nil || !ing.Win.Closes || !slices.Equal(ing.SnapshotPending, []uint32{3, 4}) {
			t.Fatalf("ingest %+v, want the Closes marker carrying pending [3 4]", ing)
		}
	default:
		t.Fatal("the Closes marker was not ingested")
	}
}

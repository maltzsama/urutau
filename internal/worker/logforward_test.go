package worker

import (
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/logging"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"google.golang.org/protobuf/proto"
)

func TestWorkerLogForwarderSendsStructuredRecord(t *testing.T) {
	logger, buf, err := logging.NewBuffered("debug", "text", 8)
	if err != nil {
		t.Fatal(err)
	}
	sess := &fakeSession{ch: make(chan *pb.WorkerMessage, 4)}
	forwarder := newWorkerLogForwarder(buf, &sessionSender{s: sess}, 7)

	logger.Warn("commit slow", "table", "raw.orders", "attempt", 2)
	forwarder.stop()

	select {
	case msg := <-sess.ch:
		got := msg.GetLog()
		if got == nil || got.Msg != "commit slow" || got.Level != slog.LevelWarn.String() || got.Epoch != 7 {
			t.Fatalf("worker log = %v", got)
		}
		var attrs map[string]any
		if err := json.Unmarshal(got.AttrsJson, &attrs); err != nil {
			t.Fatal(err)
		}
		if attrs["table"] != "raw.orders" || attrs["attempt"] != float64(2) {
			t.Fatalf("attrs = %#v", attrs)
		}
		wire, err := proto.Marshal(msg)
		if err != nil {
			t.Fatal(err)
		}
		var roundTrip pb.WorkerMessage
		if err := proto.Unmarshal(wire, &roundTrip); err != nil {
			t.Fatal(err)
		}
		if roundTrip.GetLog().Msg != got.Msg || roundTrip.GetLog().Epoch != got.Epoch {
			t.Fatalf("round-trip log = %v", roundTrip.GetLog())
		}
	case <-time.After(time.Second):
		t.Fatal("worker log was not sent")
	}
}

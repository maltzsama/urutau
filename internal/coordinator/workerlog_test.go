package coordinator

import (
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/logging"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

func TestOnWorkerLogPublishesToCoordinatorLogPath(t *testing.T) {
	_, buf, err := logging.NewBuffered("debug", "text", 8)
	if err != nil {
		t.Fatal(err)
	}
	c := &Coordinator{
		cfg:     Config{LogBuffer: buf},
		workers: map[string]*workerState{"worker-1": {epoch: 4}},
	}
	at := time.Date(2026, 9, 27, 1, 2, 3, 0, time.UTC)
	c.onWorkerLog("worker-1", &pb.WorkerLog{
		Ts:        at.Format(time.RFC3339Nano),
		Level:     "ERROR",
		Msg:       "commit failed",
		AttrsJson: []byte(`{"table":"raw.orders","attempt":2}`),
		Epoch:     4,
	})

	got := buf.Tail(0, 1)
	if len(got) != 1 {
		t.Fatalf("records = %d, want 1", len(got))
	}
	if got[0].Message != "commit failed" || got[0].Level.String() != "ERROR" || !got[0].Time.Equal(at) {
		t.Fatalf("record = %+v", got[0])
	}
	if got[0].Attrs["worker"] != "worker-1" || got[0].Attrs["table"] != "raw.orders" {
		t.Fatalf("attrs = %#v", got[0].Attrs)
	}
}

func TestOnWorkerLogRejectsStaleEpoch(t *testing.T) {
	_, buf, err := logging.NewBuffered("debug", "text", 8)
	if err != nil {
		t.Fatal(err)
	}
	c := &Coordinator{
		cfg:     Config{LogBuffer: buf},
		workers: map[string]*workerState{"worker-1": {epoch: 4}},
	}
	c.onWorkerLog("worker-1", &pb.WorkerLog{Msg: "stale", Epoch: 3})
	if got := buf.Tail(0, 1); len(got) != 0 {
		t.Fatalf("stale records = %v, want none", got)
	}
}

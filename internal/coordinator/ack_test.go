package coordinator

import (
	"testing"
	"time"

	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

// #210: an unparsable ack position used to return early, leaving the batch at
// the head of the index and its budget charge held forever. It must fail the
// run instead.
func TestOnAckUnparsablePositionFailsRun(t *testing.T) {
	c, _ := coordHarness()
	c.src = errSource{} // ParsePosition always fails
	c.terminate = make(chan error, 1)

	c.onAck("w0", &pb.Ack{Table: "raw.orders", Position: "not-a-position"})

	select {
	case err := <-c.terminate:
		if err == nil {
			t.Fatal("an unparsable ack position must produce a terminal error")
		}
	case <-time.After(time.Second):
		t.Fatal("an unparsable ack position must fail the run, not leak the budget")
	}
}

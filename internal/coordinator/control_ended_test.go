package coordinator

import (
	"errors"
	"testing"

	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

// controlStream is a distinguishable stand-in for a worker's Control stream.
type controlStream struct {
	pb.UrutauControl_ControlServer
	id int
}

// A worker reconnects before the server notices its old Control stream died.
// When the old stream's handler finally returns, the worker's new generation
// must be left alone: no epoch bump, not marked lost, its new stream kept.
func TestAStaleControlHandlerLeavesTheNewGenerationAlone(t *testing.T) {
	c, w := coordHarness()
	old, fresh := &controlStream{id: 1}, &controlStream{id: 2}
	c.mu.Lock()
	w.control = fresh // the worker already reconnected
	epoch := w.epoch
	c.mu.Unlock()

	c.controlEnded(w, old, errors.New("stream closed"))

	c.mu.Lock()
	defer c.mu.Unlock()
	if w.epoch != epoch {
		t.Errorf("epoch = %d, want %d: the stale handler bumped the new generation", w.epoch, epoch)
	}
	if w.control != pb.UrutauControl_ControlServer(fresh) {
		t.Error("the stale handler replaced the new generation's Control stream")
	}
	if c.supervisor.IsPending(w.name) {
		t.Error("the stale handler marked the reconnected worker lost")
	}
}

// The handler that still owns the stream reports the worker lost.
func TestTheOwningControlHandlerReportsTheWorkerLost(t *testing.T) {
	c, w := coordHarness()
	stream := &controlStream{id: 1}
	c.mu.Lock()
	w.control = stream
	epoch := w.epoch
	c.mu.Unlock()

	c.controlEnded(w, stream, errors.New("stream closed"))

	c.mu.Lock()
	defer c.mu.Unlock()
	if w.control != nil {
		t.Error("the worker's Control stream is still set after its handler ended")
	}
	if w.epoch == epoch {
		t.Error("the epoch did not move: the loss was not recorded")
	}
	if !c.supervisor.IsPending(w.name) {
		t.Error("the worker is not marked lost")
	}
}

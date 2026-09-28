package main

import (
	"errors"
	"fmt"
	"testing"

	"github.com/maltzsama/urutau/internal/worker"
)

// Issue #461: a worker records why it exited in its Pod's termination
// message, which the coordinator reads when the worker comes back. Losing the
// coordinator is marked "network: ", which is not a crash.
func TestTerminationMessageMarksALostCoordinator(t *testing.T) {
	lost := fmt.Errorf("run: %w", worker.CoordinatorLost(errors.New("worker: channel lost: connection timed out")))
	if got := terminationMessage(lost); got != "network: run: worker: channel lost: connection timed out" {
		t.Fatalf("terminationMessage = %q, want it marked network", got)
	}
	crash := errors.New("worker: table raw.t: commit: boom")
	if got := terminationMessage(crash); got != crash.Error() {
		t.Fatalf("terminationMessage = %q, want the error as is", got)
	}
}

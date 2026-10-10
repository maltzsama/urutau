package remote

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/maltzsama/urutau/internal/worker"
)

// Sentinel causes distinguishing graceful shutdown from channel death.
var (
	errGracefulEOF = errors.New("worker: coordinator closed the stream cleanly")
	errShutdown    = errors.New("worker: shutdown signal received")
)

// workerShutdown drains and exits cleanly when the coordinator intended to
// shut down (graceful EOF, shutdown signal, or the parent ctx cancelling);
// on an anomalous channel death it aborts in-flight transactions instead —
// a commit that completes after the channel is lost is indistinguishable
// from a zombie's (design §5.5).
func workerShutdown(cause error, pipeCancel context.CancelFunc, pipeCtx context.Context,
	runErr <-chan error, ingest chan<- worker.Ingest, log *slog.Logger) error {

	graceful := errors.Is(cause, errGracefulEOF) || errors.Is(cause, errShutdown) ||
		errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded)

	close(ingest)
	if !graceful {
		pipeCancel()
		log.Error("worker: channel lost, aborting in-flight transactions", "cause", cause)
		select {
		case <-runErr:
		case <-time.After(5 * time.Second):
		}
		return CoordinatorLost(fmt.Errorf("worker: channel lost: %w", cause))
	}

	// Graceful: drain whatever is buffered; pipeCtx is still alive unless
	// the parent ctx itself was cancelled.
	log.Info("worker: draining", "cause", cause)
	select {
	case err := <-runErr:
		return err
	case <-time.After(30 * time.Second):
		pipeCancel()
		return fmt.Errorf("worker: drain timeout: %w", cause)
	}
}

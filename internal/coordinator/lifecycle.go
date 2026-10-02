package coordinator

import (
	"context"
	"time"

	"github.com/maltzsama/urutau/internal/eventlog"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

// maxConcurrentEmits bounds the audit-trail uploads in flight at once. A slow
// S3 endpoint must not turn the ack hot path into an unbounded goroutine herd
// (issue #494).
const maxConcurrentEmits = 8

// signalReady wakes waitWorkers on an attach without ever wedging a session.
// During boot it blocks on ready, which waitWorkers drains, and also aborts on
// either context. Once booted, waitWorkers no longer drains ready, so a churny
// re-attach skips the send entirely instead of blocking on a full buffer
// (issue #493).
func (c *Coordinator) signalReady(sessCtx, streamCtx context.Context) {
	if c.booted.Load() {
		return
	}
	select {
	case c.ready <- struct{}{}:
	case <-sessCtx.Done():
	case <-streamCtx.Done():
	}
}

// emitCommit writes a commit event to the audit trail off the ack hot path,
// bounded to maxConcurrentEmits in-flight uploads. A saturated queue drops the
// event rather than stalling acks: the trail is best-effort (issue #494).
func (c *Coordinator) emitCommit(worker string, ack *pb.Ack) {
	select {
	case c.emitSem <- struct{}{}:
	default:
		c.log.Warn("coordinator: eventlog emit queue full; dropping commit event",
			"worker", worker, "table", ack.Table)
		return
	}
	go func() {
		defer func() { <-c.emitSem }()
		if err := c.emit(eventlog.KindCommit, map[string]any{
			"worker":   worker,
			"table":    ack.Table,
			"rows":     ack.Rows,
			"deletes":  ack.Deletes,
			"position": ack.Position,
		}); err != nil {
			c.log.Warn("coordinator: eventlog emit", "err", err)
		}
	}()
}

// shutdownMetrics stops the metrics/dashboard HTTP server, if one was started,
// so run's return releases MetricsAddr instead of leaking the listener and its
// goroutine (issue #495).
func (c *Coordinator) shutdownMetrics() {
	if c.metricsSrv == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.metricsSrv.Shutdown(ctx); err != nil {
		c.log.Warn("coordinator: metrics server shutdown", "err", err)
	}
}

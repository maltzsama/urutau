package coordinator

import (
	"context"
	"time"

	"github.com/maltzsama/urutau/internal/eventlog"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/source"
)

// reportDestructiveDDL records a TRUNCATE/destructive DDL the source stream
// carried but the engine did not propagate: the sink diverges from the source
// until an operator reconciles it. It increments the metric and emits a run
// event so the divergence is visible instead of only a lost log line
// (issue #671). Called from the source reader goroutine.
func (c *Coordinator) reportDestructiveDDL(d source.DestructiveDDL) {
	if c.metrics != nil {
		if d.Kind == "truncate" {
			c.metrics.SourceTruncates.WithLabelValues(d.Source, d.Table).Inc()
		} else {
			c.metrics.SourceDestructiveDDL.WithLabelValues(d.Source, d.Kind, d.Table).Inc()
		}
	}
	_ = c.emit(eventlog.KindDestructiveDDL, map[string]any{
		"source": d.Source,
		"kind":   d.Kind,
		"table":  d.Table,
		"detail": d.Detail,
	})
}

// emit writes one event to the audit trail when configured; best-effort by
// contract (a lost trail must never fail the pipeline). The dashboard's
// recent-events ring is fed here too, unconditionally, so the UI shows events
// even when no audit trail is configured.
func (c *Coordinator) emit(kind string, fields map[string]any) error {
	c.dashRecord(kind, fields)
	return c.emitTrail(kind, fields)
}

// dashRecord records an event in the dashboard ring and pushes it to the SSE
// subscribers. It is in-memory and cheap, so a saturated audit-trail queue on
// the ack hot path must not suppress it (issue #494).
func (c *Coordinator) dashRecord(kind string, fields map[string]any) {
	if c.dashEvents == nil {
		return
	}
	ev := c.dashEvents.Record(kind, fields)
	if c.dash != nil {
		c.dash.PublishEvent(ev)
	}
}

// emitTrail enqueues an event for the audit trail (S3). It is non-blocking:
// the eventlog's own bounded queue absorbs bursts and drops (with a counter)
// when saturated, so the ack hot path never stalls and no caller-side
// semaphore is needed (issue #548).
func (c *Coordinator) emitTrail(kind string, fields map[string]any) error {
	if c.ev == nil {
		return nil
	}
	return c.ev.Emit(context.Background(), kind, fields)
}

// signalReady wakes waitWorkers on an attach without ever wedging a session.
// During boot it blocks on ready, which waitWorkers drains, and also aborts on
// either context. booted is closed once waitWorkers returns; selecting on it
// too means a session already blocked on a full ready unblocks the moment boot
// finishes, even though nothing drains ready after that (issue #493).
func (c *Coordinator) signalReady(sessCtx, streamCtx context.Context) {
	select {
	case c.ready <- struct{}{}:
	case <-c.booted:
	case <-sessCtx.Done():
	case <-streamCtx.Done():
	}
}

// emitCommit records a commit event in the dashboard and enqueues it for the
// audit trail. Both are cheap and non-blocking — the dashboard is in-memory
// and the trail enqueue is bounded — so the ack hot path never stalls
// (issues #494, #548).
func (c *Coordinator) emitCommit(worker string, ack *pb.Ack) {
	fields := map[string]any{
		"worker":   worker,
		"table":    ack.Table,
		"rows":     ack.Rows,
		"deletes":  ack.Deletes,
		"position": ack.Position,
	}
	c.dashRecord(eventlog.KindCommit, fields)
	if err := c.emitTrail(eventlog.KindCommit, fields); err != nil {
		c.log.Warn("coordinator: eventlog emit", "err", err)
	}
}

// shutdownMetrics stops the metrics/dashboard HTTP server, if one was started,
// so run's return releases MetricsAddr instead of leaking the listener and its
// goroutine (issue #495). It waits for the server goroutine to exit, closing
// the race where Shutdown lands before ListenAndServe first runs.
func (c *Coordinator) shutdownMetrics() {
	if c.metricsSrv == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.metricsSrv.Shutdown(ctx); err != nil {
		c.log.Warn("coordinator: metrics server shutdown", "err", err)
		// A streaming (SSE) handler ignores the write deadline and can hold
		// its connection past ctx; Shutdown then returns with it still open.
		// Force-close so Run does not leave it behind (issue #495).
		if closeErr := c.metricsSrv.Close(); closeErr != nil {
			c.log.Warn("coordinator: metrics server close", "err", closeErr)
		}
	}
	if c.metricsDone != nil {
		<-c.metricsDone
	}
}

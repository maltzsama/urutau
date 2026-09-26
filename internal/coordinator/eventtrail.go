package coordinator

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maltzsama/urutau/internal/eventlog"
	"github.com/maltzsama/urutau/internal/logging"
)

// The run's S3 trail is where history lives once the process is gone: the
// dashboard's log buffer dies with it. logTrail bridges the two — every
// structured record the coordinator logs is queued here and appended to the
// trail as a kind="log" event, while the live tail keeps flowing to the
// dashboard from the buffer. The dashboard never reads the trail, and the
// history server never reads the buffer.

const (
	// logTrailQueue bounds the records waiting for the next flush: a burst
	// between flushes fits, and an S3 outage cannot grow the process without
	// limit — beyond it the oldest queued records are dropped and counted.
	logTrailQueue = 4096
	// logTrailBatch caps how many records travel in one PUT.
	logTrailBatch = 128
	// logTrailPeriod is how often a non-empty queue is flushed. Together with
	// the batch cap it bounds the PUT rate at one per second per run.
	logTrailPeriod = time.Second
)

// trailWriter is the slice of *eventlog.Run the trail writes through; tests
// fake it.
type trailWriter interface {
	EmitBatch(ctx context.Context, kind string, batch []map[string]any) error
}

// logTrail is the asynchronous bridge from the logging path to the run's S3
// trail. Records enter through sink (non-blocking, called on the logging
// path) and leave through one goroutine that batches them into EmitBatch
// calls. Safe for concurrent use.
type logTrail struct {
	trail  trailWriter
	buf    *logging.Buffer
	log    *slog.Logger
	ch     chan logging.Record
	stopCh chan struct{}
	wg     sync.WaitGroup
	once   sync.Once
	drop   atomic.Int64
}

// newLogTrail attaches a trail to buf and starts its flusher.
func newLogTrail(trail trailWriter, buf *logging.Buffer, log *slog.Logger) *logTrail {
	if log == nil {
		log = slog.Default()
	}
	t := &logTrail{
		trail:  trail,
		buf:    buf,
		log:    log,
		ch:     make(chan logging.Record, logTrailQueue),
		stopCh: make(chan struct{}),
	}
	buf.SetSink(t.sink)
	t.wg.Add(1)
	go t.loop()
	return t
}

// sink is the logging path: it must never block, so a full queue drops the
// record and says so instead of stalling whatever produced the log line. The
// records are still on stderr and in the local buffer.
func (t *logTrail) sink(r logging.Record) {
	select {
	case t.ch <- r:
	default:
		if n := t.drop.Add(1); n == 1 || n%1000 == 0 {
			t.log.Warn("coordinator: log trail queue full; records dropped",
				"dropped", n, "queued", len(t.ch))
		}
	}
}

// loop batches queued records and flushes them on the batch cap, the period,
// or shutdown — in that order, so a burst flushes before the timer would.
func (t *logTrail) loop() {
	defer t.wg.Done()
	ticker := time.NewTicker(logTrailPeriod)
	defer ticker.Stop()
	batch := make([]logging.Record, 0, logTrailBatch)
	flush := func() {
		if len(batch) > 0 {
			t.emit(batch)
			batch = batch[:0]
		}
	}
	for {
		select {
		case r := <-t.ch:
			batch = append(batch, r)
			if len(batch) >= logTrailBatch {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-t.stopCh:
			// Detached already: drain what is queued, flush once, and leave.
			for {
				select {
				case r := <-t.ch:
					batch = append(batch, r)
					if len(batch) >= logTrailBatch {
						flush()
					}
				default:
					flush()
					return
				}
			}
		}
	}
}

// emit appends one batch to the trail. Best-effort, like every other trail
// write: a failed PUT leaves the lines in the eventlog's own buffer, so the
// next flush (or Close) re-uploads them rather than losing them.
func (t *logTrail) emit(batch []logging.Record) {
	fields := make([]map[string]any, 0, len(batch))
	for _, r := range batch {
		fields = append(fields, logRecordFields(r))
	}
	// The run's context is often already cancelled at shutdown, and the final
	// flush must still land: the PUT carries its own timeout inside EmitBatch.
	if err := t.trail.EmitBatch(context.Background(), eventlog.KindLog, fields); err != nil {
		t.log.Warn("coordinator: log trail emit", "records", len(fields), "err", err)
	}
}

// stop detaches the sink, drains the queue and waits for the final flush. The
// caller must run it before sealing the run.
func (t *logTrail) stop() {
	t.once.Do(func() {
		t.buf.SetSink(nil)
		close(t.stopCh)
		t.wg.Wait()
	})
}

// logRecordFields renders one slog record as trail fields. ts is the record's
// own time — the reader shows it, so a batch flushed a second later is not
// stamped with the flush time.
func logRecordFields(r logging.Record) map[string]any {
	f := make(map[string]any, len(r.Attrs)+3)
	f["ts"] = r.Time.UTC().Format(time.RFC3339Nano)
	f["level"] = r.Level.String()
	f["msg"] = r.Message
	if len(r.Attrs) > 0 {
		f["attrs"] = r.Attrs
	}
	return f
}

// startEventlog opens the run's S3 trail, wires the coordinator's structured
// logs into it, and returns the shutdown func for the caller to defer: it
// drains the log trail first, then seals the run, so the last records reach
// the trail before the terminal marker closes it.
func (c *Coordinator) startEventlog(ctx context.Context, cfg eventlog.Config) (func(), error) {
	// Apply the shared key convention: the trail lives under the pipeline
	// name so it stays discoverable after the CR is deleted.
	if cfg.Pipeline == "" && c.cfg.Spec != nil {
		cfg.Pipeline = c.cfg.Spec.Pipeline
	}
	ev, err := eventlog.New(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("coordinator: eventlog: %w", err)
	}
	c.ev = ev
	var trail *logTrail
	if c.cfg.LogBuffer != nil {
		trail = newLogTrail(ev, c.cfg.LogBuffer, c.log)
	}
	if err := c.emit(eventlog.KindJobStarted, map[string]any{
		"pipeline": c.cfg.Spec.Pipeline,
		"source":   c.cfg.Spec.Source.Kind,
	}); err != nil {
		c.log.Warn("coordinator: eventlog emit", "err", err)
	}
	return func() {
		if trail != nil {
			trail.stop()
		}
		if err := ev.Close(); err != nil {
			c.log.Warn("coordinator: eventlog close", "err", err)
		}
	}, nil
}

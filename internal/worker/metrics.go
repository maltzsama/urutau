package worker

import (
	"context"
	"log/slog"
	"strings"
	"time"

	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

// workerMetricsInterval is how often the worker ships its own series to the
// coordinator. Frequent enough for the dashboard, cheap on the control stream.
const workerMetricsInterval = 5 * time.Second

// reportWorkerMetrics ships the worker's own series to the coordinator, which
// records them for the dashboard — the worker's /metrics is per-pod and the
// coordinator cannot scrape it. Best-effort: a send failure ends the reporter
// (the session is gone anyway).
func reportWorkerMetrics(ctx context.Context, w *Worker, sender *sessionSender, log *slog.Logger) {
	ticker := time.NewTicker(workerMetricsInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rep := w.MetricsSnapshot()
			if rep == nil {
				continue
			}
			if err := sender.send(&pb.WorkerMessage{Msg: &pb.WorkerMessage_WorkerMetrics{WorkerMetrics: rep}}); err != nil {
				if log != nil {
					log.Warn("worker: metrics report", "err", err)
				}
				return
			}
		}
	}
}

// MetricsSnapshot renders the worker's own series as the wire report. Counters
// are cumulative totals; snapshot progress is absolute. Nil when the worker has
// no registry (it always does today).
func (w *Worker) MetricsSnapshot() *pb.WorkerMetricsReport {
	if w.metrics == nil {
		return nil
	}
	snap := w.metrics.WorkerSnapshot()
	rep := &pb.WorkerMetricsReport{}
	byTable := map[string]*pb.TableMetrics{}
	get := func(t string) *pb.TableMetrics {
		tm := byTable[t]
		if tm == nil {
			tm = &pb.TableMetrics{Table: t}
			byTable[t] = tm
			rep.Tables = append(rep.Tables, tm)
		}
		return tm
	}
	for t, v := range snap.CommitFailures {
		get(t).CommitFailures = v
	}
	for t, v := range snap.DeletesDropped {
		get(t).DeletesDropped = v
	}
	for t, v := range snap.SnapshotProgress {
		get(t).SnapshotProgress = v
	}
	for t, v := range snap.CommitLatencyMs {
		get(t).CommitLatencyMs = v
	}
	for key, ec := range snap.Enrich {
		t, ref, _ := strings.Cut(key, "\x00")
		tm := get(t)
		tm.Enrich = append(tm.Enrich, &pb.EnrichMetrics{
			Reference:    ref,
			Misses:       ec.Misses,
			InnerDropped: ec.InnerDropped,
			Evicted:      ec.Evicted,
		})
	}
	return rep
}

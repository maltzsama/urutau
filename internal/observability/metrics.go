// Package observability exposes the lean Prometheus metrics (§13.1) and the
// live status endpoint (§13.4): a small set of series, no per-event logging.
package observability

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics is the process's metric registry.
type Metrics struct {
	reg *prometheus.Registry

	// Coordinator.
	LagSeconds    prometheus.Gauge
	InflightBytes *prometheus.GaugeVec
	WorkerResets  *prometheus.CounterVec
	CommitsTotal  *prometheus.CounterVec
	EventsDecoded prometheus.Counter

	// Worker.
	RowsWritten      *prometheus.CounterVec
	CommitDuration   *prometheus.HistogramVec
	CommitFailures   *prometheus.CounterVec
	EqualityDeletes  *prometheus.CounterVec
	SnapshotProgress *prometheus.GaugeVec
	DroppedByWindow  *prometheus.CounterVec
}

func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{reg: reg}

	m.LagSeconds = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "urutau_coordinator_lag_seconds", Help: "reader-to-worker lag in seconds."})
	m.InflightBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "urutau_coordinator_inflight_bytes", Help: "unacked batch bytes per worker."},
		[]string{"worker"})
	m.WorkerResets = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "urutau_coordinator_worker_resets_total", Help: "worker resets by reason."},
		[]string{"worker", "reason"})
	m.CommitsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "urutau_coordinator_commits_total", Help: "commits acked by the worker, per table."},
		[]string{"table"})
	m.EventsDecoded = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "urutau_coordinator_events_decoded_total", Help: "decoded source events."})

	m.RowsWritten = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "urutau_worker_rows_written_total", Help: "rows written per table and op."},
		[]string{"table", "op"})
	m.CommitDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "urutau_worker_commit_duration_seconds",
		Help:    "Iceberg commit latency per table.",
		Buckets: prometheus.DefBuckets},
		[]string{"table"})
	m.CommitFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "urutau_worker_commit_failures_total", Help: "failed commits per table."},
		[]string{"table"})
	m.EqualityDeletes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "urutau_worker_equality_deletes_written_total", Help: "equality deletes written per table."},
		[]string{"table"})
	m.SnapshotProgress = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "urutau_worker_snapshot_progress_ratio", Help: "snapshot progress per table (0..1)."},
		[]string{"table"})
	m.DroppedByWindow = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "urutau_worker_dblog_dropped_by_window_total", Help: "snapshot rows discarded by DBLog windows."},
		[]string{"table"})

	reg.MustRegister(m.LagSeconds, m.InflightBytes, m.WorkerResets, m.CommitsTotal, m.EventsDecoded)
	reg.MustRegister(m.RowsWritten, m.CommitDuration, m.CommitFailures, m.EqualityDeletes, m.SnapshotProgress, m.DroppedByWindow)
	return m
}

// Serve exposes /metrics (Prometheus) and, when encoder is non-nil,
// /statusz (live JSON state) on addr. Blocks until the server stops.
func (m *Metrics) Serve(addr string, encoder func(w http.ResponseWriter, r *http.Request)) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{}))
	if encoder != nil {
		mux.HandleFunc("/statusz", encoder)
	}
	return http.ListenAndServe(addr, mux)
}

// Registry exposes the underlying registry (for tests).
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// Package observability exposes the lean Prometheus metrics (§13.1) and the
// live status endpoint (§13.4): a small set of series, no per-event logging.
package observability

import (
	"net/http"
	"time"

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
	DeletesDropped   *prometheus.CounterVec

	// Enrich.
	EnrichMisses  *prometheus.CounterVec
	EnrichDropped *prometheus.CounterVec
	EnrichEvicted *prometheus.CounterVec

	// Iceberg table maintenance (issue #96): compaction, snapshot expiry,
	// orphan cleanup. All labeled by table; "runs" counters increment on
	// every attempt (success or failure) so a stalled maintainer (0 runs
	// while the pipeline is otherwise healthy) is visible without a
	// separate liveness signal.
	IcebergCompactionRuns          *prometheus.CounterVec
	IcebergCompactionFilesRemoved  *prometheus.CounterVec
	IcebergCompactionFilesAdded    *prometheus.CounterVec
	IcebergCompactionBytesBefore   *prometheus.CounterVec
	IcebergCompactionBytesAfter    *prometheus.CounterVec
	IcebergSnapshotExpiryRuns      *prometheus.CounterVec
	IcebergSnapshotExpirySnapshots *prometheus.CounterVec
	IcebergOrphanCleanupRuns       *prometheus.CounterVec
	IcebergOrphanCleanupFiles      *prometheus.CounterVec
	IcebergOrphanCleanupBytes      *prometheus.CounterVec
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
	m.DeletesDropped = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "urutau_worker_deletes_dropped_total", Help: "append-only deletes dropped (skip or no before image), per table."},
		[]string{"table"})

	m.EnrichMisses = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "urutau_enrich_misses_total", Help: "enrichment left-join misses (events passed with NULL reference columns)."},
		[]string{"table", "reference"})
	m.EnrichDropped = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "urutau_enrich_inner_dropped_total", Help: "events dropped by inner-join miss."},
		[]string{"table", "reference"})
	m.EnrichEvicted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "urutau_enrich_evicted_total", Help: "events evicted from the cold-start buffer (maxEvents or maxWait)."},
		[]string{"table", "reference"})

	m.IcebergCompactionRuns = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "urutau_iceberg_compaction_runs_total", Help: "compaction attempts per table, success or failure."},
		[]string{"table"})
	m.IcebergCompactionFilesRemoved = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "urutau_iceberg_compaction_files_removed_total", Help: "data files removed by compaction, per table."},
		[]string{"table"})
	m.IcebergCompactionFilesAdded = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "urutau_iceberg_compaction_files_added_total", Help: "data files added by compaction, per table."},
		[]string{"table"})
	m.IcebergCompactionBytesBefore = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "urutau_iceberg_compaction_bytes_before", Help: "input bytes rewritten by compaction, per table."},
		[]string{"table"})
	m.IcebergCompactionBytesAfter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "urutau_iceberg_compaction_bytes_after", Help: "output bytes written by compaction, per table."},
		[]string{"table"})
	m.IcebergSnapshotExpiryRuns = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "urutau_iceberg_snapshot_expiry_runs_total", Help: "snapshot expiry attempts per table, success or failure."},
		[]string{"table"})
	m.IcebergSnapshotExpirySnapshots = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "urutau_iceberg_snapshot_expiry_snapshots_removed_total", Help: "snapshots removed by expiry, per table."},
		[]string{"table"})
	m.IcebergOrphanCleanupRuns = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "urutau_iceberg_orphan_cleanup_runs_total", Help: "orphan cleanup attempts per table, success or failure."},
		[]string{"table"})
	m.IcebergOrphanCleanupFiles = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "urutau_iceberg_orphan_cleanup_files_deleted_total", Help: "unreferenced files deleted by orphan cleanup, per table."},
		[]string{"table"})
	m.IcebergOrphanCleanupBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "urutau_iceberg_orphan_cleanup_bytes_freed_total", Help: "storage bytes freed by orphan cleanup, per table."},
		[]string{"table"})

	reg.MustRegister(m.LagSeconds, m.InflightBytes, m.WorkerResets, m.CommitsTotal, m.EventsDecoded)
	reg.MustRegister(m.RowsWritten, m.CommitDuration, m.CommitFailures, m.EqualityDeletes, m.SnapshotProgress, m.DroppedByWindow, m.DeletesDropped)
	reg.MustRegister(m.EnrichMisses, m.EnrichDropped, m.EnrichEvicted)
	reg.MustRegister(m.IcebergCompactionRuns, m.IcebergCompactionFilesRemoved, m.IcebergCompactionFilesAdded,
		m.IcebergCompactionBytesBefore, m.IcebergCompactionBytesAfter)
	reg.MustRegister(m.IcebergSnapshotExpiryRuns, m.IcebergSnapshotExpirySnapshots)
	reg.MustRegister(m.IcebergOrphanCleanupRuns, m.IcebergOrphanCleanupFiles, m.IcebergOrphanCleanupBytes)
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
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	return srv.ListenAndServe()
}

// Registry exposes the underlying registry (for tests).
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

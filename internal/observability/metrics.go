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
	LagSeconds     *prometheus.GaugeVec
	PendingBatches *prometheus.GaugeVec
	InflightBytes  *prometheus.GaugeVec
	WorkerResets   *prometheus.CounterVec
	CommitsTotal   *prometheus.CounterVec
	EventsDecoded  prometheus.Counter
	// SourceTruncates counts TRUNCATEs the source stream carried but the
	// engine did not propagate to the sink (issue #671).
	SourceTruncates *prometheus.CounterVec
	// SourceDestructiveDDL counts other destructive DDL (DROP/ALTER/RENAME)
	// the source carried but the engine did not propagate (issue #671).
	SourceDestructiveDDL *prometheus.CounterVec
	// EventlogQueue is the run-trail queue depth: a growing queue means the
	// flusher is slower than the emit rate (issue #602).
	EventlogQueue prometheus.Gauge
	// ConfirmedPositionAge is how long the minimum committed position has been
	// stuck: a rising age with a live pump is a silent stall (issue #602).
	ConfirmedPositionAge prometheus.Gauge
	// StagedCycles is the number of staged cycles still accumulating or
	// waiting to commit: a growing count is a staged backlog (issue #602).
	StagedCycles prometheus.Gauge
	// KafkaRecordsSkipped counts records a Kafka decoder rejected and
	// onDecodeError: skip dropped (issue #602). A Counter: the coordinator
	// adds the delta of the reader's DecodeErrors() on each poll.
	KafkaRecordsSkipped prometheus.Counter

	// Worker.
	RowsWritten      *prometheus.CounterVec
	CommitDuration   *prometheus.HistogramVec
	CommitLatencyMs  *prometheus.GaugeVec
	CommitFailures   *prometheus.CounterVec
	EqualityDeletes  *prometheus.CounterVec
	SnapshotProgress *prometheus.GaugeVec
	DroppedByWindow  *prometheus.CounterVec
	DeletesDropped   *prometheus.CounterVec

	// Enrich.
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

	m.LagSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "urutau_coordinator_lag_seconds", Help: "reader-to-worker lag in seconds, per table."},
		[]string{"table"})
	// PendingBatches is the table's outstanding work (queued + in-flight +
	// open staged cycles): the direct backlog signal a load-based scaler
	// wants. LagSeconds alone only rises when commits STALL, so a backlog
	// the workers are still draining never moves it (issue #298).
	m.PendingBatches = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "urutau_coordinator_pending_batches", Help: "uncommitted batches the table owes, per table."},
		[]string{"table"})
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
	m.SourceTruncates = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "urutau_source_truncates_total", Help: "TRUNCATEs seen on the stream and not propagated to the sink, per source and table."},
		[]string{"source", "table"})
	m.SourceDestructiveDDL = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "urutau_source_destructive_ddl_total", Help: "destructive DDL (DROP/ALTER/RENAME) seen on the stream and not propagated, per source, kind and table."},
		[]string{"source", "kind", "table"})
	m.EventlogQueue = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "urutau_coordinator_eventlog_queue", Help: "events waiting for the run-trail flusher."})
	m.ConfirmedPositionAge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "urutau_coordinator_confirmed_position_age_seconds", Help: "seconds since the confirmed (minimum committed) position last advanced."})
	m.StagedCycles = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "urutau_coordinator_staged_cycles", Help: "staged cycles accumulating or waiting to commit."})
	m.KafkaRecordsSkipped = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "urutau_kafka_records_skipped_total", Help: "Kafka records a decoder rejected and onDecodeError: skip dropped."})

	m.RowsWritten = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "urutau_worker_rows_written_total", Help: "rows written per table and op."},
		[]string{"table", "op"})
	m.CommitDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "urutau_worker_commit_duration_seconds",
		Help:    "commit latency per table.",
		Buckets: prometheus.DefBuckets},
		[]string{"table"})
	m.CommitLatencyMs = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "urutau_worker_commit_latency_ms", Help: "last commit latency per table."},
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

	reg.MustRegister(m.LagSeconds, m.PendingBatches, m.InflightBytes, m.WorkerResets, m.CommitsTotal, m.EventsDecoded)
	reg.MustRegister(m.SourceTruncates, m.SourceDestructiveDDL, m.EventlogQueue, m.ConfirmedPositionAge, m.StagedCycles, m.KafkaRecordsSkipped)
	reg.MustRegister(m.RowsWritten, m.CommitDuration, m.CommitLatencyMs, m.CommitFailures, m.EqualityDeletes, m.SnapshotProgress, m.DroppedByWindow, m.DeletesDropped)
	reg.MustRegister(m.EnrichDropped, m.EnrichEvicted)
	reg.MustRegister(m.IcebergCompactionRuns, m.IcebergCompactionFilesRemoved, m.IcebergCompactionFilesAdded,
		m.IcebergCompactionBytesBefore, m.IcebergCompactionBytesAfter)
	reg.MustRegister(m.IcebergSnapshotExpiryRuns, m.IcebergSnapshotExpirySnapshots)
	reg.MustRegister(m.IcebergOrphanCleanupRuns, m.IcebergOrphanCleanupFiles, m.IcebergOrphanCleanupBytes)
	return m
}

// Handler builds the /metrics (+ optional /statusz) mux, so a caller can add
// its own routes (the dashboard) before serving.
func (m *Metrics) Handler(encoder func(w http.ResponseWriter, r *http.Request)) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{}))
	if encoder != nil {
		mux.HandleFunc("/statusz", encoder)
	}
	for _, route := range debugRoutes {
		route(mux)
	}
	return mux
}

// debugRoutes are extra handlers a test build mounts (pprof_debug.go, only
// with the faultinject tag of the race e2e image); none in a release build.
var debugRoutes []func(*http.ServeMux)

// Serve exposes /metrics (and /statusz) on addr, blocking until the server
// stops. A caller with extra routes builds a mux via Handler and calls ServeMux.
func (m *Metrics) Serve(addr string, encoder func(w http.ResponseWriter, r *http.Request)) error {
	return ServeMux(addr, m.Handler(encoder))
}

// NewServer builds the metrics/dashboard http.Server for addr. Unlike the
// ServeMux convenience, it hands back the server so the caller can Shutdown it
// on run return instead of leaking the listener (issue #495).
func NewServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
}

// ServeMux runs an http.Server with the given handler on addr until it stops.
func ServeMux(addr string, handler http.Handler) error {
	return NewServer(addr, handler).ListenAndServe()
}

// Registry exposes the underlying registry (for tests).
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

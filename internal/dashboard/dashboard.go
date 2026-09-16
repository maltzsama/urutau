// Package dashboard serves the coordinator's embedded monitoring UI and its
// JSON API. It reads coordinator state through the State interface (the
// coordinator implements it), so the dashboard never imports the coordinator —
// the same contract direction the rest of the orchestration keeps.
//
// The UI is a single embedded SPA (Pico.css + Alpine.js + Chart.js, all
// vendored, no CDN) served from the same HTTP server as /metrics and /statusz.
package dashboard

// PipelineSummary is the header/overview data.
type PipelineSummary struct {
	Pipeline           string `json:"pipeline"`
	RunID              string `json:"run_id"`
	SourceKind         string `json:"source_kind"`
	SinkType           string `json:"sink_type"`
	Tables             int    `json:"tables"`
	Workers            int    `json:"workers"`
	StartedAt          string `json:"started_at,omitempty"`
	UptimeS            int64  `json:"uptime_s"`
	Status             string `json:"status"`
	SnapshotActive     bool   `json:"snapshot_active"`
	MaintenanceEnabled bool   `json:"maintenance_enabled"`
}

// TableStatus is one stream (source → target) as the Streams view shows it.
type TableStatus struct {
	Source           string       `json:"source"`
	Target           string       `json:"target"`
	WriteMode        string       `json:"write_mode"`
	Position         string       `json:"position,omitempty"`
	LagS             float64      `json:"lag_s"`
	RowsTotal        int64        `json:"rows_total"`
	RowsRate         float64      `json:"rows_rate"`
	Commits          int64        `json:"commits"`
	CommitFailures   int64        `json:"commit_failures"`
	EqualityDeletes  int64        `json:"equality_deletes"`
	DeletesDropped   int64        `json:"deletes_dropped"`
	SnapshotProgress float64      `json:"snapshot_progress"`
	Maintenance      *Maintenance `json:"maintenance,omitempty"`
}

// Maintenance is the per-stream Iceberg housekeeping block.
type Maintenance struct {
	Compaction *CompactionStatus `json:"compaction,omitempty"`
	Expiry     *ExpiryStatus     `json:"expiry,omitempty"`
	Orphan     *OrphanStatus     `json:"orphan,omitempty"`
}

type CompactionStatus struct {
	Runs         int64  `json:"runs"`
	LastRun      string `json:"last_run,omitempty"`
	FilesRemoved int64  `json:"files_removed"`
	FilesAdded   int64  `json:"files_added"`
	BytesBefore  int64  `json:"bytes_before"`
	BytesAfter   int64  `json:"bytes_after"`
}

type ExpiryStatus struct {
	Runs             int64  `json:"runs"`
	LastRun          string `json:"last_run,omitempty"`
	SnapshotsRemoved int64  `json:"snapshots_removed"`
}

type OrphanStatus struct {
	Runs         int64  `json:"runs"`
	LastRun      string `json:"last_run,omitempty"`
	FilesDeleted int64  `json:"files_deleted"`
	BytesFreed   int64  `json:"bytes_freed"`
}

// WorkerStatus is one worker as the Workers view shows it.
type WorkerStatus struct {
	Name          string            `json:"name"`
	Status        string            `json:"status"`
	Epoch         uint64            `json:"epoch"`
	Tables        []string          `json:"tables"`
	LastAckS      float64           `json:"last_ack_s"`
	InflightBytes int64             `json:"inflight_bytes"`
	Committed     map[string]string `json:"committed,omitempty"`
}

// State is what the dashboard reads from the coordinator. The coordinator
// implements it; the dashboard never imports the coordinator.
type State interface {
	Summary() PipelineSummary
	Tables() []TableStatus
	Workers() []WorkerStatus
	// Cancel terminates the pipeline (graceful shutdown of every worker).
	Cancel() error
	// RestartWorker resets one worker's session (epoch bump + supervisor reset).
	RestartWorker(name string) error
}

// Package coordinator drives the source side of the split pipeline: it owns
// the replication reader and the DBLog snapshot, serves the control plane
// (Session/Assignment) and the Arrow Flight data plane, and streams change
// batches to the connected workers. A table maps to one or more worker
// groups (spec.Tables[].Workers.Number; nil or <=1 means a single group, N
// splits the table's primary-key domain into N contiguous ranges — see
// spec.Table.WorkerGroupNames), each group gets its own Flight queue, and a
// batch is routed — whole, or split by primary-key range across the
// table's partitions — to the worker group(s) that own its rows.
package coordinator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/driver"
	"github.com/maltzsama/urutau/internal/dashboard"
	"github.com/maltzsama/urutau/internal/enrich"
	"github.com/maltzsama/urutau/internal/eventlog"
	"github.com/maltzsama/urutau/internal/faultinject"
	"github.com/maltzsama/urutau/internal/grpctls"
	"github.com/maltzsama/urutau/internal/logging"
	"github.com/maltzsama/urutau/internal/observability"
	"github.com/maltzsama/urutau/internal/snapshot"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
	"google.golang.org/grpc/keepalive"
)

// Config tunes the coordinator for one pipeline.
type Config struct {
	Spec       *spec.Spec
	ListenAddr string // gRPC + Flight listen address ("host:port" or ":0")

	// TLS is the mutual-TLS material for the control plane. Empty means
	// plaintext — a loud warning is logged, because the Assignment carries
	// the source DSN (CD-1b).
	TLS grpctls.Config

	ChunkSize     int
	WindowTimeout time.Duration
	CaughtUpPoll  time.Duration
	// SnapshotChunkTimeout bounds one snapshot chunk round-trip (ChunkRequest
	// sent → ChunkReady received → gated rows flushed). It is the snapshot
	// watchdog: without it, a worker that attaches but stops draining wedges
	// the run forever (the supervisor does not run during the snapshot). It
	// must exceed WindowTimeout, or a slow-but-healthy catch-up would look
	// wedged; the default is 10m, raised above WindowTimeout when that is set
	// higher. Zero means the default.
	//
	// There is no CLI flag or spec field for it today: the default IS the
	// operator-facing behavior, and a deployment that needs a different value
	// sets this field programmatically.
	SnapshotChunkTimeout time.Duration
	ServerID             uint32
	Heartbeat            time.Duration
	// MaxParallelChunks would cap concurrent chunk SELECTs during snapshot.
	// The snapshot is currently strictly sequential (one chunk in flight,
	// waitChunkReady blocks), so the knob is validated but reserved — it is
	// not yet wired to concurrency.
	MaxParallelChunks int

	// FlowTotalBytes is the process-wide ceiling on serialized batch bytes
	// in flight (queued or sent, unacked). FlowPerWorkerMin is the floor
	// that keeps a slow worker from starving. Defaults 512Mi / 16Mi.
	FlowTotalBytes   int64
	FlowPerWorkerMin int64

	// Eventlog is optional: when set, the coordinator writes its per-run
	// audit trail (job_started, snapshots, commits, terminal) to S3.
	Eventlog *eventlog.Config

	// Checkpoint is optional: async position manifests to S3 (design §6).
	// Convenience only — the Iceberg table property is the source of truth.
	Checkpoint *CheckpointConfig

	// WaitWorker bounds how long the coordinator waits for every expected
	// worker session before failing the boot.
	WaitWorker time.Duration

	// Supervision: a worker that stops acking past AckTimeout is reset
	// (epoch++ and session cancel). Resets within ResetWindow beyond
	// MaxResets terminate the job. Defaults 30s / 5 / 15m.
	AckTimeout  time.Duration
	MaxResets   int
	ResetWindow time.Duration

	// ScaleDrainTimeout bounds how long a re-slice waits for a table's
	// open staged cycles, and for a removed owner's in-flight batches, to
	// drain. Past it the owner is forced out and its range is replayed by
	// the inheriting owner — safe because the writes are idempotent
	// upserts (issue #312). Default 60s.
	ScaleDrainTimeout time.Duration
	// MaxWorkers caps the partitions one table may be scaled to when the
	// table declares no cap of its own. Default 32.
	MaxWorkers int
	// OnReady, when set, receives the running coordinator once its
	// routing is published. It is how a scaler reaches ScaleTable
	// in-process (issue #312); the operator wires the HTTP action to it.
	OnReady func(*Coordinator)

	// MetricsAddr serves /metrics (Prometheus), /statusz (live state), and the
	// dashboard (issue #97). Empty disables the endpoint.
	MetricsAddr string

	// LogBuffer is the coordinator logger's in-memory tail, served by the
	// dashboard's Logs view. Nil disables that view.
	LogBuffer *logging.Buffer

	Logger *slog.Logger
}

// workerQueueCap bounds one worker's in-flight batches — the structural
// backpressure hop of design §1.1 (workerCh cap 64).
const workerQueueCap = 64

// defaultSnapshotChunkTimeout is the snapshot watchdog's default when the
// config is silent (see Config.SnapshotChunkTimeout).
const defaultSnapshotChunkTimeout = 10 * time.Minute

// queuedBatch is one serialized batch waiting for the Flight stream.
type queuedBatch struct {
	id   uint64 // the inflight batch id, to correlate an ack with the sent list
	body []byte // complete Arrow IPC stream
	meta []byte // BatchMeta proto
}

// Coordinator runs the source pipeline and serves workers.
type Coordinator struct {
	cfg Config
	log *slog.Logger

	src       source.Source
	qsrc      source.QuerySource
	refs      []source.TableRef
	snk       sink.Sink
	canonical map[string]core.Schema // per-source canonical schema for typed wire format

	// maint schedules the ephemeral maintenance workers (issue #96). Nil
	// unless maintenance is enabled in the spec.
	maint *maintenanceScheduler

	// Worker registry: groups resolved at boot from the spec, one queue
	// and one ticket each. route maps every target table to its N
	// partition owners, in partition order (route[target][i] owns
	// partitionRanges[target][i]) — a table with Workers<=1 has exactly
	// one entry, an unbounded range, matching today's single-worker
	// behavior byte for byte.
	// routing is the live partition layout, swapped atomically by a
	// re-slice (issue #312). A reader loads the snapshot ONCE and routes a
	// whole batch by it, so a flip never splits one batch across two
	// layouts. Boot publishes the first snapshot; ScaleTable replaces it.
	routing atomic.Pointer[routing]
	// repart serializes re-slices: two concurrent scale events would each
	// read-modify-write the snapshot and one would be lost.
	repart repartitioner
	// chunkers is the per-table chunker built at boot: reused for the
	// snapshot so a partitioned table's chunker (forced to key-based
	// chunking by Partitions) is the one the snapshot bounds come from.
	// A re-slice builds one on demand for a table booted unpartitioned, so
	// chunkersMu guards the map against the snapshot goroutine's read.
	chunkers   map[string]source.ChunkSource
	chunkersMu sync.Mutex
	workers    map[string]*workerState
	byTicket   map[string]*workerState
	budget     *flowBudget
	// index is the per-worker position index. It was written only at boot
	// until live re-slicing (issue #312) made registerOwner write it at
	// runtime, so it now needs its own lock: the readers (inFlight, enqueueTo,
	// onAck, the dashboard) deliberately run outside c.mu to avoid the
	// c.mu/supervisor lock-order deadlock (audit #3). indexMu is taken alone
	// by readers and as c.mu -> indexMu by registerOwner/unregisterOwner, so
	// the order is consistent and never cycles.
	indexMu sync.RWMutex
	index   map[string]*positionIndex

	// Kubernetes worker provisioning (issue #298): the in-cluster client is
	// built lazily and cached (workerClientset). workerK8s is set at boot
	// when the operator rendered worker pod templates — the switch that
	// turns provisioning and the replica reconcile loop on. Both stay false
	// for a pipeline that never sets spec.image, so the coordinator makes
	// zero Kubernetes API calls.
	k8sMu     sync.Mutex
	k8sClient kubernetes.Interface
	k8sNS     string
	k8sOwner  metav1.OwnerReference
	workerK8s bool
	// scaleRetryAfter suppresses one table's replica reconcile until the
	// mapped time after a failed scale: each attempt pauses the table's input
	// for the whole drain timeout, so retrying every tick would starve the
	// backlog the drain is waiting on (issue #298). Keyed by table target so
	// one table's failure does not stall another's. Read and written only by
	// the reconcile loop.
	scaleRetryAfter map[string]time.Time

	// runCtx outlives the helper goroutines that need cancellation (the
	// wireRelay) but are called outside run's select.
	runCtx context.Context

	runID string // run-id of this boot (assignment + eventlog, §5.6.1)

	ev *eventlog.Run

	ready       chan struct{} // one send per attached session
	sessionErrs chan error    // first exit wins
	// snapshotActive is true while the snapshot phase runs. A worker
	// session lost during it (reset OR death) must fail the run fast: the
	// worker's in-memory window died with it, so the protocol would either
	// wait out AckTimeout/MaxResets (15min) or let a stale ChunkReady from
	// the old generation satisfy the wait against an empty window — a
	// silently incomplete snapshot.
	snapshotActive atomic.Bool
	batchSeq       atomic.Uint64 // monotonic BatchMeta.batch_id
	mu             sync.Mutex    // guards session attach/detach

	// WK-001 C5: open staged cycles (a partitioned table's sub-batches
	// grouped by binlog batch). stagedLocks serializes CommitStaged per
	// table so two cycles of the same table never race on the catalog.
	staged      *stagedCycles
	stagedLocks map[string]*sync.Mutex
	stagedMu    sync.Mutex

	// The latest position sent per table and the worker its latest snapshot
	// window went to: where a table's snapshot-done marker goes (#428).
	sentMu     sync.Mutex
	lastSent   map[string]string
	lastWindow map[string]*workerState

	// DBLog window gate (design §3.1): while a chunk's SELECT is in flight
	// on the worker, live events of that table are held here instead of
	// being shipped — a live event racing ahead of the chunk's rows would
	// miss the window delete and duplicate the row. On ChunkReady the held
	// events are released InWindow-tagged, then the Closes marker.
	//
	// Keyed by gateKey(target, partition), not just target: a partitioned
	// table's N workers each run their own independent DBLog pass over
	// their own PK range concurrently, so N windows can be open on the
	// same table at once — one worker's chunk boundary must never gate
	// (or release) a live batch that belongs to a DIFFERENT partition of
	// the same table. An unpartitioned table (the overwhelming common
	// case) has exactly one key, gateKey(target, 0), and this collapses
	// to the previous single-window-per-table behavior exactly.
	gateMu  sync.Mutex
	gateOn  map[string]bool
	gateWin map[string]gateWindow // key -> target/partition, for gateHold's row->key routing
	// gateBuf holds source batches (live changes) per open window. Raw
	// pre-encode batches: released by flushWindow/closeWindow after they
	// are queued.
	gateBuf map[string][]*dataplane.Batch
	// gateDrain wakes a pump blocked on a full gate when flushWindow/
	// closeWindow drains it (audit #5: the gate was the only buffer without
	// a structural bound). One shared channel: any drain (of any window)
	// wakes every waiter, which re-checks its own window's state.
	gateDrain chan struct{}

	// paused holds a table whose re-slice is draining. The flip waits for the
	// table to owe nothing, which a continuously loaded table never reaches on
	// its own; pausing the input lets the queue drain, then the flip, then
	// resume. Keyed by target.
	//
	// A paused table's batches are BUFFERED (pauseBuf), not left to park the
	// pump: parking it would stall every other table's batches behind the
	// paused one in the reader channel (issue #343). resumeTable wakes the
	// pump to flush them under the new layout, in order.
	pausedMu  sync.Mutex
	paused    map[string]chan struct{}
	pauseBuf  map[string][]*dataplane.Batch
	pauseWake chan struct{}

	// chunkReady routes worker ChunkReady replies to the snapshot loop.
	chunkReady chan *pb.ChunkReady

	// confirmed tracks the latest position each WORKER durably committed
	// (from worker Acks). The minimum across workers is reported to the
	// source so its retention never advances past uncommitted data.
	//
	// Keyed by worker, not by table: a partitioned table (workers>1) has N
	// workers committing different primary-key ranges, and a per-table key
	// let the last acker overwrite the others — advancing the Postgres slot
	// past WAL a lagging partition had not read (WK-001 §2.2). This is safe
	// only because one worker serves exactly one table (WorkerGroupNames
	// embeds the target); a shared worker would fold two tables' positions
	// into one minimum. See TestWorkerGroupNamesEmbedTarget.
	confirmedMu sync.Mutex
	confirmed   map[string]position.Position

	// bootCommitted is each table's committed cdc.position read at boot
	// (resumeFrom). A crash recovery replays every table from the minimum
	// across tables, so a table ahead of it sees batches it already holds;
	// its workers skip those as covered (worker.batchReceiver.covered) and
	// never stage them. Written once before the pump starts, read-only after.
	bootCommitted map[string]position.Position

	cp         *checkpoint
	supervisor *supervisor
	terminate  chan error
	metrics    *observability.Metrics

	// Dashboard (issue #97): the recent-events ring, the read-only API handler,
	// the run's start time, and the per-table/worker aggregates the API serves.
	dashEvents *dashboard.Events
	dash       *dashboard.Handler
	startedAt  time.Time

	statsMu    sync.Mutex
	tableStats map[string]*tableStats
	maintStats map[string]map[string]*maintStats
	lastAck    map[string]time.Time
}

// workerState is one worker group's slice of the pipeline: its tables, its
// Flight queue, and — once it connects — its control surface.
type workerState struct {
	name   string
	refs   []source.TableRef
	queue  chan queuedBatch
	ticket []byte

	out      chan *pb.CoordinatorMessage // attached by Session
	control  pb.UrutauControl_ControlServer
	attached bool
	// hadSession records that the worker's session has attached at least once.
	// A worker that was attached and is now detached (its Pod deleted) can
	// never drain its queue; the supervisor uses this to distinguish that from
	// a worker that simply never attached yet (issue #372).
	hadSession bool
	epoch      uint64 // last accepted epoch (guards stale Hellos)
	cancel     context.CancelFunc

	// sent holds the batches popped from the queue and delivered (or whose
	// Send was attempted) but not yet acked, in send order. On session loss
	// the next DoGet redelivers them before draining the queue, so a batch
	// whose ack was lost is replayed rather than dropped — at-least-once, the
	// data-loss fix for issue #235. Pruned by the ack that covers each id.
	sentMu sync.Mutex
	sent   []queuedBatch

	// committed: target table → position the worker reported after its last
	// commit. Refreshed on every ready Hello (design §5.6.1).
	committed map[string]string

	// activeGet guards one DoGet stream per worker. Two concurrent streams
	// on the same ticket would each pop the queue, splitting batches across
	// readers — and each would append to the sent list, duplicating them on
	// the next redelivery.
	activeGet atomic.Bool
}

// Run boots the pipeline and blocks until ctx is cancelled or a terminal
// error occurs.
func Run(ctx context.Context, cfg Config) error {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	// Fail closed: a plaintext control plane sends the source DSN in the
	// clear. Refuse at boot unless the caller explicitly allowed it — a
	// warning on the wire is not a control (CD-1b).
	if err := cfg.TLS.RequireTLS(); err != nil {
		return fmt.Errorf("coordinator: %w", err)
	}
	// The spec's source.serverId wins over cfg.ServerID when declared —
	// the pipeline's own server id travels with it; cfg.ServerID is only
	// a default for when the spec is silent. Spec.Validate (already run
	// by every caller that loaded this spec from YAML) rejects a
	// non-numeric serverId, so this only fails for a Spec built
	// programmatically with a bad value.
	if cfg.Spec != nil {
		serverID, err := cfg.Spec.ResolveServerID(cfg.ServerID)
		if err != nil {
			return err
		}
		cfg.ServerID = serverID
	}
	// Defaults land before the struct captures cfg (audit #16): a later read
	// of c.cfg must never see the zero flow knobs.
	if cfg.FlowTotalBytes <= 0 {
		cfg.FlowTotalBytes = 512 << 20
	}
	if cfg.FlowPerWorkerMin <= 0 {
		cfg.FlowPerWorkerMin = 16 << 20
	}
	// The snapshot watchdog must outlast WaitCaughtUp's own window timeout
	// (default 5m), or a slow-but-healthy catch-up would look like a wedged
	// worker and fail the run.
	if cfg.SnapshotChunkTimeout <= 0 {
		cfg.SnapshotChunkTimeout = defaultSnapshotChunkTimeout
	}
	if cfg.WindowTimeout > 0 && cfg.SnapshotChunkTimeout <= cfg.WindowTimeout {
		cfg.SnapshotChunkTimeout = cfg.WindowTimeout + 5*time.Minute
	}
	c := &Coordinator{
		cfg:         cfg,
		log:         cfg.Logger,
		workers:     map[string]*workerState{},
		byTicket:    map[string]*workerState{},
		index:       map[string]*positionIndex{},
		ready:       make(chan struct{}, 1024),
		sessionErrs: make(chan error, 1024),
		chunkReady:  make(chan *pb.ChunkReady, 1024),
		gateOn:      map[string]bool{},
		gateWin:     map[string]gateWindow{},
		gateBuf:     map[string][]*dataplane.Batch{},
		paused:      map[string]chan struct{}{},
		pauseBuf:    map[string][]*dataplane.Batch{},
		pauseWake:   make(chan struct{}, 1),
		gateDrain:   make(chan struct{}),
		confirmed:   make(map[string]position.Position),
		staged:      newStagedCycles(),
		stagedLocks: map[string]*sync.Mutex{},
	}
	c.budget = newFlowBudget(cfg.FlowTotalBytes, cfg.FlowPerWorkerMin)
	c.runID = time.Now().UTC().Format("2006-01-02T15:04:05Z") + "-" + randSuffix(6)
	c.supervisor = newSupervisor(c)
	c.terminate = make(chan error, 1)
	c.startedAt = time.Now()
	c.dashEvents = dashboard.NewEvents(1000)
	c.tableStats = map[string]*tableStats{}
	c.maintStats = map[string]map[string]*maintStats{}
	c.lastAck = map[string]time.Time{}
	if cfg.MetricsAddr != "" {
		c.metrics = observability.New()
		mux := c.metrics.Handler(c.statusz)
		// A nil *logging.Buffer must stay a nil interface, or the dashboard
		// would call Tail on a nil pointer.
		var logSrc dashboard.LogSource
		if cfg.LogBuffer != nil {
			logSrc = cfg.LogBuffer
		}
		c.dash = dashboard.New(dashState{c}, c.dashEvents, logSrc, c.log)
		c.dash.Register(mux)
		// Every new log line is pushed to the SSE subscribers.
		if cfg.LogBuffer != nil {
			cfg.LogBuffer.SetOnAppend(c.dash.PublishLog)
		}
		go func() {
			// A busy port silently disables observability otherwise — say so.
			if err := observability.ServeMux(cfg.MetricsAddr, mux); err != nil {
				c.log.Warn("coordinator: metrics server stopped", "addr", cfg.MetricsAddr, "err", err)
			}
		}()
	}
	return c.run(ctx)
}

func (c *Coordinator) run(ctx context.Context) error {
	c.runCtx = ctx

	if cfg := c.cfg.Eventlog; cfg != nil {
		ec := *cfg
		// Apply the shared key convention: the trail lives under the
		// pipeline name so it stays discoverable after the CR is deleted.
		if ec.Pipeline == "" && c.cfg.Spec != nil {
			ec.Pipeline = c.cfg.Spec.Pipeline
		}
		ev, err := eventlog.New(ctx, ec)
		if err != nil {
			return fmt.Errorf("coordinator: eventlog: %w", err)
		}
		c.ev = ev
		defer func() {
			if err := ev.Close(); err != nil {
				c.log.Warn("coordinator: eventlog close", "err", err)
			}
		}()
		if err := c.emit(eventlog.KindJobStarted, map[string]any{
			"pipeline": c.cfg.Spec.Pipeline,
			"source":   c.cfg.Spec.Source.Kind,
		}); err != nil {
			c.log.Warn("coordinator: eventlog emit", "err", err)
		}
	}

	// Reject a bootstrap block this mode ignores before touching the source:
	// the error must not hide behind a connection or introspection failure.
	// Discovered tables (source.ExpandTables) never carry one, so checking
	// the declared list is enough.
	for _, t := range c.cfg.Spec.Tables {
		if err := requireSnapshotBootstrap(t); err != nil {
			return err
		}
	}

	// Source adapter, query connection, introspection — identical to the
	// collapsed runner; only the worker side differs.
	src, err := driver.OpenSource(c.cfg.Spec, source.Runtime{
		ServerID:  c.cfg.ServerID,
		Heartbeat: c.cfg.Heartbeat,
		Logger:    c.log,
	})
	if err != nil {
		return err
	}
	c.src = src
	// QuerySource is optional: it backs NewChunker, the snapshot/re-slice
	// chunking surface, which only a relational, snapshot-capable source
	// (Capabilities.Snapshot or ChunkQuery) ever calls. A source with
	// neither, like Kafka, has no SQL query connection at all (kafka.Source's
	// own doc comment) and must boot with c.qsrc == nil; every call site
	// gates on the source's capabilities before touching it (issue #394).
	if qsrc, ok := src.(source.QuerySource); ok {
		c.qsrc = qsrc
		defer func() { _ = qsrc.CloseQuery() }()
	}
	// The parallel-chunk setting may not exceed the ceiling the source
	// driver declares — fail fast at boot, not mid-snapshot.
	if err := driver.ValidateParallelism(c.cfg.Spec.Source.Kind, c.cfg.MaxParallelChunks); err != nil {
		return fmt.Errorf("coordinator: %w", err)
	}

	// A discovery pipeline lists no tables: the source enumerates them now.
	// The write-back must land BEFORE the partition-range loop below, which
	// indexes c.cfg.Spec.Tables positionally against refs — a second list
	// would desync the ranges from the tables.
	tables, err := source.ExpandTables(ctx, src, c.cfg.Spec)
	if err != nil {
		return fmt.Errorf("coordinator: %w", err)
	}
	c.cfg.Spec.Tables = tables

	refs := make([]source.TableRef, 0, len(c.cfg.Spec.Tables))
	// canonical holds the WIRE shape (source types; the workers encode it
	// and the sink casts). resolvedSchemas holds the sink's target shape
	// (cast types + metadata columns) for DDL.
	canonical := make(map[string]core.Schema, len(c.cfg.Spec.Tables))
	resolvedSchemas := make(map[string]core.Schema, len(c.cfg.Spec.Tables))
	tableBySource := make(map[string]spec.Table, len(c.cfg.Spec.Tables))
	for _, t := range c.cfg.Spec.Tables {
		ref, srcSchema, srcWarns, err := src.Introspect(ctx, t)
		if err != nil {
			return err
		}
		c.surfaceWarnings(ref.Source, srcWarns)
		cast, err := coreCastOf(t)
		if err != nil {
			return err
		}
		res, warns, err := core.ResolveSchema(srcSchema, cast, t.Metadata)
		if err != nil {
			return err
		}
		c.surfaceWarnings(ref.Source, warns)
		refs = append(refs, ref)
		// Reference columns join BOTH shapes: the assignment/wire schema
		// the worker encodes against and the resolved schema EnsureTable
		// creates the table from. Without it, the first enriched batch
		// carries a column the table lacks and every sink silently drops
		// it. Registered decision: nullable strings until CR-069 resolves
		// real types.
		//
		// The coordinator has no Stage of its own (it forwards enrich
		// declarations to the worker, which runs the real, long-lived
		// join). For a wildcard select, the real column names are only
		// known once the reference query runs — LoadWildcardColumns runs
		// it synchronously here, at boot, so canonical/resolvedSchemas are
		// correct before EnsureTable and before any worker session
		// connects (#56). This is a second, short-lived query against the
		// reference beyond the worker's own load — an accepted,
		// disclosed cost of the coordinator/worker split (no shared
		// connection between the two processes).
		dests, err := enrich.LoadWildcardColumns(ctx, t.Enrich)
		if err != nil {
			return fmt.Errorf("coordinator: %s: enrich: %w", t.Source, err)
		}
		canonical[t.Source] = enrich.AddColumns(core.WireSchema(srcSchema, res), dests)
		resolvedSchemas[t.Source] = enrich.AddColumns(res, dests)
		tableBySource[t.Source] = t
	}
	c.refs = refs
	c.canonical = canonical

	// Incremental mode (#157) is implemented in the collapsed runner only.
	// Reject it here rather than treat an incremental table as CDC and open a
	// slot for it.
	for _, t := range c.cfg.Spec.Tables {
		if t.Mode == spec.ModeIncremental {
			return fmt.Errorf("coordinator: %s: incremental mode is not supported in distributed mode yet — run this table in the collapsed runner", t.Target)
		}
	}

	// Fail loud on an upsert table with no key BEFORE resolving or
	// provisioning worker groups: a discovered keyless table would otherwise
	// create worker Deployments and then abort, leaving orphaned resources
	// across repeated boot failures.
	for _, ref := range refs {
		if err := dataplane.RequireUpsertKey(ref.Target, ref.PrimaryKey, tableBySource[ref.Source].WriteMode.ChangeMode()); err != nil {
			return fmt.Errorf("coordinator: %w", err)
		}
	}

	// Resolve worker groups: one per partition, derived
	// "<pipeline>-<target>-<index>" name (spec.Table.WorkerGroupNames) —
	// there is no operator-chosen worker name, so two tables can never
	// collide on one (each name embeds its own unique target).
	//
	// A table's partition RANGES are computed here, at boot, for every
	// table — not only the ones needing a snapshot — because live-stream
	// routing (enqueueBatch) depends on them from the first batch, resume
	// or not. Workers<=1 short-circuits to a single unbounded range with
	// no chunker query at all, so this is a no-op for every unpartitioned
	// table (the overwhelming common case today).
	bootRanges := make(map[string][]source.Chunk, len(c.cfg.Spec.Tables))
	bootOwners := make(map[string][]*workerState, len(c.cfg.Spec.Tables))
	c.chunkers = make(map[string]source.ChunkSource, len(c.cfg.Spec.Tables))
	// workerTarget maps every derived worker group name back to the table
	// target it belongs to — provisionWorkers uses it to pick that
	// table's own worker Pod template (one per table, rendered by the
	// operator into the coordinator's ConfigMap).
	workerTarget := make(map[string]string, len(c.cfg.Spec.Tables))
	for i, t := range c.cfg.Spec.Tables {
		if err := requirePartitionKey(t, refs[i]); err != nil {
			return err
		}
		names := t.WorkerGroupNames(c.cfg.Spec.Pipeline)
		ranges, chunker, err := c.resolvePartitionRanges(ctx, t, refs[i])
		if err != nil {
			return fmt.Errorf("coordinator: %s: %w", t.Source, err)
		}
		if err := requireOrderableRanges(t, canonical[t.Source], refs[i].PrimaryKey, ranges); err != nil {
			return err
		}
		if len(ranges) != len(names) {
			return fmt.Errorf("coordinator: %s: resolved %d partition ranges for %d worker groups", t.Source, len(ranges), len(names))
		}
		bootRanges[t.Target] = ranges
		c.chunkers[t.Target] = chunker

		owners := make([]*workerState, len(names))
		for p, name := range names {
			w, ok := c.workers[name]
			if !ok {
				// A 128-bit random ticket colliding is ~0, but the
				// queue-lookup map is keyed by it — a collision would
				// silently orphan a worker's stream, so regenerate
				// rather than assume.
				for {
					ticket := randTicket()
					if _, taken := c.byTicket[string(ticket)]; taken {
						continue
					}
					w = &workerState{
						name:   name,
						queue:  make(chan queuedBatch, workerQueueCap),
						ticket: ticket,
					}
					c.byTicket[string(ticket)] = w
					break
				}
				c.workers[name] = w
				c.setIndex(name, newPositionIndex(c.runID))
			}
			w.refs = append(w.refs, refs[i])
			owners[p] = w
			workerTarget[name] = t.Target
		}
		bootOwners[t.Target] = owners
	}
	// Publish the boot layout once: every runtime reader loads this
	// snapshot, and a re-slice swaps in a successor (issue #312).
	c.publishRouting(&routing{owners: bootOwners, ranges: bootRanges})
	if c.cfg.OnReady != nil {
		c.cfg.OnReady(c)
	}
	c.workerK8s = workerPodTemplateAvailable(workerTarget)
	if err := c.provisionWorkers(ctx, workerTarget); err != nil {
		return fmt.Errorf("coordinator: %w", err)
	}
	for _, w := range c.workers {
		c.log.Info("coordinator worker group", "worker", w.name, "tables", len(w.refs))
		if err := c.emit(eventlog.KindWorkerCreated, map[string]any{
			"worker": w.name,
			"tables": tableNames(w.refs),
		}); err != nil {
			c.log.Warn("coordinator: eventlog emit", "err", err)
		}
	}
	if cfg := c.cfg.Checkpoint; cfg != nil {
		cp, err := newCheckpoint(ctx, *cfg)
		if err != nil {
			return fmt.Errorf("coordinator: checkpoint: %w", err)
		}
		c.cp = cp
		go cp.run(ctx, c.runID, c.indexSnapshot(), c.log)
		c.log.Info("coordinator checkpoint", "uri", cfg.URI, "interval", cp.interval)
	}

	// Sink + tables: the coordinator owns DDL.
	snk, err := driver.OpenSink(ctx, c.cfg.Spec)
	if err != nil {
		return fmt.Errorf("coordinator: catalog: %w", err)
	}
	c.snk = snk
	// The sink is opened on every error path between here and the defers;
	// Close on every exit, not just the happy one (audit #14).
	defer func() { _ = c.snk.Close() }()

	// (2) A partitioned table needs a sink that can serve N concurrent
	// writers. Checked by CAPABILITY, not by sink type name: a plugin
	// registers whatever name it wants. The capability must be declared
	// false until the sink's concurrent path is actually built, so this
	// refuses the boot instead of letting N writers corrupt one table.
	for _, t := range c.cfg.Spec.Tables {
		if err := requireConcurrentSink(t, c.snk); err != nil {
			return err
		}
	}

	for _, ref := range refs {
		tbl := tableBySource[ref.Source]
		// The cast policy must reach DDL: an empty policy here creates a
		// table whose types diverge from the collapsed runner's (audit #8).
		cast, err := coreCastOf(tbl)
		if err != nil {
			return err
		}
		mode := tbl.WriteMode.ChangeMode()
		if err := snk.EnsureTable(ctx, ref, resolvedSchemas[ref.Source], tbl.PartitionBy, cast, mode); err != nil {
			return fmt.Errorf("coordinator: ensure %s: %w", ref.Target, err)
		}
	}

	// Iceberg table maintenance (issue #96): the coordinator SCHEDULES
	// maintenance but does not run it in its own process. With Kubernetes
	// worker provisioning available it provisions an ephemeral maintenance
	// worker per table (from the table's own worker pod template) and pushes
	// the due operations to it over the control stream; the worker dies when
	// the pass is done, so no maintenance work competes with the
	// coordinator's routing/commit path. Without Kubernetes there is no
	// worker to launch — the collapsed runner's in-process schedule is the
	// only path there. Checked against the NEUTRAL sink.Maintainable
	// interface — never a concrete sink package, which internal/architecture's
	// TestOrchestrationConsumesContracts forbids the coordinator from
	// importing. Maintenance is Iceberg-only (spec.Validate rejects it on
	// any other sink type), so a sink that does not implement the capability
	// here is a bug, not a configuration to tolerate: fail loudly, exactly
	// like requireConcurrentSink above. Skipped entirely when Maintenance is
	// nil or disabled.
	if err := c.startMaintenance(refs, workerTarget); err != nil {
		return err
	}

	resume, needsSnapshot, err := c.resumeFrom(ctx, refs)
	if err != nil {
		return err
	}
	c.log.Info("coordinator resume", "from", position.StringOrNone(resume), "snapshot_tables", len(needsSnapshot))
	// Before any worker can commit: a crash from here on must find these
	// tables unfinished, whatever positions the stream commits to them.
	if err := c.markSnapshotsPending(ctx, needsSnapshot); err != nil {
		return err
	}

	// Baseline every worker's confirmed position to the run's resume point.
	// confirmedPosition() takes the min over the map, so a worker that has
	// not acked yet must be IN the map holding that min back — omitting it
	// let the source slot advance past data a worker had not committed
	// (WK-001 §2.2). resume is nil on a fresh boot, which correctly holds the
	// slot until every worker has committed at least once.
	c.confirmedMu.Lock()
	for name := range c.workers {
		c.confirmed[name] = resume
	}
	c.confirmedMu.Unlock()

	// Serve gRPC (control) + Flight (data) on one listener.
	lis, err := net.Listen("tcp", c.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("coordinator: listen: %w", err)
	}
	defer func() { _ = lis.Close() }()
	c.log.Info("coordinator listening", "addr", lis.Addr().String())

	opts := []grpc.ServerOption{
		// Keepalive agreement with the worker: MinTime ≤ client Time, else
		// the server GOAWAYs a healthy worker for pinging too much.
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    10 * time.Second,
			Timeout: 5 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: false,
		}),
		// Flight batches can be a full snapshot chunk; 128Mi covers the
		// default batching ceiling.
		grpc.MaxRecvMsgSize(128 << 20),
		grpc.MaxSendMsgSize(128 << 20),
	}
	if c.cfg.TLS.Enabled() {
		tlsOpt, err := c.cfg.TLS.ServerOption()
		if err != nil {
			return fmt.Errorf("coordinator: tls: %w", err)
		}
		opts = append(opts, tlsOpt)
		c.log.Info("coordinator: control plane mTLS enabled")
	} else {
		c.log.Warn("coordinator: control plane is PLAINTEXT — the Assignment carries the source DSN; set TLS cert/key/CA (running because --allow-insecure-control-plane was set)")
	}
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterUrutauControlServer(grpcServer, &controlServer{c: c})
	flight.RegisterFlightServiceServer(grpcServer, &flightServer{c: c})
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	// Wait for every expected worker session.
	wait := c.cfg.WaitWorker
	if wait <= 0 {
		wait = 2 * time.Minute
	}
	if err := c.waitWorkers(ctx, wait); err != nil {
		return err
	}

	// Reader + stream, then snapshot — the DBLog loop the collapsed runner
	// runs, routed over the wire instead of an in-process channel.
	rdr, err := c.src.Open(ctx, refs)
	if err != nil {
		return err
	}
	defer rdr.Close()
	// A source that can take the resolved schema (optional interface) gets
	// it now: the source boundary then gates on drift against the native
	// shape and encodes stable batches. Keyed by the TARGET the changes are
	// addressed to.
	if si, ok := rdr.(source.SchemaSetter); ok {
		byTarget := make(map[string]core.Schema, len(refs))
		for _, ref := range refs {
			if cs, ok := c.canonical[ref.Source]; ok {
				byTarget[ref.Target] = cs
			}
		}
		si.SetSourceSchemas(byTarget)
	}
	// The slot's confirmed point tracks the minimum position workers have
	// durably committed (their Acks), never the decode position — otherwise
	// a crash between decode and commit would lose the in-flight window.
	rdr.SetConfirmed(c.confirmedPosition)

	start := resume
	if start == nil {
		m, err := c.src.InitialPosition(ctx)
		if err != nil {
			return fmt.Errorf("coordinator: initial position: %w", err)
		}
		start = m
	}
	if err := rdr.Start(ctx, start); err != nil {
		return fmt.Errorf("coordinator: start stream: %w", err)
	}
	// Batch-native pump (G0/M4): the reader's batches are forwarded whole
	// and serialized once per batch — no decode back to changes, no
	// per-change one-row Flight batch. The FIFO queue preserves the wire
	// ordering the window protocol needs.
	out, streamErr := sourceBatches(ctx, rdr)
	// Lag grows between commits, so the gauge needs its own clock: setting it
	// on the ack path would pin it near zero after every commit and never let
	// it rise. Mirrors the dashboard's on-demand Tables(). Started only now:
	// it reads c.snk and c.cfg.Spec.Tables, which boot writes above, and a
	// loop started earlier raced them (the race image aborted a restarting
	// coordinator on it). Before the pump runs there is no lag to report.
	if c.metrics != nil {
		go c.lagLoop(ctx)
	}
	go c.pump(ctx, out)

	// The snapshot runs in its own goroutine: run's terminal select must
	// stay live underneath it. A worker dying mid-snapshot otherwise wedges
	// the run forever — the snapshot loop blocks on waitChunkReady, the
	// session error lands in sessionErrs, and nobody reads it (audit #1).
	snapCtx, snapCancel := context.WithCancel(ctx)
	defer snapCancel()
	c.snapshotActive.Store(true)
	snapDone := make(chan error, 1)
	go func() {
		defer c.snapshotActive.Store(false)
		defer snapCancel()
		defer close(snapDone)
		// A cancelled snapshot returns before closeWindow, leaving batches
		// held in an open gate. Release them when the phase ends (normally a
		// no-op — closeWindow already drained each partition) so shutdown does
		// not leak Arrow batches (issue #212). gateHold re-checks the window
		// under gateMu before appending, so clearing it here cannot race a
		// late gate.
		defer c.releaseAllGates()
		snapCfg := snapshot.SnapshotConfig{
			WindowTimeout: c.cfg.WindowTimeout,
			CaughtUpPoll:  c.cfg.CaughtUpPoll,
		}
		for _, ref := range needsSnapshot {
			c.log.Info("coordinator snapshot", "table", ref.Source)
			faultinject.At(faultinject.CoordinatorSnapshotTableStart, "table", ref.Target)
			if err := c.emit(eventlog.KindSnapshotStarted, map[string]any{"table": ref.Source}); err != nil {
				c.log.Warn("coordinator: eventlog emit", "err", err)
			}
			chunker := c.lookupChunker(ref.Target)
			if chunker == nil {
				var cerr error
				chunker, cerr = c.qsrc.NewChunker(ref.Source, strings.Join(ref.PrimaryKey, ","), c.cfg.ChunkSize)
				if cerr != nil {
					snapDone <- fmt.Errorf("coordinator: chunker %s: %w", ref.Source, cerr)
					return
				}
			}
			if err := c.snapshotTable(snapCtx, rdr, chunker, ref, snapCfg); err != nil {
				snapDone <- fmt.Errorf("coordinator: snapshot %s: %w", ref.Source, err)
				return
			}
			if err := c.finishSnapshot(snapCtx, ref); err != nil {
				snapDone <- fmt.Errorf("coordinator: snapshot %s: %w", ref.Source, err)
				return
			}
			c.log.Info("coordinator snapshot done", "table", ref.Source)
			if err := c.emit(eventlog.KindSnapshotDone, map[string]any{"table": ref.Source}); err != nil {
				c.log.Warn("coordinator: eventlog emit", "err", err)
			}
		}
		snapDone <- nil
	}()

	// Wait for the snapshot, aborting on any terminal signal: the snapshot
	// cannot progress without its worker, and a session or stream death
	// mid-snapshot is a real run error — not a wedge to wait out.
	select {
	case err := <-snapDone:
		if err != nil {
			c.emitLog(eventlog.KindJobStopped, terminalFields("snapshot", err))
			return err
		}
	case <-ctx.Done():
		c.gracefulShutdown()
		c.emitLog(eventlog.KindJobStopped, terminalFields("shutdown", ctx.Err()))
		return ctx.Err()
	case err := <-c.sessionErrs:
		c.gracefulShutdown()
		c.emitLog(eventlog.KindJobStopped, terminalFields("session", err))
		return fmt.Errorf("coordinator: worker session: %w", err)
	case err := <-streamErr:
		c.gracefulShutdown()
		c.emitLog(eventlog.KindJobStopped, terminalFields("stream", err))
		return fmt.Errorf("coordinator: stream: %w", err)
	case err := <-c.terminate:
		c.gracefulShutdown()
		c.emitLog(eventlog.KindJobTerminated, terminalFields(terminateReason(err), err))
		return err
	}

	// Supervision after the snapshot phase: acks only flow once the stream
	// is live, so a long snapshot must not look like a stale worker.
	go c.supervisor.run(ctx, supervisionConfig(c.cfg), c.terminate)

	// Replica reconcile after the snapshot too: the snapshot assigns chunks
	// by the boot routing, and a re-slice mid-snapshot would move a range
	// out from under it. KEDA owns the worker replica count; this follows it
	// (issue #298). A no-op unless Kubernetes worker provisioning is on.
	go c.scaleReconcileLoop(ctx)

	// Block until the world ends. ctx.Done is checked first on every pass so
	// a cancelled run never races a session defer's context.Canceled into
	// the report as a spurious worker failure (audit #11); the ctx.Err()
	// guard on the error cases closes the residual race. gracefulShutdown
	// runs on every exit.
	for {
		select {
		case <-ctx.Done():
			c.gracefulShutdown()
			c.emitLog(eventlog.KindJobStopped, terminalFields("shutdown", ctx.Err()))
			return ctx.Err()
		case err := <-c.terminate:
			c.gracefulShutdown()
			c.emitLog(eventlog.KindJobTerminated, terminalFields(terminateReason(err), err))
			return err
		case err := <-streamErr:
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.gracefulShutdown()
			c.emitLog(eventlog.KindJobStopped, terminalFields("stream", err))
			return fmt.Errorf("coordinator: stream: %w", err)
		case err := <-c.sessionErrs:
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.gracefulShutdown()
			c.emitLog(eventlog.KindJobStopped, terminalFields("session", err))
			return fmt.Errorf("coordinator: worker session: %w", err)
		}
	}
}

// ── Positions ─────────────────────────────────────────────────────────

// randSuffix returns n hex chars of crypto randomness (run-id suffix).
func randSuffix(n int) string {
	b := make([]byte, n/2+1)
	if _, err := rand.Read(b); err != nil {
		// Same rule as randTicket: a deterministic fallback would make
		// runIDs collide across restarts and overwrite a prior run's S3
		// manifests. crypto/rand failure is fatal, not degraded.
		panic(fmt.Sprintf("coordinator: crypto/rand: %v", err))
	}
	return hex.EncodeToString(b)[:n]
}

// tableNames renders a group's source tables for the audit trail.
func tableNames(refs []source.TableRef) []string {
	out := make([]string, len(refs))
	for i, r := range refs {
		out[i] = r.Source
	}
	return out
}

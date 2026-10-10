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
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/dashboard"
	"github.com/maltzsama/urutau/internal/eventlog"
	"github.com/maltzsama/urutau/internal/grpctls"
	"github.com/maltzsama/urutau/internal/logging"
	"github.com/maltzsama/urutau/internal/observability"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
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
	// waitChunkReadyOr blocks), so the knob is validated but reserved — it is
	// not yet wired to concurrency.
	MaxParallelChunks int

	// FlowTotalBytes is the process-wide ceiling on serialized batch bytes
	// in flight (queued or sent, unacked). FlowPerWorkerMin is the floor
	// that keeps a slow worker from starving. Defaults 512Mi / 16Mi.
	FlowTotalBytes   int64
	FlowPerWorkerMin int64
	// FlowPerWorkerMax caps one worker's bytes in flight; zero means 128 MiB.
	FlowPerWorkerMax int64
	// CycleMaxRows and CycleMaxAge bound how much of a staged table's
	// consecutive source batches one cycle coalesces (coalesce.go); zero
	// means the defaults.
	CycleMaxRows  int
	CycleMaxBytes int64
	CycleMaxAge   time.Duration

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
	// MaxConsecutiveCrashes ends the run once one worker crashes that many
	// times in a row without delivering what it owed (default 3,
	// recovery.go). WorkerDeliveryTimeout ends it once a worker owing work
	// has delivered none of it for that long (default 5m).
	MaxConsecutiveCrashes int
	WorkerDeliveryTimeout time.Duration

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

// maxBatchBytes is the largest serialized batch the control plane will queue.
// The gRPC servers cap messages at 128 MiB; fail loud with headroom instead of
// looping on a batch the worker can never receive.
const maxBatchBytes = 120 << 20

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
	// tables is the expanded table list (a discovery pipeline enumerates it
	// at boot). It is kept here, not written back into cfg.Spec, so a
	// concurrent /statusz or dashboard read never races the boot write; both
	// read it under c.mu (issue #556).
	tables []spec.Table

	src       source.Source
	qsrc      source.QuerySource
	refs      []source.TableRef
	snk       sink.Sink
	canonical map[string]core.Schema // per-source canonical schema for typed wire format

	// Per-batch hot paths index refs/tables by key instead of scanning the
	// slices (issue #582). Built once at boot; never mutated after.
	refByTarget  map[string]core.TableRef
	specBySource map[string]spec.Table
	specByTarget map[string]spec.Table

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
	// streamErrs carries the source stream's terminal signal (sourceBatches),
	// set by phaseStream before the terminal wait and read only by
	// awaitTerminal.
	streamErrs <-chan error
	// ackNotify wakes awaitChunkCommits waiters when an ack may have dropped
	// in-flight markers, so the snapshot pacer waits on an event instead of
	// polling (issue #587). Buffered 1 + non-blocking send: coalesced, never
	// blocks the ack path.
	ackNotify chan struct{}
	// snapshotActive is true while the snapshot phase runs. A worker
	// session lost during it (reset OR death) must fail the run fast: the
	// worker's in-memory window died with it, so the protocol would either
	// wait out AckTimeout/MaxResets (15min) or let a stale ChunkReady from
	// the old generation satisfy the wait against an empty window — a
	// silently incomplete snapshot.
	snapshotActive atomic.Bool
	batchSeq       atomic.Uint64 // monotonic BatchMeta.batch_id
	// readiness is set once routing is published and the stream pump is
	// running: the signal the readiness probe reads (issue #601).
	readiness atomic.Bool
	// lastPump is the unix-nano time of the pump's last loop iteration: the
	// liveness signal a probe reads, so a wedged pump (which would otherwise
	// leave the process "live" but doing nothing) is restarted (issue #601).
	lastPump atomic.Int64
	// decodeErrors, when the source reports it, returns the running count of
	// records its decoder dropped (Kafka onDecodeError: skip); polled for the
	// skipped counter (issue #602). Written at boot before the lag loop starts;
	// lastDecodeErrors is the value at the previous poll (lag-loop goroutine
	// only), so the counter is advanced by the delta.
	decodeErrors     func() int64
	lastDecodeErrors int64
	// booted closes once waitWorkers has seen every group attach. Before that,
	// Session blocks on ready to wake waitWorkers; after, nothing drains it,
	// so signalReady also selects on booted and never wedges (#493). Closing
	// the channel (not a bool) makes the transition race-free: a session
	// already blocked on a full ready unblocks the moment boot closes it.
	booted chan struct{}
	mu     sync.Mutex // guards session attach/detach

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
	gateBuf   map[string][]*dataplane.Batch
	gateBytes map[string]int64 // gateBuf's size per window (gateMaxBytes)
	// gateDrain wakes a pump blocked on a full gate when flushWindow/
	// closeWindow drains it (audit #5: the gate was the only buffer without
	// a structural bound). One shared channel: any drain (of any window)
	// wakes every waiter, which re-checks its own window's state.
	gateDrain chan struct{}
	// gateReady is, per open window, the chunk whose ChunkReady arrived and
	// whose catch-up is still under way: its rows are in the worker's
	// window, so the gate's held batches may go out InWindow-tagged for it
	// before the catch-up ends. A full gate then drains instead of blocking
	// the pump — blocking it stalled the reader, and with it the very
	// catch-up the window waited for.
	gateReady map[string]uint64
	// gateFlushMu serializes every drain of a gate, from taking its buffer
	// to the last enqueue: the pump and the snapshot both drain, and the
	// worker must receive the held batches in source order.
	gateFlushMu sync.Mutex
	// accum holds each staged table's batches not sent yet (coalesce.go),
	// guarded by gateMu; sent under gateFlushMu.
	accum map[string]*cycleAccum

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
	// windowOpen routes worker WindowOpen announcements to the snapshot loop.
	windowOpen chan *pb.WindowOpen
	// chunkMarkers are each worker's queued snapshot Closes markers, to pace
	// chunks on commits (snapshot_pace.go).
	chunkMarkersMu sync.Mutex
	chunkMarkers   map[string][]chunkMarker
	// lostWindows are the chunk windows each lost worker had not
	// committed, taken at the loss (recovery.go); under chunkMarkersMu.
	lostWindows map[string][]chunkMarker
	// chunkCursor is, per chunkRef, the last acked window's high key (#646).
	chunkCursor map[uint32][]any
	// podTermination reads a worker's last container termination; nil
	// reads it from Kubernetes (recovery.go). Set by tests.
	podTermination func(worker string) (podTermination, bool)
	// snapshotTodo is each snapshotting table's chunks still to do
	// (snapshotPlan); read-only once set.
	snapshotTodoMu sync.Mutex
	snapshotTodo   map[string]map[uint32]bool

	// snapshotting marks a table whose snapshot is running. A re-slice must
	// not flip that table's owner layout mid-snapshot: the fan-out captured
	// the owners at start and the live stream routes by the current layout,
	// so a flip would send a key's snapshot row and its live row to different
	// workers.
	snapshottingMu sync.Mutex
	snapshotting   map[string]bool

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
	// lastConfirmedPos/At track when the confirmed (minimum committed)
	// position last advanced, for the confirmed-position age gauge (issue
	// #602). Guarded by confirmedMu.
	lastConfirmedPos position.Position
	lastConfirmedAt  time.Time

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
	// metricsSrv is the dashboard/metrics listener, kept so run's return can
	// Shutdown it instead of leaking the bound addr and its goroutine (#495).
	metricsSrv *http.Server
	// metricsDone closes when the metrics goroutine returns, so Run never
	// returns before the listener is fully stopped (#495).
	metricsDone chan struct{}

	// Dashboard (issue #97): the recent-events ring, the read-only API handler,
	// the run's start time, and the per-table/worker aggregates the API serves.
	dashEvents *dashboard.Events
	dash       *dashboard.Handler
	// dashDirty coalesces dashboard state changes: pushDashState marks it and
	// a ~4 Hz loop publishes, so a per-ack snapshot does not serialize the
	// whole state on the hot path (issue #591).
	dashDirty atomic.Bool
	startedAt time.Time

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
	// lostCh is closed when the current session is lost (recovery.go);
	// guarded by c.mu.
	lostCh chan struct{}

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
	if cfg.FlowPerWorkerMax <= 0 {
		cfg.FlowPerWorkerMax = 128 << 20
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
		ackNotify:   make(chan struct{}, 1),
		chunkReady:  make(chan *pb.ChunkReady, 1024),
		windowOpen:  make(chan *pb.WindowOpen, 1024),
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
		booted:      make(chan struct{}),
	}
	c.budget = newFlowBudget(cfg.FlowTotalBytes, cfg.FlowPerWorkerMin)
	c.budget.perWorkerMax = cfg.FlowPerWorkerMax
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
		c.metricsSrv = observability.NewServer(cfg.MetricsAddr, mux)
		c.metricsDone = make(chan struct{})
		go func() {
			defer close(c.metricsDone)
			// A busy port silently disables observability otherwise — say so.
			if err := c.metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				c.log.Warn("coordinator: metrics server stopped", "addr", cfg.MetricsAddr, "err", err)
			}
		}()
	}
	defer c.shutdownMetrics()
	return c.run(ctx)
}

// run boots the pipeline as a short sequence of named phases and blocks until
// ctx is cancelled or a terminal error occurs. Each phase is documented on its
// own method; the deferred cleanups stay here, in acquisition order, so they
// unwind LIFO exactly as the original single function did.
func (c *Coordinator) run(ctx context.Context) error {
	c.runCtx = ctx

	stopEventlog, err := c.phaseEventlog(ctx)
	if err != nil {
		return err
	}
	defer stopEventlog()

	// Reject a bootstrap block this mode ignores before touching the source:
	// the error must not hide behind a connection or introspection failure.
	// Discovered tables (source.ExpandTables) never carry one, so checking the
	// declared list is enough.
	for _, t := range c.cfg.Spec.Tables {
		if err := requireSnapshotBootstrap(t); err != nil {
			return err
		}
	}

	if err := c.phaseSource(); err != nil {
		return err
	}
	defer c.closeQuery()

	resolved, tableBySource, err := c.phaseResolveTables(ctx)
	if err != nil {
		return err
	}
	if err := c.phaseValidateTables(tableBySource); err != nil {
		return err
	}
	workerTarget, err := c.phaseResolveRouting(ctx)
	if err != nil {
		return err
	}
	if err := c.phaseProvisionWorkers(ctx, workerTarget); err != nil {
		return err
	}
	if err := c.phaseOpenSink(ctx, resolved, tableBySource); err != nil {
		return err
	}
	defer c.closeSink()

	needsSnapshot, resume, err := c.phaseMaintenanceResumeBaseline(ctx, workerTarget)
	if err != nil {
		return err
	}

	// Serve gRPC (control) + Flight (data) on one listener.
	cleanupServe, err := c.phaseServe()
	if err != nil {
		return err
	}
	defer cleanupServe()

	rdr, err := c.phaseStream(ctx, resume)
	if err != nil {
		return err
	}
	defer rdr.Close()

	snapCancel, snapDone := c.phaseSnapshot(ctx, rdr, needsSnapshot)
	defer snapCancel()

	// Wait for the snapshot, aborting on any terminal signal: the snapshot
	// cannot progress without its worker, and a session or stream death
	// mid-snapshot is a real run error — not a wedge to wait out. A snapshot
	// failure (done == false with an error) ends the run; a clean snapshot
	// completion (done == true) falls through to the steady-state wait.
	if done, err := c.awaitTerminal(ctx, snapDone); !done {
		return err
	}

	// Replica reconcile after the snapshot too: the snapshot assigns chunks by
	// the boot routing, and a re-slice mid-snapshot would move a range out from
	// under it. KEDA owns the worker replica count; this follows it (issue
	// #298). A no-op unless Kubernetes worker provisioning is on.
	go c.scaleReconcileLoop(ctx)

	// Block until the world ends. ctx.Done is checked first on every pass so a
	// cancelled run never races a session defer's context.Canceled into the
	// report as a spurious worker failure (audit #11); the ctx.Err() guard on
	// the error cases closes the residual race. gracefulShutdown runs on every
	// exit. A nil extra never resolves, so this returns only on a terminal
	// signal.
	_, err = c.awaitTerminal(ctx, nil)
	return err
}

// ── gRPC control plane ───────────────────────────────────────────────

// ── Flight data plane ────────────────────────────────────────────────

// ── Positions ─────────────────────────────────────────────────────────

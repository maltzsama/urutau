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
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/driver"
	"github.com/maltzsama/urutau/internal/dashboard"
	"github.com/maltzsama/urutau/internal/enrich"
	"github.com/maltzsama/urutau/internal/eventlog"
	"github.com/maltzsama/urutau/internal/grpctls"
	"github.com/maltzsama/urutau/internal/logging"
	"github.com/maltzsama/urutau/internal/observability"
	"github.com/maltzsama/urutau/internal/snapshot"
	"github.com/maltzsama/urutau/internal/transport"
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
	epoch    uint64 // last accepted epoch (guards stale Hellos)
	cancel   context.CancelFunc

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

	// Lag grows between commits, so the gauge needs its own clock: setting it
	// on the ack path would pin it near zero after every commit and never let
	// it rise. Mirrors the dashboard's on-demand Tables().
	if c.metrics != nil {
		go c.lagLoop(ctx)
	}

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
	qsrc, ok := src.(source.QuerySource)
	if !ok {
		return fmt.Errorf("coordinator: source %q has no SQL query surface", c.cfg.Spec.Source.Kind)
	}
	c.qsrc = qsrc
	defer func() { _ = qsrc.CloseQuery() }()
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
	c.log.Info("coordinator resume", "from", resumeOrNone(resume), "snapshot_tables", len(needsSnapshot))

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
			c.emitLog(eventlog.KindJobStopped, map[string]any{"reason": "snapshot"})
			return err
		}
	case <-ctx.Done():
		c.gracefulShutdown()
		c.emitLog(eventlog.KindJobStopped, map[string]any{"reason": "shutdown"})
		return ctx.Err()
	case err := <-c.sessionErrs:
		c.gracefulShutdown()
		c.emitLog(eventlog.KindJobStopped, map[string]any{"reason": "session"})
		return fmt.Errorf("coordinator: worker session: %w", err)
	case err := <-streamErr:
		c.gracefulShutdown()
		c.emitLog(eventlog.KindJobStopped, map[string]any{"reason": "stream"})
		return fmt.Errorf("coordinator: stream: %w", err)
	case err := <-c.terminate:
		c.gracefulShutdown()
		c.emitLog(eventlog.KindJobTerminated, map[string]any{"reason": terminateReason(err)})
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
			c.emitLog(eventlog.KindJobStopped, map[string]any{"reason": "shutdown"})
			return ctx.Err()
		case err := <-c.terminate:
			c.gracefulShutdown()
			c.emitLog(eventlog.KindJobTerminated, map[string]any{"reason": terminateReason(err)})
			return err
		case err := <-streamErr:
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.gracefulShutdown()
			c.emitLog(eventlog.KindJobStopped, map[string]any{"reason": "stream"})
			return fmt.Errorf("coordinator: stream: %w", err)
		case err := <-c.sessionErrs:
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.gracefulShutdown()
			c.emitLog(eventlog.KindJobStopped, map[string]any{"reason": "session"})
			return fmt.Errorf("coordinator: worker session: %w", err)
		}
	}
}

// resolvePartitionRanges returns the ordered, contiguous PK ranges one
// table's Workers count requires — the SAME ranges the DBLog snapshot
// (each chunk aligned within its owning range) and live-stream routing
// both use, so a key never switches partition ownership between the two
// phases. Workers<=1 returns a single unbounded range without touching
// the database at all — the common, unpartitioned case pays no extra
// cost. Workers>1 requires the source's chunker to implement
// source.PartitionSource; a source that doesn't (Postgres, today) fails
// the boot loudly rather than silently running unpartitioned.
// requirePartitionKey rejects workers>1 for a table with no primary key:
// partitioning splits the key range, and a table without one has no way to
// divide it. Checked before resolvePartitionRanges so the error names the
// real cause instead of the chunker's cryptic empty-key failure.
func requirePartitionKey(t spec.Table, ref core.TableRef) error {
	if t.WorkerCount() > 1 && len(ref.PrimaryKey) == 0 {
		return fmt.Errorf("coordinator: %s: workers>1 requires a primary key: "+
			"partitioning splits the key range, and a table without one has no "+
			"way to divide it", t.Target)
	}
	return nil
}

// requireConcurrentSink rejects workers>1 when the sink does not declare the
// ConcurrentWriter capability (or declares it false). snk is taken as any so
// the check is a pure capability probe — the coordinator never reaches into a
// concrete sink.
func requireConcurrentSink(t spec.Table, snk any) error {
	if t.WorkerCount() <= 1 {
		return nil
	}
	cw, ok := snk.(sink.ConcurrentWriter)
	if !ok || !cw.SupportsConcurrentWriters() {
		return fmt.Errorf("coordinator: %s: workers>1 is not supported by "+
			"this sink (it cannot order concurrent writers to one table); "+
			"use workers: 1, or for couchbase set sink.commitMode: atomic",
			t.Target)
	}
	return nil
}

func (c *Coordinator) resolvePartitionRanges(ctx context.Context, t spec.Table, ref source.TableRef) ([]source.Chunk, source.ChunkSource, error) {
	n := t.WorkerCount()
	if n <= 1 {
		// Unpartitioned: no chunker needed at boot; the snapshot builds it
		// lazily (the common case pays no chunker construction here).
		return []source.Chunk{{}}, nil, nil
	}
	chunker, err := c.qsrc.NewChunker(ref.Source, strings.Join(ref.PrimaryKey, ","), c.cfg.ChunkSize)
	if err != nil {
		return nil, nil, fmt.Errorf("workers: %d: chunker: %w", n, err)
	}
	ps, ok := chunker.(source.PartitionSource)
	if !ok {
		return nil, nil, fmt.Errorf("workers: %d: this source does not support range partitioning yet", n)
	}
	ranges, err := ps.Partitions(ctx, n)
	if err != nil {
		return nil, nil, fmt.Errorf("workers: %d: %w", n, err)
	}
	return ranges, chunker, nil
}

// supervisionConfig maps the Config knobs to the supervisor defaults.
func supervisionConfig(cfg Config) SupervisorConfig {
	return SupervisorConfig{
		AckTimeout:  cfg.AckTimeout,
		MaxResets:   cfg.MaxResets,
		ResetWindow: cfg.ResetWindow,
	}
}

// emitLog writes an event and logs any failure (best-effort trail).
func (c *Coordinator) emitLog(kind string, fields map[string]any) {
	if err := c.emit(kind, fields); err != nil {
		c.log.Warn("coordinator: eventlog emit", "kind", kind, "err", err)
	}
}

// statusz renders the live coordinator state for /statusz (design §13.4).
func (c *Coordinator) statusz(w http.ResponseWriter, r *http.Request) {
	type workerStatus struct {
		Phase     string            `json:"phase"`
		Epoch     uint64            `json:"epoch"`
		Attached  bool              `json:"attached"`
		Inflight  int64             `json:"inflight_bytes"`
		Committed map[string]string `json:"committed,omitempty"`
	}
	st := map[string]any{
		"run_id": c.runID,
	}
	// Point-in-time enrichment is visible to the operator: enriched columns
	// are NOT reproducible by replay (the reference is a snapshot, not
	// CDC), and that trade is declared, not hidden.
	for _, t := range c.cfg.Spec.Tables {
		if len(t.Enrich) > 0 {
			st["enrichment"] = "point-in-time"
			break
		}
	}
	ws := map[string]*workerStatus{}
	// One lock across the whole iteration: statusz runs from the metrics
	// server, which boots BEFORE run() populates c.workers — a per-entry
	// lock still races the map write on boot (audit #13).
	c.mu.Lock()
	for name, w := range c.workers {
		// Phase reflects reality: the field used to hardcode "attached" for
		// every worker, which lied about detached/pending ones.
		phase := "detached"
		if w.attached {
			phase = "attached"
		}
		ws[name] = &workerStatus{
			Phase:     phase,
			Epoch:     w.epoch,
			Attached:  w.attached,
			Inflight:  c.budget.inFlight(name),
			Committed: w.committed,
		}
	}
	c.mu.Unlock()
	st["workers"] = ws
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(st); err != nil {
		c.log.Warn("statusz encode", "err", err)
	}
}

// emit writes one event to the audit trail when configured; best-effort by
// contract (a lost trail must never fail the pipeline). The dashboard's
// recent-events ring is fed here too, unconditionally, so the UI shows events
// even when no audit trail is configured.
func (c *Coordinator) emit(kind string, fields map[string]any) error {
	if c.dashEvents != nil {
		ev := c.dashEvents.Record(kind, fields)
		if c.dash != nil {
			c.dash.PublishEvent(ev)
		}
	}
	if c.ev == nil {
		return nil
	}
	// Bounded: the audit-trail upload must not hang the caller (the ack hot
	// path already fires-and-forgets, but emit is also called synchronously
	// on boot/terminal paths).
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.ev.Emit(ctx, kind, fields); err != nil {
		return err
	}
	return nil
}

// waitWorkers blocks until every expected group has a session attached.
// Counting ready signals would miscount a flapping worker that attaches,
// dies, and reattaches inside the window (audit #4) — so the wait checks
// the attached flag directly, woken by each attach and the poll.
func (c *Coordinator) waitWorkers(ctx context.Context, wait time.Duration) error {
	deadline := time.Now().Add(wait)
	allAttached := func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, w := range c.workers {
			if !w.attached {
				return false
			}
		}
		return true
	}
	poll := time.NewTicker(100 * time.Millisecond)
	defer poll.Stop()
	for {
		if allAttached() {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("coordinator: not all workers connected within %s", wait)
		}
		select {
		case <-c.ready:
		case <-poll.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// pump encodes reader events into the data queue. While a DBLog window is
// open (gateOn), events of the gated table are buffered instead — released
// InWindow-tagged by flushWindow after the worker confirms ChunkReady. Other
// tables flow freely.
func (c *Coordinator) pump(ctx context.Context, out <-chan *dataplane.Batch) {
	for {
		select {
		case b, ok := <-out:
			if !ok {
				return
			}
			if c.metrics != nil {
				c.metrics.EventsDecoded.Inc()
			}
			// A re-slice pauses this table so its drain can converge. The
			// batch is BUFFERED (not routed, not counted by the drain) until
			// the flip, then enqueued under the new layout — so no batch
			// spans the flip and ordering is preserved, and the pump keeps
			// draining the reader for every other table (issue #343).
			if c.pauseHold(b) {
				continue
			}
			if c.gateHold(ctx, b) {
				continue
			}
			// enqueueBatch takes ownership of b (serializes + releases).
			if err := c.enqueueBatch(ctx, b, nil); err != nil {
				c.pumpFail(ctx, err)
				return
			}
		case <-c.pauseWake:
			// A table resumed: route the batches held during its pause, in
			// order, under the new layout.
			if err := c.flushPaused(ctx); err != nil {
				c.pumpFail(ctx, err)
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

// pumpFail surfaces a pump failure on the terminal plane: a pump death is a
// real failure — the reader stalls behind the closed out channel and the
// coordinator stays "alive" doing nothing (audit #9). On a cancelled pipeline
// run's select already owns the exit.
func (c *Coordinator) pumpFail(ctx context.Context, err error) {
	c.log.Warn("coordinator: enqueue failed", "err", err)
	if ctx.Err() == nil {
		select {
		case c.terminate <- fmt.Errorf("coordinator: pump: %w", err):
		default:
		}
	}
}

// gateMaxEvents bounds one snapshot window's held live batches. Beyond it
// the pump blocks until flushWindow drains — the gate's structural
// backpressure (audit #5). Tuned to a few minutes of a busy table at
// ~10k/s; batches are source-sized (≤ batchTarget rows), so the row volume
// held is bounded by gateMaxEvents × batchTarget.
const gateMaxEvents = 1024

// gateWindow identifies one open DBLog window: a table's Nth partition.
// An unpartitioned table's sole window is always {target, 0}.
type gateWindow struct {
	target    string
	partition int
}

// gateKey renders a gateWindow into the map key gateOn/gateBuf/gateWin use.
func gateKey(target string, partition int) string {
	return fmt.Sprintf("%s#%d", target, partition)
}

// gateHold buffers a batch when a window is open for a partition its rows
// could belong to. A full gate blocks the pump until the snapshot drains
// it, instead of growing the buffer without bound. Ownership: when
// gateHold returns true the batch is in the gate and released by
// flushWindow/closeWindow.
//
// A table with only one partition (the common case) always gates on
// gateKey(target, 0) — the same single-window behavior as before
// partitioning existed. A partitioned table's batch is gated by EVERY
// open window that its PK range could overlap: gateHold does not decode
// rows to know precisely which partitions a batch touches, so it
// conservatively holds a batch against any open window for its table
// rather than risk releasing a row whose partition's snapshot chunk
// hasn't confirmed caught-up yet. This can hold a batch slightly longer
// than strictly necessary (extra latency, never data loss) when multiple
// partitions of the same table snapshot concurrently.
//
// If the context dies while the pump waits on a full gate, gateHold returns
// false and the batch is treated as live (not gated). That is only reachable
// during shutdown, where the pump exits on ctx.Done immediately after — it
// must not be relied on in any live path.
func (c *Coordinator) gateHold(ctx context.Context, b *dataplane.Batch) bool {
	c.gateMu.Lock()
	key, held := c.openKeyForTableLocked(b.Table)
	if !held {
		c.gateMu.Unlock()
		return false
	}
	full := len(c.gateBuf[key]) >= gateMaxEvents
	c.gateMu.Unlock()
	if full {
		select {
		case <-c.gateDrain:
		case <-ctx.Done():
			return false
		}
	}
	c.gateMu.Lock()
	// Re-check after the wait: the gate may have drained, closed, or the
	// table's window may have changed while the pump was asleep.
	key, held = c.openKeyForTableLocked(b.Table)
	if !held {
		c.gateMu.Unlock()
		return false
	}
	c.gateBuf[key] = append(c.gateBuf[key], b)
	c.gateMu.Unlock()
	return true
}

// openKeyForTableLocked returns the first open gate key for target, if
// any. Callers must hold gateMu. A table has at most as many
// simultaneously-open keys as it has partitions actively snapshotting;
// gateHold only needs to know "is ANY window for this table open" since
// it gates conservatively at the whole-table (not per-row) level.
func (c *Coordinator) openKeyForTableLocked(target string) (string, bool) {
	for k, on := range c.gateOn {
		if !on {
			continue
		}
		if w, ok := c.gateWin[k]; ok && w.target == target {
			return k, true
		}
	}
	return "", false
}

// openWindow pauses the pump for one table's partition, tagging the
// current chunk. The gate stays open for the WHOLE snapshot of that
// partition (design §3.1: the coordinator pauses relaying while it works
// the table); flushWindow drains per chunk without closing it, and
// closeWindow seals it at the end. A gate that opened and closed per
// chunk would let gap events (positioned AFTER the gate's backlog) flow
// straight through, then release older backlog after them — a
// reordering that resurrects old values.
func (c *Coordinator) openWindow(target string, partition int) {
	c.gateMu.Lock()
	key := gateKey(target, partition)
	if c.gateOn == nil {
		c.gateOn = map[string]bool{}
		c.gateWin = map[string]gateWindow{}
		c.gateBuf = map[string][]*dataplane.Batch{}
	}
	c.gateOn[key] = true
	c.gateWin[key] = gateWindow{target: target, partition: partition}
	c.gateMu.Unlock()
}

// flushWindow drains the gated batches collected since the last drain for
// one partition's window, each InWindow-tagged for the given chunk, then
// returns (that window stays open). Batch ownership transfers to
// enqueueBatch per drain. enqueueBatch itself splits a batch across
// partition owners by PK range when the table is partitioned, so a
// drained batch reaches only the rows' actual owning worker(s) even
// though the gate held it at whole-table granularity.
func (c *Coordinator) flushWindow(ctx context.Context, target string, partition int, chunkID uint32) error {
	key := gateKey(target, partition)
	c.gateMu.Lock()
	buf := c.gateBuf[key]
	c.gateBuf[key] = nil
	close(c.gateDrain)
	c.gateDrain = make(chan struct{})
	c.gateMu.Unlock()

	meta := &pb.BatchMeta{
		Table:  target,
		Window: &pb.WindowTag{InWindow: true, ChunkId: chunkID},
	}
	for i, b := range buf {
		if err := c.enqueueBatch(ctx, b, meta); err != nil {
			for _, rest := range buf[i+1:] {
				rest.Release()
			}
			return err
		}
	}
	return nil
}

// closeWindow releases any remaining gated batches (post-last-chunk) for
// one partition's window and closes just that window — other partitions
// of the same table still snapshotting keep their own windows open. The
// trailing events are ordinary live changes: no window tag.
func (c *Coordinator) closeWindow(ctx context.Context, target string, partition int) error {
	key := gateKey(target, partition)
	c.gateMu.Lock()
	buf := c.gateBuf[key]
	delete(c.gateOn, key)
	delete(c.gateWin, key)
	delete(c.gateBuf, key)
	close(c.gateDrain)
	c.gateDrain = make(chan struct{})
	c.gateMu.Unlock()

	meta := &pb.BatchMeta{Table: target}
	for i, b := range buf {
		if err := c.enqueueBatch(ctx, b, meta); err != nil {
			for _, rest := range buf[i+1:] {
				rest.Release()
			}
			return err
		}
	}
	return nil
}

// releaseAllGates closes every open gate and releases the batches held in it.
// Called when the snapshot phase ends, so an aborted snapshot (ctx cancelled
// before closeWindow) does not leak the gated batches (issue #212). gateHold
// re-checks the window under gateMu before appending, so a gate cleared here
// cannot be re-populated afterwards.
func (c *Coordinator) releaseAllGates() {
	c.gateMu.Lock()
	var held []*dataplane.Batch
	for k := range c.gateOn {
		held = append(held, c.gateBuf[k]...)
		delete(c.gateOn, k)
		delete(c.gateWin, k)
		delete(c.gateBuf, k)
	}
	// Wake any pump blocked on a full gate so it re-checks and sees the gate
	// gone (gateHold returns false and the batch flows as live).
	close(c.gateDrain)
	c.gateDrain = make(chan struct{})
	c.gateMu.Unlock()
	for _, b := range held {
		b.Release()
	}
}

// recordConfirmed stores a worker's latest durably-committed position and
// recomputes the pipeline-wide minimum. The minimum uses the position's own
// ordering — LSNs and GTID sets are not lexicographically ordered, and a
// wrong minimum would advance the source slot past data still in flight.
//
// Keyed by worker (not table): for a partitioned table the N workers commit
// disjoint key ranges and must each hold their own position; a per-table key
// would let the last acker overwrite the lagging partitions (WK-001 §2.2).
func (c *Coordinator) recordConfirmed(worker string, pos position.Position) {
	if pos == nil {
		return
	}
	c.confirmedMu.Lock()
	defer c.confirmedMu.Unlock()
	c.confirmed[worker] = pos
}

// confirmedPosition returns the minimum committed position across all
// workers; nil while nothing is durably committed.
//
// MinSafe, not Min: if the committed positions are not mutually comparable
// (never for one source, but a guard), there is no safe minimum — advancing
// the source's retention to an arbitrary one could pass uncommitted data.
// Nil means "nothing provably committed", which holds retention back.
func (c *Coordinator) confirmedPosition() position.Position {
	c.confirmedMu.Lock()
	defer c.confirmedMu.Unlock()
	if len(c.confirmed) == 0 {
		return nil
	}
	vals := make([]position.Position, 0, len(c.confirmed))
	for _, p := range c.confirmed {
		if p == nil {
			// A registered worker with no committed position yet (its boot
			// baseline): nothing is provably committed past the resume point,
			// so hold retention back rather than advance over its data.
			return nil
		}
		vals = append(vals, p)
	}
	best, err := position.MinSafe(vals)
	if err != nil {
		c.log.Warn("coordinator: incomparable committed positions; not advancing retention", "err", err)
		return nil
	}
	return best
}

// waitChunkReady blocks until the worker reports the chunk SELECT done for
// THIS epoch. A ChunkReady from a superseded generation (same table+chunkID,
// different epoch) is ignored, so a stale reply cannot satisfy the wait
// against a dead window.
func (c *Coordinator) waitChunkReady(ctx context.Context, table string, chunkID uint32, epoch uint64) error {
	for {
		select {
		case cr := <-c.chunkReady:
			if cr.Table == table && cr.ChunkId == chunkID && cr.Epoch == epoch {
				return nil
			}
			c.log.Warn("coordinator: ignoring stale/unexpected ChunkReady",
				"table", cr.Table, "chunk", cr.ChunkId, "epoch", cr.Epoch, "want_epoch", epoch)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// snapshotTable runs the DBLog snapshot for one table with the chunk SELECT
// executed by the worker (design §3.1): for each chunk the coordinator pauses
// the pump (openWindow), sends ChunkRequest bounds, waits ChunkReady, proves
// caught-up, then releases the gated live events InWindow-tagged and the
// Closes marker. The worker holds the chunk rows in its window; the window
// is what InWindow events drain and the Closes marker flushes.
func (c *Coordinator) snapshotTable(ctx context.Context, rdr source.SourceReader, chunker source.ChunkSource, ref source.TableRef, cfg snapshot.SnapshotConfig) error {
	rt := c.loadRouting()
	owners, ok := rt.ownersOf(ref.Target)
	if !ok {
		return fmt.Errorf("coordinator: snapshot: no worker owns %s", ref.Target)
	}
	ranges := rt.rangesOf(ref.Target)
	if len(ranges) != len(owners) {
		return fmt.Errorf("coordinator: snapshot: table %s: %d partition ranges for %d owners", ref.Target, len(ranges), len(owners))
	}

	// One partition at a time: all partitions of a table share the same
	// replication reader (rdr) — there is one binlog/WAL connection per
	// pipeline, not per partition — so this is sequential I/O today, not
	// parallel. Each partition still gets its own correct, independent
	// window (design requirement; the parallelism this feature is FOR is
	// steady-state live-stream throughput, which the per-partition
	// worker processes already give — see enqueueBatch's PK-range split).
	// A future iteration could parallelize the chunk SELECTs themselves
	// (they run on the WORKER, not rdr) while keeping rdr's caught-up
	// proof sequential; not needed for this to be correct.
	//
	// A partition whose range has no rows gets no chunks and its owner never
	// commits, so it would never record a position. Collect those owners and
	// seed their baseline AFTER the snapshot — here "empty" is provable (no
	// chunks at snapshot time), unlike at boot, where an owner with no
	// position could equally be one interrupted mid-snapshot (whose partition
	// MUST re-snapshot, not be resumed past). WK-001 §2.6.
	bounds, err := chunker.Bounds(ctx)
	if err != nil {
		return err
	}
	allChunks := snapshot.Chunks(bounds)
	var emptyOwners []string
	for p, w := range owners {
		if len(clipChunksToRange(allChunks, ranges[p])) == 0 {
			emptyOwners = append(emptyOwners, w.name)
		}
		if err := c.snapshotPartition(ctx, rdr, allChunks, ref, ranges[p], p, w, cfg); err != nil {
			return fmt.Errorf("partition %d: %w", p, err)
		}
	}
	if len(emptyOwners) > 0 {
		if seeder, ok := c.snk.(sink.PositionSeeder); ok {
			if err := seeder.SeedPositions(ctx, core.TableRef{Target: ref.Target}, emptyOwners); err != nil {
				return fmt.Errorf("coordinator: seed %s: %w", ref.Target, err)
			}
		}
	}
	return nil
}

// snapshotPartition runs the DBLog snapshot for one partition of one
// table: only chunks that fall within partitionRange are sent to w, and
// the window/gate lifecycle (openWindow/flushWindow/closeWindow) is
// scoped to this partition alone, so a different partition's concurrent
// snapshot (if any) is never gated or released by this one's chunks.
// allChunks is computed once by snapshotTable (one Bounds query per table,
// not per partition — issue #216).
func (c *Coordinator) snapshotPartition(ctx context.Context, rdr source.SourceReader, allChunks []source.Chunk, ref source.TableRef, partitionRange source.Chunk, partition int, w *workerState, cfg snapshot.SnapshotConfig) error {
	chunks := clipChunksToRange(allChunks, partitionRange)
	if len(chunks) == 0 {
		return nil // this partition's range contains no rows right now
	}
	// The epoch the ChunkRequests are sent under; the worker echoes it on
	// ChunkReady so a reply from a superseded generation is ignored.
	c.mu.Lock()
	epoch := w.epoch
	c.mu.Unlock()

	for i, ch := range chunks {
		chunkID := uint32(i)
		if err := ctx.Err(); err != nil {
			return err
		}
		if i == 0 {
			c.openWindow(ref.Target, partition)
		}
		// The snapshot watchdog: a worker that attaches but stops draining
		// would otherwise block the chunk round-trip (send, wait, flush)
		// forever — the supervisor does not run during the snapshot (issue
		// #206). The deadline must outlast WaitCaughtUp's own window timeout,
		// or a slow-but-healthy catch-up would look wedged (Config doc).
		timeout := c.cfg.SnapshotChunkTimeout
		if timeout <= 0 {
			timeout = defaultSnapshotChunkTimeout
		}
		chunkCtx, cancel := context.WithTimeout(ctx, timeout)
		err := c.snapshotChunk(chunkCtx, rdr, ref, partition, w, cfg, ch, chunkID, epoch)
		cancel()
		if err != nil {
			// A parent deadline/cancel (the run's own) surfaces here as
			// DeadlineExceeded too; report it as-is rather than blaming a
			// wedged worker. Only a chunk deadline with a live parent is the
			// watchdog firing.
			if ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded) {
				return fmt.Errorf("coordinator: snapshot %s: chunk %d did not complete within %s (the worker may be wedged): %w",
					ref.Source, chunkID, timeout, err)
			}
			return err
		}
	}
	// Seal this partition's gate and release anything collected after its
	// last chunk. Other partitions' gates (if any) are untouched.
	return c.closeWindow(ctx, ref.Target, partition)
}

// snapshotChunk runs one chunk's round-trip: send the ChunkRequest, wait for
// the worker's ChunkReady, prove the reader caught up, then release the gated
// live rows and the Closes marker. ctx carries the watchdog deadline, so a
// worker that never acks the chunk fails the run instead of wedging it.
func (c *Coordinator) snapshotChunk(ctx context.Context, rdr source.SourceReader, ref source.TableRef, partition int, w *workerState, cfg snapshot.SnapshotConfig, ch source.Chunk, chunkID uint32, epoch uint64) error {
	boundsB, err := transport.EncodeBounds(ch.Low, ch.High)
	if err != nil {
		return fmt.Errorf("coordinator: chunk %d bounds: %w", chunkID, err)
	}
	req := &pb.ChunkRequest{Table: ref.Source, ChunkId: chunkID, Bounds: boundsB}

	select {
	case w.out <- &pb.CoordinatorMessage{Msg: &pb.CoordinatorMessage_Chunk{Chunk: req}}:
	case <-ctx.Done():
		return ctx.Err()
	}

	if err := c.waitChunkReady(ctx, ref.Source, chunkID, epoch); err != nil {
		return err
	}
	c.log.Info("chunk ready", "table", ref.Source, "partition", partition, "chunk", chunkID)

	// The worker has the chunk rows in its window; prove the reader is
	// caught up before releasing anything that touches this window. The
	// high watermark is the source's FIXED position after the SELECT —
	// never a live master — so a busy source cannot keep the window open
	// forever.
	high, err := rdr.Master(ctx)
	if err != nil {
		return fmt.Errorf("dblog: chunk %d: master: %w", chunkID, err)
	}
	if err := snapshot.WaitCaughtUp(ctx, rdr, high, cfg); err != nil {
		return fmt.Errorf("dblog: chunk %d: %w", chunkID, err)
	}
	at := rdr.Synced()

	// Release this chunk's gated live events (InWindow-tagged) ahead of
	// the Closes marker — FIFO keeps them before it. The gate stays
	// open: the next chunk's backlog must not race ahead of these.
	if err := c.flushWindow(ctx, ref.Target, partition, chunkID); err != nil {
		return err
	}
	// The Closes marker belongs to exactly this partition's worker —
	// not routed through enqueueBatch's table-wide lookup, since a
	// marker carries no rows for enqueueBatch to route by key.
	return c.enqueueTo(ctx, w, nil, &pb.BatchMeta{
		Table:  ref.Target,
		LowPos: at.String(),
		Window: &pb.WindowTag{Closes: true, ChunkId: chunkID},
	})
}

// clipChunksToRange keeps only the chunks that intersect partitionRange,
// clamping each kept chunk's own Low/High to the range's bounds so a
// chunk straddling the partition boundary never sends rows outside it.
// An empty partitionRange (the unpartitioned {} zero value) matches
// everything unchanged.
func clipChunksToRange(chunks []source.Chunk, partitionRange source.Chunk) []source.Chunk {
	if partitionRange.Low == nil && partitionRange.High == nil {
		return chunks
	}
	var out []source.Chunk
	for _, ch := range chunks {
		if partitionRange.High != nil && ch.Low != nil && comparePK(ch.Low, partitionRange.High) >= 0 {
			continue // chunk starts at/after the range ends
		}
		if partitionRange.Low != nil && ch.High != nil && comparePK(ch.High, partitionRange.Low) <= 0 {
			continue // chunk ends at/before the range starts — both are half-open [Low,High)
		}
		clipped := ch
		if partitionRange.Low != nil && (ch.Low == nil || comparePK(ch.Low, partitionRange.Low) < 0) {
			clipped.Low = partitionRange.Low
		}
		if partitionRange.High != nil && (ch.High == nil || comparePK(ch.High, partitionRange.High) > 0) {
			clipped.High = partitionRange.High
		}
		out = append(out, clipped)
	}
	return out
}

// enqueueBatch queues ONE source batch on a worker's Flight stream and
// charges its share of the global flow budget. A full budget blocks here —
// the backpressure that stalls the pump and, through it, the reader. The
// charge is released when the worker's Ack covers the batch's position
// (onAck).
//
// b must be non-nil: it is a source batch, serialized ONCE as-is (no per-row
// re-encode). meta may be nil (plain live) or carry a window tag; the table
// and commit position are derived from the batch when the meta lacks them.
// OWNERSHIP: enqueueBatch always releases b on every exit.
//
// Window-lifecycle markers (the empty Closes record) are NOT this function's
// job — they go to one explicit partition owner via enqueueTo.
func (c *Coordinator) enqueueBatch(ctx context.Context, b *dataplane.Batch, meta *pb.BatchMeta) error {
	if b != nil {
		defer b.Release()
	}
	if b == nil && (meta == nil || meta.Table == "") {
		return fmt.Errorf("coordinator: marker batch requires a table in meta")
	}
	if meta == nil {
		meta = &pb.BatchMeta{}
	}
	// ONE snapshot load for this whole batch: a concurrent re-slice may
	// publish a new layout mid-batch, and routing half the rows by each
	// would split a key range across two owners (issue #312).
	rt := c.loadRouting()
	owners, ok := rt.ownersOf(meta.Table)
	if !ok {
		if b != nil {
			meta.Table = b.Table
		}
		owners, ok = rt.ownersOf(meta.Table)
		if !ok {
			return fmt.Errorf("coordinator: no worker owns table %s", meta.Table)
		}
	}

	if b == nil {
		// Window-lifecycle markers are enqueued to one explicit partition
		// owner via enqueueTo (snapshotTable does this per partition);
		// enqueueBatch carries source batches only (issue #214).
		return fmt.Errorf("coordinator: enqueueBatch requires a batch; window markers go through enqueueTo")
	}

	if len(owners) == 1 {
		return c.enqueueTo(ctx, owners[0], b, meta)
	}

	// Partitioned table: one cycle id for the whole binlog batch. Every
	// sub-batch sent below shares it, so the staged deliveries group back
	// into the one cycle that must commit atomically (WK-001 C5). The
	// expected count is the number of partitions that actually have rows
	// — a nil sub-batch is never sent, so it is never expected.
	if meta.BatchId == 0 {
		meta.BatchId = c.batchSeq.Add(1)
	}
	// Partitioned table: split b's rows by primary-key range, one
	// sub-batch per owning partition — the SAME ranges the DBLog
	// snapshot uses (c.partitionRanges), so a key is always routed to
	// the one worker that also owns it during bootstrap.
	pk := c.primaryKeyFor(meta.Table)
	if len(pk) == 0 {
		return fmt.Errorf("coordinator: table %s has %d partition owners but no primary key to route by", meta.Table, len(owners))
	}
	ranges := rt.rangesOf(meta.Table)
	if len(ranges) != len(owners) {
		return fmt.Errorf("coordinator: table %s: %d partition ranges for %d owners", meta.Table, len(ranges), len(owners))
	}
	reader, err := transport.NewBatchReader(b.Record, pk)
	if err != nil {
		return fmt.Errorf("coordinator: table %s: partition routing: %w", meta.Table, err)
	}
	nrows := reader.NumRows()
	owner := make([]int, nrows)
	for i := 0; i < nrows; i++ {
		p := partitionOwner(ranges, reader.Key(i))
		if p < 0 {
			return fmt.Errorf("coordinator: table %s: row %d's key %v matches no partition range", meta.Table, i, reader.Key(i))
		}
		owner[i] = p
	}
	subBatches, err := splitByOwner(ctx, b.Record, owner, len(owners))
	if err != nil {
		return fmt.Errorf("coordinator: table %s: split by partition: %w", meta.Table, err)
	}
	total := 0
	for _, sub := range subBatches {
		if sub != nil {
			total++
		}
	}
	cycleOwners := make([]string, 0, total)
	for p, sub := range subBatches {
		if sub != nil {
			cycleOwners = append(cycleOwners, owners[p].name)
		}
	}
	// Only a staging sink commits per cycle; for any other concurrent sink
	// (ClickHouse, Couchbase) the workers commit their own sub-batches, so
	// no cycle is tracked and none can leak.
	if c.stagesCycles() {
		c.staged.expect(core.TableRef{Target: meta.Table}, meta.BatchId, cycleOwners)
	}
	for p, sub := range subBatches {
		if sub == nil {
			continue // no rows for this partition in this batch
		}
		subMeta := cloneBatchMeta(meta)
		subMeta.HighPos = "" // recomputed per sub-batch below
		subBatch := &dataplane.Batch{Table: b.Table, Record: sub, Watermark: b.Watermark, Mode: b.Mode}
		if err := c.enqueueTo(ctx, owners[p], subBatch, subMeta); err != nil {
			// enqueueTo already released subBatch (and its Record — the same
			// pointer as sub), so do NOT release sub again; release only the
			// sub-batches not yet sent (issue #217).
			releaseRecords(subBatches[p+1:])
			return err
		}
	}
	return nil
}

// enqueueTo serializes and queues ONE batch (or a nil-record marker) on
// ONE worker's Flight stream, charging its share of the global flow
// budget. A full budget blocks here — the backpressure that stalls the
// pump and, through it, the reader. The charge is released when the
// worker's Ack covers the batch's position (onAck).
//
// OWNERSHIP: releases b (if non-nil) on every exit — the caller must not
// use b again after this returns, matching enqueueBatch's existing
// single-owner contract.
func (c *Coordinator) enqueueTo(ctx context.Context, w *workerState, b *dataplane.Batch, meta *pb.BatchMeta) error {
	if b != nil {
		defer b.Release()
	}
	// A partitioned table's sub-batches all share one cycle id, assigned
	// once by enqueueBatch (WK-001 C5): the staged cycle key. Every other
	// caller gets its own id here.
	if meta.BatchId == 0 {
		meta.BatchId = c.batchSeq.Add(1)
	}
	// The coordinator owns the table's commit mode and states it per batch:
	// a staging sink with >1 owner stages (the coordinator commits the
	// cycle), everything else commits directly. Setting it here, at send
	// time, keeps every owner on the same mode even when the table changes
	// mode under it (1 owner → N on a re-slice, issue #312).
	meta.Staged = c.isStagedTable(meta.Table)

	var body []byte
	var metaBytes []byte
	var err error
	if b == nil {
		// Resolve the canonical schema for typed wire encoding of the
		// zero-row marker record. A marker whose table is not in refs would
		// otherwise encode with a zero-value schema (0 data columns) and be
		// rejected downstream with an error pointing at the wrong place.
		var cs core.Schema
		found := false
		for _, ref := range c.refs {
			if ref.Target == meta.Table {
				cs = c.canonical[ref.Source]
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("coordinator: marker batch: table %q has no canonical schema (not in refs)", meta.Table)
		}
		body, metaBytes, err = transport.EncodeBatch(nil, cs, meta, nil)
		if err != nil {
			return err
		}
	} else {
		// The batch already carries the wire schema (data + metadata
		// columns); serialize it whole. The commit position is the last
		// row's __pos — ack truncation frees the batch once the worker
		// commits at or past it.
		if meta.HighPos == "" {
			reader, rerr := transport.NewBatchReader(b.Record, nil)
			if rerr != nil {
				return rerr
			}
			if reader.NumRows() > 0 {
				meta.HighPos = reader.Position(reader.NumRows() - 1)
			}
		}
		body, err = transport.EncodeRecord(b.Record)
		if err != nil {
			return err
		}
		metaBytes, err = proto.Marshal(meta)
		if err != nil {
			return fmt.Errorf("coordinator: marshal batch meta: %w", err)
		}
	}
	n := int64(len(body) + len(metaBytes))
	if err := c.budget.acquire(ctx, w.name, n); err != nil {
		return err
	}
	// Marker batches (window closes) carry their position in LowPos.
	posStr := meta.HighPos
	if posStr == "" {
		posStr = meta.LowPos
	}
	var high position.Position
	if posStr != "" {
		high, err = c.src.ParsePosition(posStr)
		if err != nil {
			c.budget.release(w.name, n)
			return fmt.Errorf("coordinator: batch %s position %q: %w", meta.Table, posStr, err)
		}
	}
	select {
	case w.queue <- queuedBatch{id: meta.BatchId, body: body, meta: metaBytes}:
		if idx := c.indexOf(w.name); idx != nil {
			idx.add(inflightBatch{id: meta.BatchId, table: meta.Table, high: high, bytes: n, oversized: c.budget.isOversized(n)})
		}
		return nil
	case <-ctx.Done():
		c.budget.release(w.name, n)
		return ctx.Err()
	}
}

// primaryKeyFor returns the primary key columns for the table targeted
// by target (a TARGET table name, matching c.refs' shape).
func (c *Coordinator) primaryKeyFor(target string) []string {
	for _, ref := range c.refs {
		if ref.Target == target {
			return ref.PrimaryKey
		}
	}
	return nil
}

// cloneBatchMeta returns a shallow copy of meta — enqueueTo mutates
// BatchId/HighPos on the instance it's given, and a marker sent to every
// partition owner (or a sub-batch's per-partition meta) must not share
// one struct across concurrent-ish sends.
func cloneBatchMeta(meta *pb.BatchMeta) *pb.BatchMeta {
	return proto.Clone(meta).(*pb.BatchMeta)
}

// partitionOwner returns the index of the partition range containing key
// — the range r such that r.Low <= key < r.High (nil bounds are open).
// Ranges must be contiguous and ordered (as Partitions/the single-range
// default always produce); returns -1 only if no range matches, which
// never happens for a correctly resolved table.
func partitionOwner(ranges []source.Chunk, key []any) int {
	if len(ranges) == 1 {
		return 0 // the common, unpartitioned case — skip the comparison
	}
	for i, r := range ranges {
		if r.Low != nil && comparePK(key, r.Low) < 0 {
			continue
		}
		if r.High != nil && comparePK(key, r.High) >= 0 {
			continue
		}
		return i
	}
	return -1
}

// comparePK compares two same-shaped primary-key tuples column by
// column, the same row-constructor semantics the chunkers' own bounds
// comparisons use (lexicographic over the tuple). Supports the ordered
// scalar types a partition key can be: int64-family, float64, and
// string/[]byte (partitioning today only supports a single-column key —
// see source.PartitionSource — so in practice these tuples always have
// exactly one element, but the comparison is written for the general
// tuple shape to match Chunk's own []any convention).
func comparePK(a, b []any) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if c := compareScalar(a[i], b[i]); c != 0 {
			return c
		}
	}
	return len(a) - len(b)
}

func compareScalar(a, b any) int {
	// Exact integer comparison first: float64 cannot represent adjacent
	// int64 values above 2^53, so a float round-trip would collapse distinct
	// keys and boundaries and route a key to the wrong worker.
	if ai, aok := toInt64(a); aok {
		if bi, bok := toInt64(b); bok {
			switch {
			case ai < bi:
				return -1
			case ai > bi:
				return 1
			default:
				return 0
			}
		}
	}
	af, aok := toFloat(a)
	bf, bok := toFloat(b)
	if aok && bok {
		switch {
		case af < bf:
			return -1
		case af > bf:
			return 1
		default:
			return 0
		}
	}
	as, bs := fmt.Sprint(a), fmt.Sprint(b)
	return strings.Compare(as, bs)
}

// toInt64 extracts an exact integer when v is an integer type. A float is not
// coerced here: a float64 that is integral may still be an approximation of a
// larger int64.
func toInt64(v any) (int64, bool) {
	switch t := v.(type) {
	case int64:
		return t, true
	case int32:
		return int64(t), true
	case int:
		return int64(t), true
	default:
		return 0, false
	}
}

func toFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case int64:
		return float64(t), true
	case int32:
		return float64(t), true
	case int:
		return float64(t), true
	case float64:
		return t, true
	case float32:
		return float64(t), true
	default:
		return 0, false
	}
}

// splitByOwner filters rec into len(nOwners) sub-records, one per
// partition index in owner (owner[i] is the partition row i belongs to).
// A partition with zero matching rows gets a nil entry (skipped by the
// caller) rather than an empty-but-non-nil record — enqueueTo's
// zero-row marker path is for markers only, not empty data batches.
func splitByOwner(ctx context.Context, rec arrow.RecordBatch, owner []int, nOwners int) ([]arrow.RecordBatch, error) {
	out := make([]arrow.RecordBatch, nOwners)
	for p := 0; p < nOwners; p++ {
		idxBuilder := array.NewInt64Builder(memory.DefaultAllocator)
		for i, o := range owner {
			if o == p {
				idxBuilder.Append(int64(i))
			}
		}
		if idxBuilder.Len() == 0 {
			idxBuilder.Release()
			continue
		}
		idxArr := idxBuilder.NewInt64Array()
		idxBuilder.Release()
		datum, err := compute.Take(ctx, *compute.DefaultTakeOptions(),
			&compute.RecordDatum{Value: rec}, &compute.ArrayDatum{Value: idxArr.Data()})
		idxArr.Release()
		if err != nil {
			releaseRecords(out)
			return nil, fmt.Errorf("partition %d: %w", p, err)
		}
		// Take returns a *RecordDatum for a record input; guard the assertion
		// so a future kernel change cannot panic the coordinator (issue #211).
		rd, ok := datum.(*compute.RecordDatum)
		if !ok {
			datum.Release()
			releaseRecords(out)
			return nil, fmt.Errorf("partition %d: unexpected Take datum %T", p, datum)
		}
		out[p] = rd.Value
	}
	return out, nil
}

// releaseRecords releases every non-nil record in a splitByOwner result.
func releaseRecords(recs []arrow.RecordBatch) {
	for _, r := range recs {
		if r != nil {
			r.Release()
		}
	}
}

// onHello processes a worker's ready Hello: it carries the phase and the
// committed positions the worker read from Iceberg. A Hello with a stale
// epoch is a zombie from a superseded generation — reject it (design §5.5).
func (c *Coordinator) onHello(worker string, h *pb.Hello) {
	c.mu.Lock()
	defer c.mu.Unlock()
	w, ok := c.workers[worker]
	if !ok {
		c.log.Warn("coordinator: Hello for unknown worker", "worker", worker)
		return
	}
	if h.Epoch != w.epoch {
		c.log.Warn("coordinator: stale Hello epoch", "worker", worker, "have", w.epoch, "got", h.Epoch)
		return
	}
	w.committed = h.Committed
	c.log.Info("worker hello", "worker", worker, "phase", h.Phase.String(),
		"committed", len(h.Committed))
}

// onSchemaDrift records a worker's schema-drift report, so the log and the
// event trail surface WHY a worker is stopping instead of a bare
// CrashLoopBackOff (issue #272).
func (c *Coordinator) onSchemaDrift(worker string, d *pb.SchemaDrift) {
	if d == nil {
		return
	}
	c.log.Error("coordinator: worker schema drift",
		"worker", worker, "table", d.Table, "column", d.Column, "kind", d.Kind)
	c.emitLog(eventlog.KindSchemaDrift, map[string]any{
		"worker": worker, "table": d.Table, "column": d.Column, "kind": d.Kind,
	})
}

// onAck advances the worker's position index: every head batch the commit
// covers leaves the flight window and returns its bytes to the budget.
func (c *Coordinator) onAck(worker string, ack *pb.Ack) {
	c.supervisor.noteAck(worker, time.Now())
	pos, err := c.src.ParsePosition(ack.Position)
	if err != nil {
		// The position format is a shared contract; a worker that cannot
		// produce a valid one is broken, and the ack's batch would sit at the
		// head of the index forever, leaking its budget charge with no visible
		// error (issue #210). Terminate for replay instead of continuing.
		c.log.Warn("coordinator: ack position", "worker", worker, "err", err)
		c.fail(fmt.Errorf("coordinator: worker %s: unparsable ack position %q: %w", worker, ack.Position, err))
		return
	}
	idx := c.indexOf(worker)
	if idx == nil {
		// The worker's index is gone (unregistered on a rolled-back scale):
		// nothing to truncate, and it holds no budget this ack would free.
		return
	}
	freed, freedOversized, popped := idx.truncate(ack.Table, pos)
	if freed > 0 {
		c.budget.release(worker, freed)
	}
	if freedOversized {
		c.budget.clearOversized(worker)
	}
	// The ack is durable evidence: the popped batches can leave the sent list
	// and need no redelivery (issue #235). c.workers is immutable after boot.
	if w := c.workers[worker]; w != nil {
		w.dropSent(popped)
	}
	// The ack is evidence of a durable commit: record it and recompute the
	// pipeline-wide minimum the source's retention may advance to. Keyed by
	// worker, so a partitioned table's N partitions each hold their own
	// position and the minimum is the lagging one (WK-001 §2.2).
	//
	// EXCEPT on a staged table, where the ack precedes the coordinator's
	// CommitStaged: there the confirmed position advances from
	// commitStagedCycle, after the cycle is durable, so source retention
	// never passes data the cycle still owes (WK-001 §2.2/F2). The ack still
	// released the budget above.
	if !c.isStagedTable(ack.Table) {
		c.recordConfirmed(worker, pos)
	}
	var commitLatencyMs float64
	if d := ack.GetCommitDuration(); d != nil {
		commitLatencyMs = float64(d.AsDuration().Milliseconds())
	}
	c.recordTableStats(worker, ack.Table, int64(ack.Rows), int64(ack.Deletes), commitLatencyMs, time.Now())
	if c.metrics != nil {
		c.metrics.InflightBytes.WithLabelValues(worker).Set(float64(c.budget.inFlight(worker)))
		c.metrics.CommitsTotal.WithLabelValues(ack.Table).Inc()
	}
	c.log.Info("worker ack", "worker", worker, "table", ack.Table,
		"rows", ack.Rows, "position", ack.Position, "inflight", c.budget.inFlight(worker))
	// The audit trail upload is a synchronous S3 put; on the ack hot path a
	// slow endpoint would delay budget release and trip the supervisor's
	// stale-ack resets (audit #15). Fire it and forget.
	go func() {
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

// assignmentFor builds one worker's table assignment with its own ticket.
// The table schema travels as Arrow IPC derived from the canonical schema —
// the same typed discipline as the Flight data plane, no JSON on the wire.
func (c *Coordinator) assignmentFor(w *workerState) (*pb.CoordinatorMessage, error) {
	// The assignment's epoch is the worker's CURRENT generation: the worker
	// echoes it in its ready Hello, and onHello rejects any other value. A
	// hardcoded 1 diverges from w.epoch's lifecycle (0 on first boot, ++ on
	// every reset) and silently drops every ready Hello's committed map
	// (audit #6).
	c.mu.Lock()
	epoch := w.epoch
	c.mu.Unlock()
	dsn, err := c.snapshotDSN()
	if err != nil {
		return nil, err
	}
	assign := &pb.Assignment{
		WorkerName: w.name,
		Epoch:      epoch,
		RunId:      c.runID,
		Ticket:     w.ticket,
		SourceKind: c.cfg.Spec.Source.Kind,
		SourceDsn:  dsn,
		ChunkSize:  uint32(c.cfg.ChunkSize),
		Batching: &pb.BatchConfig{
			MaxInterval: durationpb.New(2 * time.Second),
		},
	}
	// A structured postgres source that the DSN cannot fully express (an SSH
	// tunnel has a DialFunc, not a DSN) travels as its JSON block so the
	// worker rebuilds the same source (#170). A scoped SnapshotURI wins: the
	// operator deliberately gave the worker a directly reachable read-only
	// connection, and the replication credential stays coordinator-side
	// (D-CD1) — so the block is withheld then.
	if pg := c.cfg.Spec.Source.Postgres; pg != nil && c.cfg.Spec.Source.SnapshotURI == "" {
		b, err := json.Marshal(pg)
		if err != nil {
			return nil, fmt.Errorf("coordinator: postgres config: %w", err)
		}
		assign.Postgres = b
	}
	for _, ref := range w.refs {
		schemaB, err := transport.EncodeTableSchema(c.canonical[ref.Source])
		if err != nil {
			return nil, fmt.Errorf("coordinator: schema %s: %w", ref.Source, err)
		}
		ta := &pb.TableAssignment{
			SourceTable:       ref.Source,
			TargetTable:       ref.Target,
			WriteMode:         pb.WriteMode_WRITE_MODE_UPSERT,
			PrimaryKey:        ref.PrimaryKey,
			CreateIfNotExists: true,
			SchemaArrow:       schemaB,
			// A partitioned table on a staging sink: the worker stages its
			// data files and the coordinator commits the cycle (WK-001 C5).
			// This is the mode at ATTACH time; live batches carry the current
			// mode per batch (BatchMeta.staged), so a table that becomes
			// partitioned under this worker (issue #312) is staged from the
			// next batch even though this flag is stale.
			Staged: c.isStagedTable(ref.Target),
		}
		// The table's write shape travels with the assignment so the worker's
		// collapse and the coordinator's DDL agree: the per-table write mode
		// (a hardcoded UPSERT here silently flipped append tables to upsert
		// semantics, audit #10) and the cast/metadata JSON (a dropped cast
		// policy made the worker create a table divergent from the
		// coordinator's, audit #8).
		tbl, ok := c.specForSource(ref.Source)
		if ok {
			ta.WriteMode = writeModeToPB(tbl.WriteMode.ChangeMode())
			cast, cerr := coreCastOf(tbl)
			if cerr != nil {
				return nil, cerr
			}
			if castB, err := json.Marshal(cast); err != nil {
				return nil, fmt.Errorf("coordinator: cast %s: %w", ref.Source, err)
			} else {
				ta.CastPolicy = castB
			}
			if metaB, err := json.Marshal(tbl.Metadata); err != nil {
				return nil, fmt.Errorf("coordinator: metadata %s: %w", ref.Source, err)
			} else {
				ta.Metadata = metaB
			}
			// The source read projection travels too: the worker builds the
			// snapshot chunk SELECT, so it needs the column list (#162) and
			// the compiled predicate (#163). The predicate is shipped as the
			// structured filter and compiled by the worker's source, so the
			// same code path resolves it in both modes.
			ta.ColumnFilter = tbl.ColumnFilter
			// onDelete travels with the assignment: the worker applies it to
			// an append-only table (issue #264).
			ta.OnDelete = string(tbl.OnDelete)
			// The chunk column travels too: a partitioned table is chunked
			// by its (single-column) key even without an explicit
			// chunkColumn, and the worker's chunker must match the
			// coordinator's or the key bounds would be read as CTID.
			ta.ChunkColumn = tbl.ChunkColumn
			if ta.ChunkColumn == "" && tbl.WorkerCount() > 1 && len(ref.PrimaryKey) == 1 {
				ta.ChunkColumn = ref.PrimaryKey[0]
			}
			if tbl.Filter != nil {
				filterB, ferr := json.Marshal(tbl.Filter)
				if ferr != nil {
					return nil, fmt.Errorf("coordinator: filter %s: %w", ref.Source, ferr)
				}
				ta.Filter = filterB
			}
		}
		// Broadcast reference joins travel with the assignment: the worker
		// owns the join, the coordinator only forwards the declaration.
		for _, t := range c.cfg.Spec.Tables {
			if t.Source != ref.Source {
				continue
			}
			for _, e := range t.Enrich {
				ta.Enrich = append(ta.Enrich, &pb.EnrichRef{
					Table:           e.Table,
					SourceUri:       e.Source.URI,
					SourceQuery:     e.Source.Query,
					On:              e.On,
					Select:          e.Select,
					As:              e.As,
					JoinType:        e.JoinType,
					Refresh:         e.Refresh,
					OnColdStart:     e.OnColdStart,
					BufferMaxEvents: int64(e.BufferLimits.MaxEvents),
					BufferMaxWait:   e.BufferLimits.MaxWait,
				})
			}
			break
		}
		assign.Tables = append(assign.Tables, ta)
	}
	return &pb.CoordinatorMessage{Msg: &pb.CoordinatorMessage_Assign{Assign: assign}}, nil
}

// specForSource finds the spec table for a source name.
func (c *Coordinator) specForSource(src string) (spec.Table, bool) {
	for _, t := range c.cfg.Spec.Tables {
		if t.Source == src {
			return t, true
		}
	}
	return spec.Table{}, false
}

// writeModeToPB maps the dataplane write mode onto the wire enum.
func writeModeToPB(m dataplane.WriteMode) pb.WriteMode {
	if m == dataplane.AppendMode {
		return pb.WriteMode_WRITE_MODE_APPEND
	}
	return pb.WriteMode_WRITE_MODE_UPSERT
}

// coreCastOf parses the spec's cast map into the policy the coordinator's
// DDL and the worker's writes must both apply. Parse errors are ignored the
// same way the collapsed runner ignores them (the cast is re-validated on
// the write path); the coordinator must not diverge from the runner.
// snapshotDSN returns the connection string the WORKER uses for the snapshot
// chunk SELECT: the scoped read-only SnapshotURI when set, else the full
// source URI (pre-scoping behavior). When the source is configured with the
// structured source.postgres block instead of a URI, the block is rendered to
// its libpq DSN here so the worker can still open the query connection.
//
// An SSH-tunneled structured source cannot be expressed by this DSN. When
// SnapshotURI is unset, assignmentFor ships the whole source.postgres block
// instead (Assignment.postgres, issue #170) and the worker builds the tunnel
// from it; the DSN below is then only a fallback the worker ignores. The
// worker never opens a replication connection, so a deployment can grant it a
// SELECT-only user and keep the replication credential coordinator-side
// (D-CD1).
func (c *Coordinator) snapshotDSN() (string, error) {
	if u := c.cfg.Spec.Source.SnapshotURI; u != "" {
		return u, nil
	}
	if u := c.cfg.Spec.Source.URI; u != "" {
		return u, nil
	}
	if pg := c.cfg.Spec.Source.Postgres; pg != nil {
		return pg.DSN(), nil
	}
	return "", nil
}

// surfaceWarnings logs the advisory warnings from source introspection and
// cast resolution at boot — never swallowed, matching the runner (the
// core.Warning contract is operator-facing).
func (c *Coordinator) surfaceWarnings(table string, warns []core.Warning) {
	for _, w := range warns {
		c.log.Warn("schema", "table", table, "warning", w.Message)
	}
}

// coreCastOf parses the table's declared cast policy, failing loud: a
// swallowed parse error would ship an empty policy, creating a sink table
// whose types diverge from the spec (audit #8). The source Introspect
// validates the same string at boot, so this cannot normally fail — but the
// coordinator must not degrade silently if it ever does.
func coreCastOf(tbl spec.Table) (core.CastPolicy, error) {
	cast, err := core.ParseCastPolicy(tbl.Cast)
	if err != nil {
		return core.CastPolicy{}, fmt.Errorf("coordinator: table %s cast: %w", tbl.Target, err)
	}
	return cast, nil
}

// resumeFrom reads cdc.position per target table; the minimum across tables
// is the resume point, tables without one need the snapshot.
func (c *Coordinator) resumeFrom(ctx context.Context, refs []source.TableRef) (position.Position, []source.TableRef, error) {
	var positions []position.Position
	var needsSnapshot []source.TableRef
	byTarget := make(map[string]position.Position, len(refs))
	for _, ref := range refs {
		// The expected partition count travels to the sink: a per-partition
		// Position() must not return a MinSafe over an incomplete owner set,
		// or an owner with no committed position yet is resumed past (§2.6).
		ref.OwnerCount = len(c.loadRouting().owners[ref.Target])
		pos, err := c.snk.Position(ctx, ref)
		if err != nil {
			return nil, nil, fmt.Errorf("coordinator: %s: %w", ref.Target, err)
		}
		if pos != "" {
			p, err := c.src.ParsePosition(pos)
			if err != nil {
				return nil, nil, fmt.Errorf("coordinator: %s cdc.position %q: %w", ref.Target, pos, err)
			}
			positions = append(positions, p)
			byTarget[ref.Target] = p
		} else {
			needsSnapshot = append(needsSnapshot, ref)
		}
	}
	if len(positions) == 0 {
		return nil, needsSnapshot, nil
	}
	// MinSafe: an incomparable pair (should not happen for one source) is an
	// error — guessing a minimum could resume past uncommitted data (P1).
	best, err := position.MinSafe(positions)
	if err != nil {
		return nil, nil, fmt.Errorf("coordinator: %w", err)
	}
	// Streams ahead of the resume point replay from it (idempotent under
	// upsert); naming them makes a crash-recovery replay observable (#155).
	if recovery := position.Ahead(best, byTarget); len(recovery) > 0 {
		c.log.Info("crash recovery: streams ahead of the resume point replay from it",
			"from", best.String(), "streams", recovery)
	}
	return best, needsSnapshot, nil
}

// ── gRPC control plane ───────────────────────────────────────────────

type controlServer struct {
	pb.UnimplementedUrutauControlServer
	c *Coordinator
}

// Session accepts one worker: the Hello names the group it claims (unknown
// names and second connects are rejected; epoch validation arrives with
// supervision). The server then streams assignments and collects acks until
// the worker goes.
func (s *controlServer) Session(stream pb.UrutauControl_SessionServer) (retErr error) {
	c := s.c
	msg, err := stream.Recv()
	if err != nil {
		return err
	}
	hello := msg.GetHello()
	if hello == nil {
		return errors.New("coordinator: first worker message must be Hello")
	}
	// An ephemeral maintenance worker is a worker, but has no data plane:
	// route it to the maintenance scheduler instead of the assignment/ack
	// path below.
	if hello.Maintenance {
		return c.maintenanceSession(stream, hello)
	}

	c.mu.Lock()
	w, known := c.workers[hello.WorkerName]
	if known && w.attached {
		c.mu.Unlock()
		return fmt.Errorf("coordinator: worker %q already connected", hello.WorkerName)
	}
	// The supervisor cancels this ctx to force a reset; the worker sees the
	// stream die and suicides.
	sessCtx, sessCancel := context.WithCancel(stream.Context())
	sess := &workerSession{
		out:  make(chan *pb.CoordinatorMessage, 16),
		done: make(chan error, 1),
	}
	// Publish the session BEFORE signaling ready: the ready send
	// happens-before run's receive, so run may use w.out the moment it
	// wakes — attaching after the signal is a data race.
	if known {
		w.out, w.attached = sess.out, true
		w.cancel = sessCancel
	}
	c.mu.Unlock()
	if known {
		// noteAttach takes the supervisor lock; calling it under c.mu would
		// invert the order supervisor.tick uses (supervisor.mu → c.mu) and
		// deadlock the two (audit #3).
		c.supervisor.noteAttach(hello.WorkerName)
		c.pushDashState() // the worker attached
	}
	if !known {
		sessCancel()
		return fmt.Errorf("coordinator: unknown worker %q", hello.WorkerName)
	}

	defer func() {
		c.mu.Lock()
		w.attached, w.out, w.cancel = false, nil, nil
		c.mu.Unlock()
		sessCancel()
		c.signalSessionEnd(hello.WorkerName, retErr)
	}()

	select {
	case c.ready <- struct{}{}:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	c.log.Info("worker session", "worker", hello.WorkerName)

	// Assignment on every attach (including after a reset): the worker
	// waits for it before opening Flight, and a resurrected worker needs a
	// fresh one.
	if msg, err := c.assignmentFor(w); err != nil {
		return err
	} else {
		select {
		case sess.out <- msg:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}

	// Recv loop: acks and worker errors.
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				sess.done <- err
				return
			}
			switch m := msg.Msg.(type) {
			case *pb.WorkerMessage_Ack:
				c.onAck(hello.WorkerName, m.Ack)
			case *pb.WorkerMessage_Staged:
				c.onStagedBatch(hello.WorkerName, m.Staged)
			case *pb.WorkerMessage_Hello:
				c.onHello(hello.WorkerName, m.Hello)
			case *pb.WorkerMessage_WorkerMetrics:
				c.onWorkerMetrics(m.WorkerMetrics)
			case *pb.WorkerMessage_ChunkReady:
				// A full chunkReady buffer with no draining snapshot loop
				// (stale replies after a reset) must not wedge this recv
				// goroutine; it aborts on session cancellation instead.
				select {
				case c.chunkReady <- m.ChunkReady:
				case <-sessCtx.Done():
					sess.done <- context.Canceled
					return
				}
			case *pb.WorkerMessage_Error:
				sess.done <- errors.New("coordinator: worker error: " + m.Error.Detail)
				return
			case *pb.WorkerMessage_SchemaDrift:
				c.onSchemaDrift(hello.WorkerName, m.SchemaDrift)
			}
		}
	}()

	// Send loop: assignments and future control messages.
	for {
		select {
		case m := <-sess.out:
			if err := stream.Send(m); err != nil {
				return err
			}
		case err := <-sess.done:
			return err
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-sessCtx.Done():
			return errSessionReset
		}
	}
}

// errSessionReset marks a session cancelled by the supervisor; not a worker
// failure, so it must not kill the run via sessionErrs.
var errSessionReset = errors.New("session reset")

// signalSessionEnd reports a session's terminal state to the run loop.
//
// A supervisor reset is not a worker failure, and neither is the death of a
// worker mid-reset (it suicides on channel loss); the supervisor owns the
// outcome. The one exception is a snapshot in progress: the worker's
// in-memory window died with the session, so the protocol cannot continue —
// either it waits out AckTimeout/MaxResets, or a stale ChunkReady from the
// old generation satisfies waitChunkReady against an empty window (a
// silently incomplete snapshot). Fail the run instead, so it restarts and
// re-snapshots cleanly (CD-5).
func (c *Coordinator) signalSessionEnd(worker string, retErr error) {
	// A lost worker leaves the staged cycles it owed permanently incomplete:
	// discard them (never commit a partial cycle). The run terminates below
	// and replays every partition from the committed position, so no later
	// cycle may be committed over the gap.
	if n := c.staged.discardWorker(worker); n > 0 {
		c.log.Warn("coordinator: discarded staged cycles of lost worker", "worker", worker, "cycles", n)
	}
	c.pushDashState() // the worker is no longer attached
	// A session that ends with context.Canceled — the Pod was deleted, or the
	// client's stream closed — while the worker still owes batches strands
	// them: the pump keeps routing to the detached owner until a re-slice
	// removes it, and no session will ever deliver them, so a re-slice's drain
	// waits on them forever and the flip never commits (issue #363). The other
	// terminal paths below already fail the run; this one does not, so fail it
	// here for a clean replay — the restart replays every partition from the
	// committed position, recovering the stranded batches. A supervisor reset
	// (pending) is excluded: that worker reconnects and redelivers.
	if errors.Is(retErr, context.Canceled) && !c.supervisor.isPending(worker) && c.workerOwes(worker) {
		c.sessionErrs <- fmt.Errorf("coordinator: worker %s session lost owing work: %w", worker, retErr)
		return
	}
	// A session cancelled by the coordinator — a supervisor reset or a
	// re-slice retiring an owner — surfaces as errSessionReset OR, when the
	// recv goroutine wins the race, as context.Canceled. Neither is a worker
	// failure; only a real stream error is. Treating the cancel as a failure
	// ended the whole run the moment a scale-in retired an owner.
	if !errors.Is(retErr, errSessionReset) && !errors.Is(retErr, context.Canceled) && !c.supervisor.isPending(worker) {
		c.sessionErrs <- retErr
	} else if c.snapshotActive.Load() {
		c.sessionErrs <- fmt.Errorf("coordinator: worker %s session lost during snapshot: %w", worker, retErr)
	}
}

// workerOwes reports whether a worker still holds undelivered queued batches
// or delivered-but-unacked in-flight batches — work a re-slice's drain would
// wait on, and that no detached session can drain.
func (c *Coordinator) workerOwes(worker string) bool {
	c.mu.Lock()
	w := c.workers[worker]
	c.mu.Unlock()
	if w == nil {
		return false
	}
	return len(w.queue) > 0 || c.inFlight(worker) > 0
}

// workerSession is one connected worker's session-local surface; the group's
// durable state lives in workerState.
type workerSession struct {
	out  chan *pb.CoordinatorMessage
	done chan error
}

// Control is the urgent-signal plane on the same ClientConn as Session and
// DoGet. The first frame must be a Hello naming the worker so urgent
// signals route to the right stream. No data rides here.
func (s *controlServer) Control(stream pb.UrutauControl_ControlServer) (retErr error) {
	c := s.c
	msg, err := stream.Recv()
	if err != nil {
		return err
	}
	hello := msg.GetHello()
	if hello == nil {
		return errors.New("coordinator: Control first message must be Hello")
	}
	c.mu.Lock()
	w, known := c.workers[hello.WorkerName]
	if known {
		w.control = stream
	}
	c.mu.Unlock()
	if !known {
		return fmt.Errorf("coordinator: unknown worker %q", hello.WorkerName)
	}
	defer func() {
		c.mu.Lock()
		w.control = nil
		c.mu.Unlock()
		// A worker mid-reset suicides and closes this stream too; only a
		// non-reset death is a real session failure (signalSessionEnd owns
		// the reset/snapshot rule).
		c.signalSessionEnd(hello.WorkerName, retErr)
	}()
	// The worker never writes again; its death is the stream ending.
	<-stream.Context().Done()
	return stream.Context().Err()
}

// gracefulShutdown tells every connected worker to drain: flush + commit +
// ack what is in flight, then exit 0 (design §5.3.2). Called on shutdown
// and before terminal exits.
func (c *Coordinator) gracefulShutdown() {
	for _, w := range c.workers {
		c.mu.Lock()
		ctrl := w.control
		c.mu.Unlock()
		if ctrl == nil {
			continue
		}
		msg := &pb.ControlMessage{Msg: &pb.ControlMessage_Shutdown{
			Shutdown: &pb.Shutdown{
				Grace: durationpb.New(30 * time.Second),
				Drain: true,
			},
		}}
		if err := ctrl.Send(msg); err != nil {
			c.log.Warn("coordinator: shutdown send", "worker", w.name, "err", err)
		}
	}
}

// ── Flight data plane ────────────────────────────────────────────────

type flightServer struct {
	flight.BaseFlightServer
	c *Coordinator
}

// DoGet streams the worker's queued batches; the ticket (from its
// Assignment) selects which queue. Each FlightData carries one complete IPC
// stream in DataBody and a BatchMeta proto in AppMetadata — both produced
// at enqueue time, so the server only moves bytes.
//
// A batch that has left the queue is kept on the worker's sent list until an
// Ack covers it. On session loss the next DoGet redelivers the whole sent list
// before draining the queue, so a delivered-but-unacked batch is replayed
// rather than dropped — at-least-once (issue #235). FIFO is preserved: the
// sent list is in send order, ahead of the queue's head, and the budget charge
// is released only by the Ack that truncates past the batch.
func (s *flightServer) DoGet(req *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	w, ok := s.c.byTicket[string(req.Ticket)]
	if !ok {
		return fmt.Errorf("coordinator: unknown flight ticket %q", string(req.Ticket))
	}
	if !w.activeGet.CompareAndSwap(false, true) {
		return status.Error(codes.ResourceExhausted, "coordinator: a DoGet stream is already active for this worker")
	}
	defer w.activeGet.Store(false)

	// Redeliver everything a previous session delivered but never had acked.
	// The list is pruned by the ack, so what remains here is exactly the
	// in-flight window of a lost session.
	w.sentMu.Lock()
	redeliver := append([]queuedBatch(nil), w.sent...)
	w.sentMu.Unlock()
	for _, qb := range redeliver {
		if err := stream.Send(&flight.FlightData{
			DataHeader:  []byte("urutau-batch"),
			DataBody:    qb.body,
			AppMetadata: qb.meta,
		}); err != nil {
			return err // still on the sent list; the next session retries
		}
	}

	for {
		select {
		case qb := <-w.queue:
			// Record before sending: a batch that has left the queue must be
			// redeliverable until its ack arrives.
			w.sentMu.Lock()
			w.sent = append(w.sent, qb)
			w.sentMu.Unlock()
			if err := stream.Send(&flight.FlightData{
				DataHeader:  []byte("urutau-batch"),
				DataBody:    qb.body,
				AppMetadata: qb.meta,
			}); err != nil {
				return err // still on the sent list; the next session retries
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

// dropSent removes the batches an ack covered from the worker's sent list.
func (w *workerState) dropSent(ids []uint64) {
	if len(ids) == 0 {
		return
	}
	drop := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		drop[id] = struct{}{}
	}
	w.sentMu.Lock()
	kept := w.sent[:0]
	for _, qb := range w.sent {
		if _, ok := drop[qb.id]; !ok {
			kept = append(kept, qb)
		}
	}
	w.sent = kept
	w.sentMu.Unlock()
}

// ── Positions ─────────────────────────────────────────────────────────

func resumeOrNone(p position.Position) string {
	if p == nil {
		return "none"
	}
	return p.String()
}

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

// randTicket is the worker's Flight DoGet ticket: 128 random bits so a
// worker's stream cannot be opened by guessing "urutau/<name>" (audit #3).
// crypto/rand failure is fatal — a deterministic fallback would restore the
// guessable-ticket hole the randomness exists to close.
func randTicket() []byte {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("coordinator: crypto/rand: %v", err))
	}
	return []byte(hex.EncodeToString(b))
}

// tableNames renders a group's source tables for the audit trail.
func tableNames(refs []source.TableRef) []string {
	out := make([]string, len(refs))
	for i, r := range refs {
		out[i] = r.Source
	}
	return out
}

// sourceBatches forwards the reader's columnar batches to the pump — no
// decode, no per-row hop (G0/M4). Ownership: each batch moves to the pump,
// which gates, serializes and releases it.
func sourceBatches(ctx context.Context, rdr source.Reader) (<-chan *dataplane.Batch, <-chan error) {
	out := make(chan *dataplane.Batch, 16)
	errCh := make(chan error, 1)
	go func() {
		defer close(out)
		for {
			b, err := rdr.Next(ctx)
			if err != nil {
				errCh <- err
				return
			}
			if b == nil {
				errCh <- nil
				return
			}
			select {
			case out <- b:
			case <-ctx.Done():
				b.Release()
				return
			}
		}
	}()
	return out, errCh
}

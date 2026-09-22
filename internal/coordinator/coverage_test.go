package coordinator

// Batch 5: the coordinator's pure and state-machine helpers, exercised with
// hand-built Coordinators — no gRPC session, no k8s. The gRPC/k8s halves
// (Run/session/pump/provisionWorkers) need the heavy fixtures the Makefile's
// floor comment calls out and are covered elsewhere or not at all.

import (
	"context"
	"errors"
	"log/slog"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// errSource is a source.Source whose ParsePosition always fails.
type errSource struct{ source.Source }

func (errSource) ParsePosition(string) (position.Position, error) {
	return nil, errors.New("bad position")
}

// coordHarness is a minimal live-shaped Coordinator: one attached worker
// owning one table, a real budget and position index, a supervisor, and the
// discard logger.
func coordHarness() (*Coordinator, *workerState) {
	w := &workerState{
		name:     "w0",
		attached: true,
		refs:     []source.TableRef{{Source: "shop.orders", Target: "raw.orders", PrimaryKey: []string{"id"}}},
		queue:    make(chan queuedBatch, 8),
	}
	c := &Coordinator{
		cfg: Config{Spec: &spec.Spec{
			Pipeline: "p",
			Source:   spec.Source{Kind: "postgres"},
			Sink:     spec.Sink{Type: "iceberg"},
		}},
		log:         slog.New(slog.DiscardHandler),
		src:         fakeSource{},
		workers:     map[string]*workerState{"w0": w},
		budget:      newFlowBudget(1<<20, 1<<10),
		index:       map[string]*positionIndex{"w0": newPositionIndex("run-1")},
		confirmed:   map[string]position.Position{},
		staged:      newStagedCycles(),
		stagedLocks: map[string]*sync.Mutex{},
		runID:       "run-1",
		gateDrain:   make(chan struct{}),
	}
	c.supervisor = newSupervisor(c)
	c.publishRouting(&routing{
		owners: map[string][]*workerState{"raw.orders": {w}},
		ranges: map[string][]source.Chunk{},
	})
	return c, w
}

func testWireRecord(t *testing.T) *dataplane.Batch {
	t.Helper()
	changes := []rowchange.Change{{
		Op:       rowchange.OpInsert,
		Table:    "raw.orders",
		Key:      []any{int64(1)},
		After:    map[string]any{"id": int64(1), "v": "x"},
		Position: "0/10",
		IngestTS: time.Now(),
	}}
	cs := transport.InferSchemaFromChanges(changes)
	rec, err := transport.RecordFromChanges(changes, cs, nil)
	if err != nil {
		t.Fatalf("RecordFromChanges: %v", err)
	}
	return &dataplane.Batch{Table: "raw.orders", Record: rec, Watermark: []byte("0/10"), Mode: dataplane.UpsertMode}
}

// ── pure helpers ─────────────────────────────────────────────────────

func TestGateKey(t *testing.T) {
	if got := gateKey("raw.orders", 3); got != "raw.orders#3" {
		t.Fatalf("gateKey = %q", got)
	}
}

func TestPrimaryKeyFor(t *testing.T) {
	c := &Coordinator{refs: []source.TableRef{
		{Target: "raw.orders", PrimaryKey: []string{"id"}},
	}}
	if got := c.primaryKeyFor("raw.orders"); len(got) != 1 || got[0] != "id" {
		t.Fatalf("primaryKeyFor = %v", got)
	}
	if got := c.primaryKeyFor("nope"); got != nil {
		t.Fatalf("primaryKeyFor(unknown) = %v, want nil", got)
	}
}

func TestResumeOrNone(t *testing.T) {
	if got := resumeOrNone(nil); got != "none" {
		t.Fatalf("resumeOrNone(nil) = %q", got)
	}
	if got := resumeOrNone(position.MustLSN("0/10")); got != "0/10" {
		t.Fatalf("resumeOrNone = %q", got)
	}
}

func TestRandSuffix(t *testing.T) {
	a := randSuffix(12)
	b := randSuffix(12)
	if len(a) != 12 || len(b) != 12 {
		t.Fatalf("lengths = %d, %d; want 12", len(a), len(b))
	}
	if a == b {
		t.Fatal("two suffixes must differ")
	}
	if strings.Trim(a, "0123456789abcdef") != "" {
		t.Fatalf("suffix %q is not hex", a)
	}
}

func TestTableNames(t *testing.T) {
	got := tableNames([]source.TableRef{{Source: "a"}, {Source: "b"}})
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("tableNames = %v", got)
	}
}

func TestSupervisionConfig(t *testing.T) {
	cfg := Config{AckTimeout: 5 * time.Second, MaxResets: 3, ResetWindow: time.Minute}
	got := supervisionConfig(cfg)
	if got.AckTimeout != 5*time.Second || got.MaxResets != 3 || got.ResetWindow != time.Minute {
		t.Fatalf("supervisionConfig = %+v", got)
	}
}

func TestToFloatAndCompareScalar(t *testing.T) {
	for _, v := range []any{int64(1), int32(1), int(1), float64(1), float32(1)} {
		if _, ok := toFloat(v); !ok {
			t.Fatalf("toFloat(%T) = false", v)
		}
	}
	if _, ok := toFloat("x"); ok {
		t.Fatal("toFloat(string) must be false")
	}
	if compareScalar(int64(1), int64(2)) != -1 || compareScalar(int64(2), int64(1)) != 1 || compareScalar(int64(1), int64(1)) != 0 {
		t.Fatal("numeric compareScalar ordering is wrong")
	}
	if compareScalar("a", "b") >= 0 {
		t.Fatal("string compareScalar must order a < b")
	}
	// Adjacent int64 values above 2^53 must not collapse: a float64 round
	// trip would call them equal and route a key to the wrong worker.
	const a = int64(9007199254740992) // 2^53
	const b = int64(9007199254740993) // 2^53+1
	if compareScalar(b, a) <= 0 {
		t.Fatal("int64 compareScalar lost precision above 2^53")
	}
}

func TestTerminateReason(t *testing.T) {
	if got := terminateReason(errOperatorCancel); got != "cancelled" {
		t.Fatalf("terminateReason(cancel) = %q", got)
	}
	if got := terminateReason(errors.New("boom")); got != "crashloop" {
		t.Fatalf("terminateReason(other) = %q", got)
	}
}

func TestAnyWorkerDown(t *testing.T) {
	c, w := coordHarness()
	if c.anyWorkerDown() {
		t.Fatal("attached worker must not be down")
	}
	w.attached = false
	if !c.anyWorkerDown() {
		t.Fatal("detached worker must be down")
	}
}

func TestResolvePartitionRangesUnpartitioned(t *testing.T) {
	c := &Coordinator{}
	got, _, err := c.resolvePartitionRanges(context.Background(), spec.Table{}, source.TableRef{})
	if err != nil {
		t.Fatalf("resolvePartitionRanges: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("ranges = %v, want one unbounded range", got)
	}
}

// ── dashboard.State ──────────────────────────────────────────────────

func TestDashSummaryStatus(t *testing.T) {
	c, _ := coordHarness()
	c.startedAt = time.Now().Add(-time.Minute)
	s := dashState{c}

	if got := s.Summary(); got.Status != "streaming" || got.Workers != 1 || got.Tables != 0 {
		t.Fatalf("Summary = %+v", got)
	}
	c.snapshotActive.Store(true)
	if got := s.Summary(); got.Status != "snapshotting" {
		t.Fatalf("snapshot status = %q", got.Status)
	}
	c.snapshotActive.Store(false)
	c.workers["w0"].attached = false
	if got := s.Summary(); got.Status != "degraded" {
		t.Fatalf("degraded status = %q", got.Status)
	}
}

func TestDashWorkersStatuses(t *testing.T) {
	c, w := coordHarness()
	c.cfg.Spec = &spec.Spec{Tables: []spec.Table{
		{Source: "shop.orders", Target: "raw.orders", WriteMode: spec.WriteModeUpsert},
	}}
	c.tableStats = map[string]*tableStats{}
	c.maintStats = map[string]map[string]*maintStats{}
	c.lastAck = map[string]time.Time{"w0": time.Now().Add(-time.Second)}
	w.epoch = 4
	w.committed = map[string]string{"raw.orders": "0/10"}

	got := dashState{c}.Workers()
	if len(got) != 1 {
		t.Fatalf("Workers = %d, want 1", len(got))
	}
	if got[0].Status != "attached" || got[0].Epoch != 4 || got[0].LastAckS <= 0 {
		t.Fatalf("worker status = %+v", got[0])
	}
}

func TestDashWorkersPending(t *testing.T) {
	c, w := coordHarness()
	c.cfg.Spec = &spec.Spec{}
	c.tableStats = map[string]*tableStats{}
	c.maintStats = map[string]map[string]*maintStats{}
	c.lastAck = map[string]time.Time{}
	w.attached = false
	c.supervisor.mu.Lock()
	c.supervisor.pending["w0"] = true
	c.supervisor.mu.Unlock()

	got := dashState{c}.Workers()
	if len(got) != 1 || got[0].Status != "pending" {
		t.Fatalf("pending worker = %+v", got)
	}
}

func TestDashTablesUsesTableStats(t *testing.T) {
	c, _ := coordHarness()
	c.cfg.Spec = &spec.Spec{Tables: []spec.Table{
		{Source: "shop.orders", Target: "raw.orders", WriteMode: spec.WriteModeUpsert},
	}}
	c.tableStats = map[string]*tableStats{}
	c.maintStats = map[string]map[string]*maintStats{}
	c.recordTableStats("w0", "raw.orders", 3, 1, 5, time.Now())

	got := dashState{c}.Tables()
	if len(got) != 1 || got[0].RowsTotal != 3 || got[0].EqualityDeletes != 1 {
		t.Fatalf("Tables = %+v", got)
	}
}

// ── statusz ──────────────────────────────────────────────────────────

func TestStatuszRendersWorkers(t *testing.T) {
	c, w := coordHarness()
	c.cfg.Spec = &spec.Spec{Tables: []spec.Table{{Source: "s", Target: "raw.orders", Enrich: []spec.Enrich{{}}}}}
	w.epoch = 2
	w.committed = map[string]string{"raw.orders": "0/10"}

	rr := httptest.NewRecorder()
	c.statusz(rr, httptest.NewRequest("GET", "/statusz", nil))
	body := rr.Body.String()
	if rr.Code != 200 {
		t.Fatalf("statusz code = %d", rr.Code)
	}
	for _, want := range []string{"run_id", "point-in-time", "raw.orders"} {
		if !strings.Contains(body, want) {
			t.Fatalf("statusz body missing %q: %s", want, body)
		}
	}
}

// ── onHello / onAck ──────────────────────────────────────────────────

func TestOnHelloUpdatesCommitted(t *testing.T) {
	c, w := coordHarness()
	c.onHello("unknown", &pb.Hello{}) // must not panic
	c.onHello("w0", &pb.Hello{Epoch: 99, Committed: map[string]string{"t": "stale"}})
	if w.committed != nil {
		t.Fatal("a stale-epoch Hello must be dropped")
	}
	c.onHello("w0", &pb.Hello{Epoch: 0, Committed: map[string]string{"t": "0/5"}})
	if w.committed["t"] != "0/5" {
		t.Fatalf("committed = %v", w.committed)
	}
}

func TestOnAckTruncatesAndRecords(t *testing.T) {
	c, _ := coordHarness()
	c.tableStats = map[string]*tableStats{}
	c.maintStats = map[string]map[string]*maintStats{}
	c.index["w0"].add(inflightBatch{id: 1, table: "raw.orders", high: position.MustLSN("0/10"), bytes: 64})
	_ = c.budget.acquire(context.Background(), "w0", 64)

	c.onAck("w0", &pb.Ack{Table: "raw.orders", Position: "0/10", Rows: 5, Deletes: 1})
	if got := c.index["w0"].InFlight(); got != 0 {
		t.Fatalf("in-flight = %d, want 0", got)
	}
	if got := c.budget.inFlight("w0"); got != 0 {
		t.Fatalf("budget in-flight = %d, want 0", got)
	}
	if pos := c.confirmed["w0"]; pos == nil || pos.String() != "0/10" {
		t.Fatalf("confirmed = %v", pos)
	}
}

func TestOnAckStagedTableDoesNotAdvanceConfirmed(t *testing.T) {
	c, _ := coordHarness()
	c.tableStats = map[string]*tableStats{}
	c.maintStats = map[string]map[string]*maintStats{}
	c.snk = fakeStagedSink{}
	c.setRouteForTest("raw.orders", []*workerState{{name: "w0"}, {name: "w1"}})

	c.onAck("w0", &pb.Ack{Table: "raw.orders", Position: "0/10"})
	if len(c.confirmed) != 0 {
		t.Fatalf("confirmed = %v, want empty on a staged table", c.confirmed)
	}
}

func TestOnAckBadPositionIsIgnored(t *testing.T) {
	c, _ := coordHarness()
	c.src = errSource{}
	c.onAck("w0", &pb.Ack{Table: "raw.orders", Position: "nonsense"})
	if len(c.confirmed) != 0 {
		t.Fatalf("confirmed = %v, want empty", c.confirmed)
	}
}

// ── staged ───────────────────────────────────────────────────────────

func TestMinSafePositions(t *testing.T) {
	c, _ := coordHarness()
	if got, err := c.minSafePositions([]string{"0/20", "0/10"}); err != nil || got != "0/10" {
		t.Fatalf("minSafePositions = %q, %v; want 0/10", got, err)
	}
	if got, err := c.minSafePositions(nil); err != nil || got != "" {
		t.Fatalf("minSafePositions(empty) = %q, %v; want empty", got, err)
	}
	c.src = errSource{}
	if _, err := c.minSafePositions([]string{"x"}); err == nil {
		t.Fatal("a bad position must error")
	}
}

func TestStagedLockSerializesPerTable(t *testing.T) {
	c, _ := coordHarness()
	a1 := c.stagedLock("a")
	a2 := c.stagedLock("a")
	if a1 != a2 {
		t.Fatal("same table must share one mutex")
	}
	if a1 == c.stagedLock("b") {
		t.Fatal("different tables must not share a mutex")
	}
}

func TestOnStagedBatchGuards(t *testing.T) {
	c, _ := coordHarness()
	c.onStagedBatch("w0", nil) // nil: no-op
	c.onStagedBatch("ghost", &pb.StagedBatch{Table: "raw.orders"})
	c.onStagedBatch("w0", &pb.StagedBatch{Table: "raw.orders", Epoch: 99}) // stale
	c.onStagedBatch("w0", &pb.StagedBatch{Table: "raw.orders", Epoch: 0, Seq: 1, Descriptor_: []byte{1}, Position: "0/10"})
}

func TestFailIsNonBlocking(t *testing.T) {
	c := &Coordinator{terminate: make(chan error, 1)}
	c.fail(errors.New("first"))
	c.fail(errors.New("second")) // must not block; first is kept
	if err := <-c.terminate; err.Error() != "first" {
		t.Fatalf("terminate = %v, want first", err)
	}
}

// ── enqueue ──────────────────────────────────────────────────────────

func TestEnqueueBatchDataAndMarker(t *testing.T) {
	c, w := coordHarness()
	ctx := context.Background()

	b := testWireRecord(t)
	if err := c.enqueueBatch(ctx, b, &pb.BatchMeta{Table: "raw.orders"}); err != nil {
		t.Fatalf("enqueueBatch(data): %v", err)
	}
	if len(w.queue) != 1 {
		t.Fatalf("queue = %d, want 1", len(w.queue))
	}
	if got := c.index["w0"].InFlight(); got != 1 {
		t.Fatalf("in-flight = %d, want 1", got)
	}

	// Marker: needs refs + a canonical schema for the table. Markers go
	// through enqueueTo (issue #214).
	cs := transport.InferSchemaFromChanges([]rowchange.Change{{
		Op: rowchange.OpInsert, Table: "raw.orders", Key: []any{int64(1)},
		After: map[string]any{"id": int64(1)}, Position: "0/11", IngestTS: time.Now(),
	}})
	c.refs = []source.TableRef{{Source: "shop.orders", Target: "raw.orders", PrimaryKey: []string{"id"}}}
	c.canonical = map[string]core.Schema{"shop.orders": cs}
	if err := c.enqueueTo(ctx, w, nil, &pb.BatchMeta{Table: "raw.orders"}); err != nil {
		t.Fatalf("enqueueTo(marker): %v", err)
	}
	if len(w.queue) != 2 {
		t.Fatalf("queue after marker = %d, want 2", len(w.queue))
	}
}

// ── gate windows ─────────────────────────────────────────────────────

func TestGateWindowLifecycle(t *testing.T) {
	c, w := coordHarness()
	ctx := context.Background()

	// No window open: a batch is not gated.
	if c.gateHold(ctx, &dataplane.Batch{Table: "raw.orders"}) {
		t.Fatal("no open window must not gate")
	}

	c.openWindow("raw.orders", 0)
	if key, ok := c.openKeyForTableLocked("raw.orders"); !ok || key != "raw.orders#0" {
		t.Fatalf("openKeyForTableLocked = %q, %v", key, ok)
	}

	b := testWireRecord(t)
	if !c.gateHold(ctx, b) {
		t.Fatal("an open window must gate")
	}
	if len(c.gateBuf["raw.orders#0"]) != 1 {
		t.Fatalf("gate buffer = %d, want 1", len(c.gateBuf["raw.orders#0"]))
	}

	// A second window on another table does not affect this one.
	c.openWindow("other", 0)
	if key, ok := c.openKeyForTableLocked("other"); !ok || key != "other#0" {
		t.Fatalf("other window key = %q, %v", key, ok)
	}

	if err := c.closeWindow(ctx, "raw.orders", 0); err != nil {
		t.Fatalf("closeWindow: %v", err)
	}
	if _, ok := c.gateOn["raw.orders#0"]; ok {
		t.Fatal("closeWindow must delete the window")
	}
	if len(w.queue) != 1 {
		t.Fatalf("queue after closeWindow = %d, want 1", len(w.queue))
	}
}

func TestFlushWindowDrainsAndKeepsOpen(t *testing.T) {
	c, w := coordHarness()
	ctx := context.Background()
	c.openWindow("raw.orders", 0)
	c.gateHold(ctx, testWireRecord(t))

	if err := c.flushWindow(ctx, "raw.orders", 0, 7); err != nil {
		t.Fatalf("flushWindow: %v", err)
	}
	if !c.gateOn["raw.orders#0"] {
		t.Fatal("flushWindow must keep the window open")
	}
	if len(c.gateBuf["raw.orders#0"]) != 0 {
		t.Fatalf("gate buffer = %d, want drained", len(c.gateBuf["raw.orders#0"]))
	}
	if len(w.queue) != 1 {
		t.Fatalf("queue = %d, want 1", len(w.queue))
	}
}

func TestGateHoldContextCancelReturnsLive(t *testing.T) {
	c, _ := coordHarness()
	c.openWindow("raw.orders", 0)
	c.gateBuf["raw.orders#0"] = make([]*dataplane.Batch, gateMaxEvents)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if c.gateHold(ctx, &dataplane.Batch{Table: "raw.orders"}) {
		t.Fatal("a cancelled context while waiting on a full gate must return live")
	}
}

// ── checkpoint ───────────────────────────────────────────────────────

func TestNewCheckpointRejectsBadURI(t *testing.T) {
	if _, err := newCheckpoint(context.Background(), CheckpointConfig{URI: "http://nope"}); err == nil {
		t.Fatal("a non-s3 URI must be rejected")
	}
}

func TestNewCheckpointBuildsClient(t *testing.T) {
	cp, err := newCheckpoint(context.Background(), CheckpointConfig{
		URI:       "s3://bucket/prefix/",
		Endpoint:  "http://127.0.0.1:1",
		AccessKey: "a",
		SecretKey: "b",
	})
	if err != nil {
		// No AWS config in the sandbox: the constructor's error arm ran.
		return
	}
	if cp.bucket != "bucket" || cp.prefix != "prefix" {
		t.Fatalf("checkpoint = %+v", cp)
	}
	if cp.interval != 10*time.Second {
		t.Fatalf("default interval = %v, want 10s", cp.interval)
	}
}

// #212: an aborted snapshot returns before closeWindow, so releaseAllGates
// must drain any batches still held in an open gate.
func TestReleaseAllGatesDrainsHeldBatches(t *testing.T) {
	c, _ := coordHarness()
	ctx := context.Background()
	c.openWindow("raw.orders", 0)
	if !c.gateHold(ctx, &dataplane.Batch{Table: "raw.orders"}) {
		t.Fatal("a batch must be held while a window is open")
	}

	c.releaseAllGates()

	c.gateMu.Lock()
	open := len(c.gateOn)
	c.gateMu.Unlock()
	if open != 0 {
		t.Fatalf("releaseAllGates left %d open gate(s)", open)
	}
	if c.gateHold(ctx, &dataplane.Batch{Table: "raw.orders"}) {
		t.Fatal("a cleared gate must not hold")
	}
}

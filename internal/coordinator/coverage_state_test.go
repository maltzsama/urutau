package coordinator

// Batch 5b: the remaining testable coordinator state paths that Batch 5 left
// uncovered — graceful shutdown, dashboard Cancel/RestartWorker, the
// supervisor poll loop, sourceBatches, emit/emitLog, the partitioned enqueue
// path, resolvePartitionRanges, maintenance wiring, and the k8s helpers that
// do not need a live cluster.

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/dashboard"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// fakeControl satisfies the control-plane server by embedding the interface;
// only Send is ever reached.
type fakeControl struct {
	pb.UrutauControl_ControlServer
	sent    []*pb.ControlMessage
	sendErr error
}

func (f *fakeControl) Send(m *pb.ControlMessage) error {
	f.sent = append(f.sent, m)
	return f.sendErr
}

func TestGracefulShutdownSendsToAttachedControls(t *testing.T) {
	c, w := coordHarness()
	fc := &fakeControl{}
	w.control = fc
	c.gracefulShutdown()
	if len(fc.sent) != 1 {
		t.Fatalf("shutdown messages = %d, want 1", len(fc.sent))
	}
	// A worker with no control is skipped; a send failure is logged, not fatal.
	w.control = nil
	c.gracefulShutdown()
	w.control = &fakeControl{sendErr: errors.New("stream closed")}
	c.gracefulShutdown()
}

func TestCancelTerminatesWithOperatorError(t *testing.T) {
	c, _ := coordHarness()
	c.terminate = make(chan error, 1)
	if err := (dashState{c}).Cancel(); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if err := <-c.terminate; !errors.Is(err, errOperatorCancel) {
		t.Fatalf("terminate = %v, want errOperatorCancel", err)
	}
}

func TestRestartWorkerResetsKnownWorker(t *testing.T) {
	c, w := coordHarness()
	if err := (dashState{c}).RestartWorker("ghost"); err == nil {
		t.Fatal("an unknown worker must be rejected")
	}
	if err := (dashState{c}).RestartWorker("w0"); err != nil {
		t.Fatalf("RestartWorker: %v", err)
	}
	if w.epoch != 1 {
		t.Fatalf("epoch = %d, want 1 after restart", w.epoch)
	}
	if !c.supervisor.isPending("w0") {
		t.Fatal("a restarted worker must be pending")
	}
}

func TestSupervisorRunTerminatesCrashloop(t *testing.T) {
	c, _ := coordHarness()
	c.supervisor.noteAck("w0", time.Now().Add(-time.Hour))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	terminate := make(chan error, 1)
	go c.supervisor.run(ctx, SupervisorConfig{
		Poll: time.Millisecond, AckTimeout: time.Millisecond, MaxResets: 1, ResetWindow: time.Hour,
	}, terminate)

	select {
	case <-terminate:
	case <-time.After(3 * time.Second):
		t.Fatal("supervisor.run never terminated a crashlooping worker")
	}
}

func TestSupervisorRunStopsOnCancel(t *testing.T) {
	c, _ := coordHarness()
	c.supervisor.noteAck("w0", time.Now())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		c.supervisor.run(ctx, SupervisorConfig{Poll: time.Millisecond}, make(chan error, 1))
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("supervisor.run did not stop on cancel")
	}
}

// fakeReader yields the queued batches, then a terminal (nil, err).
type fakeReader struct {
	source.Reader
	batches []*dataplane.Batch
	err     error
	i       int
}

func (r *fakeReader) Next(context.Context) (*dataplane.Batch, error) {
	if r.i < len(r.batches) {
		b := r.batches[r.i]
		r.i++
		return b, nil
	}
	return nil, r.err
}

func TestSourceBatchesForwardsThenEnds(t *testing.T) {
	b1 := wireBatchIDs(t, 1)
	b2 := wireBatchIDs(t, 2)
	out, errCh := sourceBatches(context.Background(), &fakeReader{batches: []*dataplane.Batch{b1, b2}})

	got := 0
	for range out {
		got++
	}
	if got != 2 {
		t.Fatalf("forwarded = %d, want 2", got)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("errCh = %v, want nil", err)
	}
}

func TestSourceBatchesPropagatesError(t *testing.T) {
	boom := errors.New("read failed")
	out, errCh := sourceBatches(context.Background(), &fakeReader{err: boom})
	for range out {
	}
	if err := <-errCh; !errors.Is(err, boom) {
		t.Fatalf("errCh = %v, want boom", err)
	}
}

func TestEmitAndEmitLogRecordEvents(t *testing.T) {
	c, _ := coordHarness()
	c.dashEvents = dashboard.NewEvents(4)
	if err := c.emit("commit", map[string]any{"worker": "w0"}); err != nil {
		t.Fatalf("emit: %v", err)
	}
	c.emitLog("worker_reset", map[string]any{"worker": "w0"})
}

func wireBatchIDs(t *testing.T, ids ...int64) *dataplane.Batch {
	t.Helper()
	changes := make([]rowchange.Change, len(ids))
	for i, id := range ids {
		changes[i] = rowchange.Change{
			Op: rowchange.OpInsert, Table: "raw.orders", Key: []any{id},
			After: map[string]any{"id": id, "v": "x"}, Position: "0/10", IngestTS: time.Now(),
		}
	}
	cs := transport.InferSchemaFromChanges(changes)
	rec, err := transport.RecordFromChanges(changes, cs, nil)
	if err != nil {
		t.Fatalf("RecordFromChanges: %v", err)
	}
	return &dataplane.Batch{Table: "raw.orders", Record: rec, Watermark: []byte("0/10"), Mode: dataplane.UpsertMode}
}

func TestEnqueueBatchSplitsByPartition(t *testing.T) {
	c, w0 := coordHarness()
	w1 := &workerState{name: "w1", attached: true, queue: make(chan queuedBatch, 8)}
	c.workers["w1"] = w1
	c.route["raw.orders"] = []*workerState{w0, w1}
	c.index["w1"] = newPositionIndex("run-1")
	c.refs = []source.TableRef{{Source: "shop.orders", Target: "raw.orders", PrimaryKey: []string{"id"}}}
	c.partitionRanges = map[string][]source.Chunk{
		"raw.orders": {{Low: nil, High: []any{int64(100)}}, {Low: []any{int64(100)}, High: nil}},
	}

	b := wireBatchIDs(t, 1, 150)
	if err := c.enqueueBatch(context.Background(), b, &pb.BatchMeta{Table: "raw.orders"}); err != nil {
		t.Fatalf("enqueueBatch(partitioned): %v", err)
	}
	if len(w0.queue) != 1 || len(w1.queue) != 1 {
		t.Fatalf("queues = %d, %d; want 1, 1", len(w0.queue), len(w1.queue))
	}
}

// ── resolvePartitionRanges ───────────────────────────────────────────

type plainChunker struct{ source.ChunkSource }

type partitionChunker struct {
	source.ChunkSource
	ranges []source.Chunk
	err    error
}

func (p partitionChunker) Partitions(context.Context, int) ([]source.Chunk, error) {
	return p.ranges, p.err
}

type fakeQSource struct {
	source.QuerySource
	chunker source.ChunkSource
	err     error
}

func (f fakeQSource) NewChunker(string, string, int) (source.ChunkSource, error) {
	return f.chunker, f.err
}

func (f fakeQSource) CloseQuery() error { return nil }

func TestResolvePartitionRanges(t *testing.T) {
	ctx := context.Background()
	ref := source.TableRef{Source: "s", PrimaryKey: []string{"id"}}
	two := spec.Table{Workers: &spec.WorkerSpec{Number: 2}}

	ranges := []source.Chunk{{Low: nil, High: []any{int64(5)}}, {Low: []any{int64(5)}, High: nil}}
	c := &Coordinator{qsrc: fakeQSource{chunker: partitionChunker{ranges: ranges}}}
	got, _, err := c.resolvePartitionRanges(ctx, two, ref)
	if err != nil {
		t.Fatalf("resolvePartitionRanges: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("ranges = %d, want 2", len(got))
	}

	// A chunker that is not a PartitionSource fails loudly.
	c.qsrc = fakeQSource{chunker: plainChunker{}}
	if _, _, err := c.resolvePartitionRanges(ctx, two, ref); err == nil {
		t.Fatal("a non-partitioning chunker must fail")
	}

	// NewChunker error propagates.
	boom := errors.New("no query conn")
	c.qsrc = fakeQSource{err: boom}
	if _, _, err := c.resolvePartitionRanges(ctx, two, ref); !errors.Is(err, boom) {
		t.Fatalf("resolvePartitionRanges(newchunker err) = %v, want boom", err)
	}

	// Partitions error propagates.
	c.qsrc = fakeQSource{chunker: partitionChunker{err: boom}}
	if _, _, err := c.resolvePartitionRanges(ctx, two, ref); !errors.Is(err, boom) {
		t.Fatalf("resolvePartitionRanges(partitions err) = %v, want boom", err)
	}
}

// ── maintenance wiring ───────────────────────────────────────────────

type fakeMaintainableSink struct{ sink.Sink }

func (fakeMaintainableSink) Maintain(core.TableRef, spec.Maintenance, *slog.Logger, func() string, sink.MaintainerMetrics) sink.Maintainer {
	return nil
}

func TestStartMaintenanceGating(t *testing.T) {
	disabled := &Coordinator{
		cfg: Config{Spec: &spec.Spec{Sink: spec.Sink{}}},
		log: slog.New(slog.DiscardHandler),
	}
	if err := disabled.startMaintenance(nil, nil); err != nil {
		t.Fatalf("disabled maintenance must be a no-op: %v", err)
	}

	enabled := &Coordinator{
		cfg: Config{Spec: &spec.Spec{Sink: spec.Sink{
			Type:        "clickhouse",
			Maintenance: &spec.Maintenance{Enabled: true},
		}}},
		log: slog.New(slog.DiscardHandler),
	}
	enabled.snk = fakeStagedSink{}
	if err := enabled.startMaintenance(nil, nil); err == nil {
		t.Fatal("a non-Maintainable sink must be rejected")
	}

	enabled.snk = fakeMaintainableSink{}
	if err := enabled.startMaintenance([]core.TableRef{{Target: "t"}}, nil); err != nil {
		t.Fatalf("startMaintenance (no k8s template): %v", err)
	}
	if enabled.maint == nil {
		t.Fatal("maintenance scheduler must be registered")
	}
}

func TestMaintenanceSessionRejectsWhenDisabled(t *testing.T) {
	c, _ := coordHarness()
	if err := c.maintenanceSession(nil, &pb.Hello{}); err == nil {
		t.Fatal("a maintenance session with maintenance disabled must be rejected")
	}
}

// ── k8s helpers (no cluster) ─────────────────────────────────────────

func TestWorkerPodTemplateHelpers(t *testing.T) {
	if got := workerPodTemplateFile("raw.orders"); got != "worker-pod-template.raw.orders.yaml" {
		t.Fatalf("workerPodTemplateFile = %q", got)
	}
	if got := anyTarget(nil); got != "" {
		t.Fatalf("anyTarget(nil) = %q", got)
	}
	if got := anyTarget(map[string]string{"a": "x"}); got != "x" {
		t.Fatalf("anyTarget = %q", got)
	}
	if workerPodTemplateAvailable(nil) {
		t.Fatal("no targets means no template")
	}
	if workerPodTemplateAvailable(map[string]string{"a": "x"}) {
		t.Fatal("a template file that does not exist must report unavailable")
	}
	if _, err := loadWorkerPodTemplate("missing"); err == nil {
		t.Fatal("a missing template file must error")
	}
}

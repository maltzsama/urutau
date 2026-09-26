package coordinator

import (
	"context"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/driver"
	"github.com/maltzsama/urutau/internal/snapshot"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// testSnapshotKind is a source kind registered only for these tests, so
// resumeFrom reads a snapshotting source's capabilities.
const testSnapshotKind = "test-snapshotting"

var registerTestSnapshotKind = sync.OnceValue(func() error {
	return driver.RegisterSource(testSnapshotKind, source.Capabilities{Snapshot: true}, nil)
})

// propsSink holds per-table positions and properties.
type propsSink struct {
	fakeStagedSink
	mu        sync.Mutex
	positions map[string]string
	props     map[string]map[string]string
}

func newPropsSink() *propsSink {
	return &propsSink{positions: map[string]string{}, props: map[string]map[string]string{}}
}

func (s *propsSink) Position(_ context.Context, ref core.TableRef) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.positions[ref.Target], nil
}

func (s *propsSink) Properties(_ context.Context, ref core.TableRef) (map[string]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]string{}
	for k, v := range s.props[ref.Target] {
		out[k] = v
	}
	return out, nil
}

func (s *propsSink) SetProperties(_ context.Context, ref core.TableRef, props map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.props[ref.Target] == nil {
		s.props[ref.Target] = map[string]string{}
	}
	for k, v := range props {
		s.props[ref.Target][k] = v
	}
	return nil
}

func (s *propsSink) state(target string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.props[target][snapshot.PropSnapshotState]
}

// A table the stream committed to while its snapshot had not finished holds
// a position, and a crash there must not take that position for a finished
// snapshot (#428): the chaos run lost every pre-existing row of such a table.
// Only a table marked complete, or one that predates the marking, is resumed
// without a snapshot.
func TestResumeSnapshotsATableLeftInProgress(t *testing.T) {
	if err := registerTestSnapshotKind(); err != nil {
		t.Fatal(err)
	}
	snk := newPropsSink()
	snk.positions["raw.left"] = "0/20"
	snk.props["raw.left"] = map[string]string{snapshot.PropSnapshotState: string(snapshot.StateInProgress)}
	snk.positions["raw.done"] = "0/30"
	snk.props["raw.done"] = map[string]string{snapshot.PropSnapshotState: string(snapshot.StateComplete)}
	snk.positions["raw.legacy"] = "0/40"
	snk.positions["raw.queued"] = "0/50"
	snk.props["raw.queued"] = map[string]string{snapshot.PropSnapshotState: string(snapshot.StateNotStarted)}
	c, _ := coordHarness()
	c.cfg = Config{Spec: &spec.Spec{Source: spec.Source{Kind: testSnapshotKind}}}
	c.snk = snk
	refs := []source.TableRef{
		{Source: "shop.left", Target: "raw.left"},
		{Source: "shop.done", Target: "raw.done"},
		{Source: "shop.legacy", Target: "raw.legacy"},
		{Source: "shop.queued", Target: "raw.queued"},
		{Source: "shop.fresh", Target: "raw.fresh"},
	}
	resume, needs, err := c.resumeFrom(context.Background(), refs)
	if err != nil {
		t.Fatalf("resumeFrom: %v", err)
	}
	var got []string
	for _, r := range needs {
		got = append(got, r.Target)
	}
	if len(got) != 3 || got[0] != "raw.left" || got[1] != "raw.queued" || got[2] != "raw.fresh" {
		t.Fatalf("tables to snapshot = %v, want [raw.left raw.queued raw.fresh]", got)
	}
	// The interrupted table's position still bounds the resume point.
	if resume == nil || resume.String() != "0/20" {
		t.Fatalf("resume = %v, want 0/20", resume)
	}
}

// Before the stream starts, every table about to be snapshotted is marked
// unfinished; one already unfinished keeps its state (a collapsed run's
// in_progress carries resumable bounds that not_started would drop).
func TestMarkSnapshotsPending(t *testing.T) {
	snk := newPropsSink()
	snk.props["raw.b"] = map[string]string{snapshot.PropSnapshotState: string(snapshot.StateInProgress)}
	snk.props["raw.c"] = map[string]string{snapshot.PropSnapshotState: string(snapshot.StateComplete)}
	c, _ := coordHarness()
	c.snk = snk
	refs := []source.TableRef{{Source: "shop.a", Target: "raw.a"}, {Source: "shop.b", Target: "raw.b"}, {Source: "shop.c", Target: "raw.c"}}
	if err := c.markSnapshotsPending(context.Background(), refs); err != nil {
		t.Fatal(err)
	}
	for target, want := range map[string]snapshot.SnapshotState{
		"raw.a": snapshot.StateNotStarted, "raw.b": snapshot.StateInProgress, "raw.c": snapshot.StateNotStarted,
	} {
		if got := snk.state(target); got != string(want) {
			t.Errorf("%s: state %q, want %s", target, got, want)
		}
	}
}

// A table that was never sent anything had no rows to copy: nothing is in
// flight for it, and its completion is written directly.
func TestFinishSnapshotOfAnEmptyTableWritesCompleteDirectly(t *testing.T) {
	snk := newPropsSink()
	c, w0 := coordHarness()
	c.snk = snk
	c.refs = []source.TableRef{{Source: "shop.orders", Target: "raw.orders", PrimaryKey: []string{"id"}}}
	if err := c.finishSnapshot(context.Background(), c.refs[0]); err != nil {
		t.Fatal(err)
	}
	if got := snk.state("raw.orders"); got != string(snapshot.StateComplete) {
		t.Fatalf("state %q, want complete", got)
	}
	if n := len(w0.queue); n != 0 {
		t.Fatalf("%d batch(es) queued, want none", n)
	}
}

// The completion is committed by the table's writer, behind its last window:
// written directly it could land before that window's rows, and a crash in
// between would take the table as snapshotted without them.
func TestFinishSnapshotQueuesTheDoneMarkerBehindTheLastWindow(t *testing.T) {
	snk := newPropsSink()
	c, w0 := coordHarness()
	c.snk = snk
	c.refs = []source.TableRef{{Source: "shop.orders", Target: "raw.orders", PrimaryKey: []string{"id"}}}
	if err := c.sendCloses(context.Background(), w0, "raw.orders", position.MustLSN("0/10"), 3); err != nil {
		t.Fatal(err)
	}
	if err := c.finishSnapshot(context.Background(), c.refs[0]); err != nil {
		t.Fatal(err)
	}
	if got := snk.state("raw.orders"); got != "" {
		t.Fatalf("state written directly (%q); the writer must commit it", got)
	}
	metas := drainMetas(t, w0)
	if len(metas) != 2 || !metas[0].Window.GetCloses() || !metas[1].Window.GetSnapshotDone() {
		t.Fatalf("queued %v, want the Closes marker then the done marker", metas)
	}
	if metas[1].LowPos != "0/10" {
		t.Fatalf("done marker position %q, want the latest sent, 0/10", metas[1].LowPos)
	}
	// The marker is in flight until the worker acks it, like any batch.
	if n := c.inFlight("w0"); n != 2 {
		t.Fatalf("in flight %d, want 2", n)
	}
}

// On a staged table the done marker is a cycle of the table's send order, so
// it commits after every window of every partition.
func TestFinishSnapshotOnAStagedTableIsACycle(t *testing.T) {
	snk := newPropsSink()
	c, w0 := coordHarness()
	c.snk = snk
	w1 := &workerState{name: "w1", attached: true, queue: make(chan queuedBatch, 8)}
	c.workers["w1"] = w1
	c.index["w1"] = newPositionIndex("run-1")
	c.setRouteForTest("raw.orders", []*workerState{w0, w1})
	c.refs = []source.TableRef{{Source: "shop.orders", Target: "raw.orders", PrimaryKey: []string{"id"}}}
	if err := c.sendCloses(context.Background(), w1, "raw.orders", position.MustLSN("0/10"), 0); err != nil {
		t.Fatal(err)
	}
	if err := c.finishSnapshot(context.Background(), c.refs[0]); err != nil {
		t.Fatal(err)
	}
	metas := drainMetas(t, w1)
	if len(metas) != 2 {
		t.Fatalf("queued %d on the last window's worker, want 2", len(metas))
	}
	done := metas[1]
	if !done.Window.GetSnapshotDone() || !done.Staged || done.BatchId <= metas[0].BatchId {
		t.Fatalf("done marker = %+v, want a staged cycle after the window's", done)
	}
	if n := c.staged.len(); n != 2 {
		t.Fatalf("%d staged cycles, want the window's and the done marker's", n)
	}
}

func drainMetas(t *testing.T, w *workerState) []*pb.BatchMeta {
	t.Helper()
	var out []*pb.BatchMeta
	for {
		select {
		case q := <-w.queue:
			m := &pb.BatchMeta{}
			if err := proto.Unmarshal(q.meta, m); err != nil {
				t.Fatal(err)
			}
			out = append(out, m)
		default:
			return out
		}
	}
}

// The done marker expects only the last window's worker, yet cannot commit
// ahead of another partition's earlier window: a staged table's cycles commit
// in send order whatever worker owns them. Delivered first, the done cycle
// waits; the earlier window's delivery then commits both, window first.
func TestSnapshotDoneWaitsForEveryEarlierWindow(t *testing.T) {
	snk := newPropsSink()
	c, w0 := coordHarness()
	c.snk = snk
	w1 := &workerState{name: "w1", attached: true, queue: make(chan queuedBatch, 8)}
	c.workers["w1"] = w1
	c.index["w1"] = newPositionIndex("run-1")
	c.setRouteForTest("raw.orders", []*workerState{w0, w1})
	c.refs = []source.TableRef{{Source: "shop.orders", Target: "raw.orders", PrimaryKey: []string{"id"}}}
	ctx := context.Background()
	if err := c.sendCloses(ctx, w0, "raw.orders", position.MustLSN("0/10"), 0); err != nil {
		t.Fatal(err)
	}
	if err := c.sendCloses(ctx, w1, "raw.orders", position.MustLSN("0/20"), 0); err != nil {
		t.Fatal(err)
	}
	if err := c.finishSnapshot(ctx, c.refs[0]); err != nil {
		t.Fatal(err)
	}
	w0win := drainMetas(t, w0)[0]
	w1metas := drainMetas(t, w1)
	w1win, done := w1metas[0], w1metas[1]

	// w1's window and the done marker are delivered; w0's window is not.
	if got, _ := c.staged.deliver(stagedRef("raw.orders", "w1"), w1win.BatchId, []byte{1}, "0/20", "", nil); len(got) != 0 {
		t.Fatalf("w1's window committed ahead of w0's earlier one: %d cycle(s)", len(got))
	}
	if got, _ := c.staged.deliver(stagedRef("raw.orders", "w1"), done.BatchId, []byte{2}, "", "complete", nil); len(got) != 0 {
		t.Fatalf("the done cycle committed ahead of w0's window: %d cycle(s)", len(got))
	}
	got, _ := c.staged.deliver(stagedRef("raw.orders", "w0"), w0win.BatchId, []byte{3}, "0/10", "", nil)
	if len(got) != 3 || got[0].seq != w0win.BatchId || got[2].seq != done.BatchId {
		t.Fatalf("committable after w0's window: %d cycle(s), want w0's window, w1's window, then the done cycle", len(got))
	}
}

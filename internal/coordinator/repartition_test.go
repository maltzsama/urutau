package coordinator

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"

	"github.com/maltzsama/urutau/core"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// partitionChunker hands out n contiguous ranges over an int64 key domain,
// the shape a real PartitionSource returns.
type scalingChunker struct {
	fakeChunkSource
	calls int
}

func (p *scalingChunker) Partitions(_ context.Context, n int) ([]source.Chunk, error) {
	p.calls++
	out := make([]source.Chunk, n)
	const span = int64(1000)
	step := span / int64(n)
	for i := 0; i < n; i++ {
		var low, high []any
		if i > 0 {
			low = []any{int64(i) * step}
		}
		if i < n-1 {
			high = []any{int64(i+1) * step}
		}
		out[i] = source.Chunk{Low: low, High: high}
	}
	return out, nil
}

// scaleHarness is a coordinator with one partitioned table routed to a
// single owner, ready to be scaled.
func scaleHarness(t *testing.T) (*Coordinator, *scalingChunker) {
	t.Helper()
	c, w := coordHarness()
	c.cfg.Spec.Tables = []spec.Table{{
		Source: "shop.orders", Target: "raw.orders",
		Workers: &WorkerSpecAlias{Number: 1},
	}}
	ch := &scalingChunker{}
	c.chunkers = map[string]source.ChunkSource{"raw.orders": ch}
	c.snk = fakeStagedSink{} // a staging sink that supports concurrent writers
	c.setRangesForTest(map[string][]source.Chunk{"raw.orders": {{}}})
	c.workers[w.name] = w
	c.byTicket = map[string]*workerState{}
	return c, ch
}

// WorkerSpecAlias keeps the harness readable when spec.WorkerSpec grows.
type WorkerSpecAlias = spec.WorkerSpec

func TestScaleOutRegistersOwnerAndSwapsRouting(t *testing.T) {
	c, ch := scaleHarness(t)
	before, _ := c.loadRouting().ownersOf("raw.orders")
	if len(before) != 1 {
		t.Fatalf("precondition: want 1 owner, got %d", len(before))
	}

	if err := c.ScaleTable(context.Background(), "raw.orders", 3); err != nil {
		t.Fatalf("ScaleTable: %v", err)
	}

	after, _ := c.loadRouting().ownersOf("raw.orders")
	if len(after) != 3 {
		t.Fatalf("owners after scale = %d, want 3", len(after))
	}
	ranges := c.loadRouting().rangesOf("raw.orders")
	if len(ranges) != 3 {
		t.Fatalf("ranges after scale = %d, want 3", len(ranges))
	}
	if ch.calls != 1 {
		t.Fatalf("Partitions called %d times, want 1", ch.calls)
	}
	// Minimal remap: partition 0 keeps its original owner.
	if after[0] != before[0] {
		t.Fatalf("owner 0 changed: %p -> %p (re-slice must keep existing owners)", before[0], after[0])
	}
	// The new owners are registered, so Session accepts their Hello.
	for _, w := range after[1:] {
		if _, ok := c.workers[w.name]; !ok {
			t.Fatalf("owner %q not in the registry: its Hello would be rejected", w.name)
		}
		if _, ok := c.byTicket[string(w.ticket)]; !ok {
			t.Fatalf("owner %q has no ticket: DoGet could not find its queue", w.name)
		}
		if c.index[w.name] == nil {
			t.Fatalf("owner %q has no position index", w.name)
		}
	}
}

// The scale-out blocker from #312: a pod KEDA starts must be able to join.
func TestScaledOutWorkerIsAcceptedBySession(t *testing.T) {
	c, _ := scaleHarness(t)
	srv := &controlServer{c: c}

	newName := partitionName(c.cfg.Spec.Pipeline, "raw.orders", 1)
	stream := &fakeSessionStream{in: []*pb.WorkerMessage{
		{Msg: &pb.WorkerMessage_Hello{Hello: &pb.Hello{WorkerName: newName}}},
	}}
	// Before the scale the name is unknown and the Hello is rejected.
	if err := srv.Session(stream); err == nil || !strings.Contains(err.Error(), "unknown worker") {
		t.Fatalf("before scale: want an 'unknown worker' rejection, got %v", err)
	}

	if err := c.ScaleTable(context.Background(), "raw.orders", 2); err != nil {
		t.Fatalf("ScaleTable: %v", err)
	}
	if _, ok := c.workers[newName]; !ok {
		t.Fatalf("after scale %q is still unregistered", newName)
	}
}

func TestScaleInDropsOwnerAndShrinksRanges(t *testing.T) {
	c, _ := scaleHarness(t)
	ctx := context.Background()
	if err := c.ScaleTable(ctx, "raw.orders", 3); err != nil {
		t.Fatalf("scale out: %v", err)
	}
	owners, _ := c.loadRouting().ownersOf("raw.orders")
	dropped := owners[2]

	if err := c.ScaleTable(ctx, "raw.orders", 2); err != nil {
		t.Fatalf("scale in: %v", err)
	}
	after, _ := c.loadRouting().ownersOf("raw.orders")
	if len(after) != 2 {
		t.Fatalf("owners = %d, want 2", len(after))
	}
	if len(c.loadRouting().rangesOf("raw.orders")) != 2 {
		t.Fatal("ranges must shrink with the owners")
	}
	if _, ok := c.workers[dropped.name]; ok {
		t.Fatalf("retired owner %q still in the registry", dropped.name)
	}
	if _, ok := c.byTicket[string(dropped.ticket)]; ok {
		t.Fatalf("retired owner %q still holds a ticket", dropped.name)
	}
	// Its position index stays: the ack path dereferences c.index without
	// c.mu, so deleting it under an in-flight batch would panic.
	if c.index[dropped.name] == nil {
		t.Fatalf("retired owner %q lost its position index: the ack path would nil-panic", dropped.name)
	}
	c.confirmedMu.Lock()
	_, stillConfirmed := c.confirmed[dropped.name]
	c.confirmedMu.Unlock()
	if stillConfirmed {
		t.Fatalf("retired owner %q still counts toward confirmedPosition: it would pin retention", dropped.name)
	}
}

// A new owner with no confirmed position pins confirmedPosition to nil,
// which stalls source retention for the WHOLE pipeline.
func TestScaleOutSeedsConfirmedPosition(t *testing.T) {
	c, _ := scaleHarness(t)
	owners, _ := c.loadRouting().ownersOf("raw.orders")
	base := position.MustLSN("0/100")
	c.recordConfirmed(owners[0].name, base)
	if got := c.confirmedPosition(); got == nil {
		t.Fatal("precondition: one owner with a position must yield a confirmed position")
	}

	if err := c.ScaleTable(context.Background(), "raw.orders", 3); err != nil {
		t.Fatalf("ScaleTable: %v", err)
	}
	if got := c.confirmedPosition(); got == nil {
		t.Fatal("confirmedPosition went nil after scale-out: retention would stall pipeline-wide")
	}
	after, _ := c.loadRouting().ownersOf("raw.orders")
	c.confirmedMu.Lock()
	defer c.confirmedMu.Unlock()
	for _, w := range after[1:] {
		if c.confirmed[w.name] == nil {
			t.Fatalf("new owner %q was not seeded", w.name)
		}
	}
}

func TestScaleRejectsInvalidRequests(t *testing.T) {
	c, _ := scaleHarness(t)
	ctx := context.Background()

	if err := c.ScaleTable(ctx, "raw.orders", 0); err == nil {
		t.Fatal("n<1 must be rejected")
	}
	if err := c.ScaleTable(ctx, "nope", 2); err == nil {
		t.Fatal("an unrouted table must be rejected")
	}
	c.cfg.MaxWorkers = 2
	if err := c.ScaleTable(ctx, "raw.orders", 5); err == nil {
		t.Fatal("exceeding maxWorkers must be rejected")
	}
	// A per-table cap overrides the global one.
	c.cfg.Spec.Tables[0].Workers.Max = 8
	if err := c.ScaleTable(ctx, "raw.orders", 5); err != nil {
		t.Fatalf("per-table Max must override the global cap: %v", err)
	}
}

// A table booted with one worker has no chunker: scaling it out must build
// one on demand, which is the main case this feature exists for. The e2e hit
// this as "no chunker: table was booted unpartitioned".
func TestScaleOutBuildsChunkerForUnpartitionedTable(t *testing.T) {
	c, _ := scaleHarness(t)
	c.chunkers = map[string]source.ChunkSource{} // booted unpartitioned
	c.qsrc = fakeQSource{chunker: &scalingChunker{}}

	if err := c.ScaleTable(context.Background(), "raw.orders", 2); err != nil {
		t.Fatalf("ScaleTable on a table booted unpartitioned: %v", err)
	}
	if owners, _ := c.loadRouting().ownersOf("raw.orders"); len(owners) != 2 {
		t.Fatalf("owners = %d, want 2", len(owners))
	}
	if c.lookupChunker("raw.orders") == nil {
		t.Fatal("the chunker built on demand must be memoized for the next re-slice")
	}
}

func TestScaleToSameCountIsNoop(t *testing.T) {
	c, ch := scaleHarness(t)
	if err := c.ScaleTable(context.Background(), "raw.orders", 1); err != nil {
		t.Fatalf("ScaleTable: %v", err)
	}
	if ch.calls != 0 {
		t.Fatalf("a no-op scale must not re-resolve ranges, got %d calls", ch.calls)
	}
}

// The flip must wait on in-flight batches, not only staged cycles. The
// positionIndex frees a worker's queue strictly from the head, and after a
// flip the owner's new key range need not produce acks that cover the batch
// still at that head — the queue wedges, the supervisor calls the owner
// stale, and the reset discards its staged cycles. The e2e hit exactly this.
func TestScaleWaitsForInFlightBatches(t *testing.T) {
	c, _ := scaleHarness(t)
	c.cfg.ScaleDrainTimeout = 500 * time.Millisecond
	owners, _ := c.loadRouting().ownersOf("raw.orders")

	// One batch delivered and unacked on the current owner.
	c.index[owners[0].name].add(inflightBatch{id: 1, table: "raw.orders", bytes: 10})
	if c.inFlight(owners[0].name) == 0 {
		t.Fatal("precondition: the owner must have an in-flight batch")
	}

	err := c.ScaleTable(context.Background(), "raw.orders", 3)
	if err == nil {
		t.Fatal("the flip must wait for in-flight batches")
	}
	if !strings.Contains(err.Error(), "drain") {
		t.Fatalf("want a drain error, got %v", err)
	}
	if got, _ := c.loadRouting().ownersOf("raw.orders"); len(got) != 1 {
		t.Fatalf("owners = %d, want the pre-scale 1 (no flip)", len(got))
	}
}

// The barrier: a table with an open staged cycle must not flip until the
// cycle commits, or the cycle would span two layouts.
func TestScaleWaitsForOpenStagedCycle(t *testing.T) {
	c, _ := scaleHarness(t)
	c.snk = fakeStagedSink{}
	c.cfg.ScaleDrainTimeout = 2 * time.Second

	owners, _ := c.loadRouting().ownersOf("raw.orders")
	second := &workerState{name: "w1", queue: make(chan queuedBatch, 8)}
	c.setRouteForTest("raw.orders", []*workerState{owners[0], second})
	c.setRangesForTest(map[string][]source.Chunk{"raw.orders": {{}, {}}})
	if !c.isStagedTable("raw.orders") {
		t.Fatal("precondition: table must be staged")
	}

	// Open a cycle that never completes.
	c.staged.expect(core.TableRef{Target: "raw.orders"}, 7, []string{owners[0].name, second.name})
	if got := c.staged.openFor(core.TableRef{Target: "raw.orders"}); got != 1 {
		t.Fatalf("openFor = %d, want 1", got)
	}

	err := c.ScaleTable(context.Background(), "raw.orders", 3)
	if err == nil {
		t.Fatal("a scale must not flip while a cycle is open")
	}
	if !strings.Contains(err.Error(), "drain") {
		t.Fatalf("want a drain error, got %v", err)
	}
	// The layout is unchanged: the flip did not happen.
	if got, _ := c.loadRouting().ownersOf("raw.orders"); len(got) != 2 {
		t.Fatalf("owners = %d, want the pre-scale 2 (no flip)", len(got))
	}
}

// Routing must stay internally consistent under a concurrent re-slice:
// a reader that loads a snapshot always sees len(owners) == len(ranges).
// Run with -race.
func TestRoutingSnapshotIsConsistentUnderConcurrentScale(t *testing.T) {
	c, _ := scaleHarness(t)
	ctx := context.Background()

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			n := 2 + i%4
			if err := c.ScaleTable(ctx, "raw.orders", n); err != nil {
				t.Errorf("ScaleTable(%d): %v", n, err)
				return
			}
		}
	}()

	// Readers mimic the routing path: load once, then use both halves.
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				rt := c.loadRouting()
				owners, ok := rt.ownersOf("raw.orders")
				if !ok {
					continue
				}
				ranges := rt.rangesOf("raw.orders")
				if len(owners) != len(ranges) {
					t.Errorf("torn snapshot: %d owners but %d ranges", len(owners), len(ranges))
					return
				}
				_ = c.isStagedTable("raw.orders")
			}
		}()
	}

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// A table booted unpartitioned (workers: 1) may only be scaled to N>1 if its
// sink can order concurrent writers — the same capability boot-time
// validation requires.
func TestScaleRejectsNonConcurrentSink(t *testing.T) {
	c, _ := scaleHarness(t)
	c.snk = fakeNonConcurrentSink{}

	err := c.ScaleTable(context.Background(), "raw.orders", 2)
	if err == nil || !strings.Contains(err.Error(), "concurrent") {
		t.Fatalf("want a concurrent-writer error, got %v", err)
	}
	if got, _ := c.loadRouting().ownersOf("raw.orders"); len(got) != 1 {
		t.Fatalf("owners = %d, want the pre-scale 1 (no flip)", len(got))
	}
}

// A scale whose prepare phase fails (the drain never converges) must roll
// back the owners it CREATED, or they linger as ghosts under the old layout:
// accepting Hellos, supervised, and holding a retention position.
func TestScaleRollsBackCreatedOwnersOnFailedPrepare(t *testing.T) {
	c, _ := scaleHarness(t)
	c.cfg.ScaleDrainTimeout = 200 * time.Millisecond
	owners, _ := c.loadRouting().ownersOf("raw.orders")
	// One in-flight batch the drain can never clear.
	c.index[owners[0].name].add(inflightBatch{id: 1, table: "raw.orders", bytes: 10})

	if err := c.ScaleTable(context.Background(), "raw.orders", 2); err == nil {
		t.Fatal("the scale must fail when the drain never converges")
	}

	name := partitionName(c.cfg.Spec.Pipeline, "raw.orders", 1)
	c.mu.Lock()
	_, inWorkers := c.workers[name]
	_, inIndex := c.index[name]
	c.mu.Unlock()
	c.confirmedMu.Lock()
	_, inConfirmed := c.confirmed[name]
	c.confirmedMu.Unlock()
	if inWorkers || inIndex || inConfirmed {
		t.Fatalf("created owner %q lingered after a failed scale: workers=%v index=%v confirmed=%v",
			name, inWorkers, inIndex, inConfirmed)
	}
	if got, _ := c.loadRouting().ownersOf("raw.orders"); len(got) != 1 {
		t.Fatalf("owners = %d, want the pre-scale 1 (no flip)", len(got))
	}
}

// After the flip commits, a scale-in must detach the removed owner even if
// the caller context is cancelled mid-drain — otherwise the scale reports
// success while the owner keeps its session and resources under the new
// layout.
func TestRetireOwnerDetachesWhenContextCancelled(t *testing.T) {
	c, _ := scaleHarness(t)
	w, _ := c.loadRouting().ownersOf("raw.orders")
	owner := w[0]
	// A batch that will never drain, so the loop would spin.
	c.index[owner.name].add(inflightBatch{id: 1, table: "raw.orders", bytes: 10})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c.retireOwner(ctx, owner)

	c.mu.Lock()
	_, still := c.workers[owner.name]
	c.mu.Unlock()
	if still {
		t.Fatal("a retired owner must be detached even when the context is cancelled")
	}
}

// A re-slice registers owners (writing byTicket) while a worker opens or
// reopens its Flight DoGet (reading byTicket): the lookup must hold c.mu, or
// the concurrent map access races and, without -race, can panic.
func TestDoGetTicketLookupDoesNotRaceRegisterOwner(t *testing.T) {
	c, _ := scaleHarness(t)
	ref, ok := c.tableRef("raw.orders")
	if !ok {
		ref = source.TableRef{Source: "shop.orders", Target: "raw.orders"}
	}
	srv := &flightServer{c: c}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		stream := &fakeDoGetStream{ctx: context.Background()}
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = srv.DoGet(&flight.Ticket{Ticket: []byte("no-such-ticket")}, stream)
		}
	}()
	for i := 0; i < 200; i++ {
		if _, _, err := c.registerOwner(fmt.Sprintf("owner-%d", i), ref); err != nil {
			t.Fatalf("registerOwner: %v", err)
		}
	}
	close(stop)
	<-done
}

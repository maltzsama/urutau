package e2e

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/coordinator"
	"github.com/maltzsama/urutau/internal/grpctls"
	"github.com/maltzsama/urutau/internal/worker"
	"github.com/maltzsama/urutau/spec"
)

// scalablePipeline boots a coordinator that hands its handle back, plus the
// workers for the groups that exist at boot. Extra workers are started later
// by startWorker, the way a KEDA-scaled pod joins a running pipeline.
type scalablePipeline struct {
	t     *testing.T
	ctx   context.Context
	addr  string
	coord *coordinator.Coordinator

	mu    sync.Mutex
	stops []context.CancelFunc
	done  chan error
}

func bootScalable(t *testing.T, ctx context.Context, addr string, s *spec.Spec, workers ...string) *scalablePipeline {
	t.Helper()
	p := &scalablePipeline{t: t, ctx: ctx, addr: addr, done: make(chan error, 64)}

	ready := make(chan *coordinator.Coordinator, 1)
	cCtx, cStop := context.WithCancel(ctx)
	p.track(cStop)
	go func() {
		p.done <- coordinator.Run(cCtx, coordinator.Config{
			TLS:           grpctls.Config{AllowInsecure: true},
			Spec:          s,
			ListenAddr:    addr,
			ServerID:      1103, // distinct from the other distributed runs
			Heartbeat:     5 * time.Second,
			ChunkSize:     10,
			WindowTimeout: 2 * time.Minute,
			CaughtUpPoll:  300 * time.Millisecond,
			WaitWorker:    2 * time.Minute,
			OnReady:       func(c *coordinator.Coordinator) { ready <- c },
		})
	}()

	select {
	case c := <-ready:
		p.coord = c
	case <-time.After(2 * time.Minute):
		t.Fatal("coordinator never became ready")
	}

	for _, name := range workers {
		p.startWorker(name)
	}
	return p
}

// startWorker brings up one worker group, as a new pod would.
func (p *scalablePipeline) startWorker(name string) {
	p.t.Helper()
	wCtx, wStop := context.WithCancel(p.ctx)
	p.track(wStop)
	go func() {
		p.done <- worker.RunRemote(wCtx, worker.RemoteConfig{
			Coordinator: p.addr,
			Name:        name,
			Namespace:   "raw",
			Sink:        workerSink(),
			MaxRows:     100,
			MaxInterval: 2 * time.Second,
		})
	}()
}

func (p *scalablePipeline) track(stop context.CancelFunc) {
	p.mu.Lock()
	p.stops = append(p.stops, stop)
	p.mu.Unlock()
	p.t.Cleanup(stop)
}

func (p *scalablePipeline) stop() {
	p.mu.Lock()
	stops := append([]context.CancelFunc(nil), p.stops...)
	p.mu.Unlock()
	for _, s := range stops {
		s()
	}
	deadline := time.After(90 * time.Second)
	for {
		select {
		case err := <-p.done:
			if err != nil && !errors.Is(err, context.Canceled) {
				p.t.Errorf("process exited with: %v", err)
			}
		case <-deadline:
			return
		case <-time.After(2 * time.Second):
			return
		}
	}
}

// TestLiveRepartitionScaleOut proves issue #312 end to end: a table running
// with one worker is scaled to three WHILE the source keeps writing, a new
// worker pod joins the running pipeline, and the final Iceberg state matches
// the source exactly — no key lost across the moved boundary, none written
// twice.
//
// Before this feature the new pod's Hello was rejected outright ("unknown
// worker"), so no key range could ever reach it.
func TestLiveRepartitionScaleOut(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	addr := reserveAddr(t)
	s := loadPipeline(t)
	s.Tables[0].Workers = &spec.WorkerSpec{Number: 1, Max: 8}
	if err := s.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	target := s.Tables[0].Target
	boot := s.Tables[0].WorkerGroupNames(s.Pipeline)

	db := mysqlConn(t)
	resetBinlog(t, db)
	dropIcebergTable(t, ctx)
	dropAll(t, db)
	seedOrders(t, db, 0, 200)

	p := bootScalable(t, ctx, addr, s, boot...)
	defer p.stop()
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(200))
	t.Log("snapshot converged with 1 partition")

	// Keep the source writing across the whole re-slice: the flip must not
	// lose or duplicate anything that lands mid-scale.
	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		for i := 200; i < 260; i++ {
			dml(t, db, fmt.Sprintf("INSERT INTO orders (id, v, amount) VALUES (%d, 'live%d', %d.0)", i, i, i))
			time.Sleep(40 * time.Millisecond)
		}
	}()

	// Scale to 3 while that writer runs.
	time.Sleep(500 * time.Millisecond)
	if err := p.coord.ScaleTable(ctx, target, 3); err != nil {
		t.Fatalf("ScaleTable(%s, 3): %v", target, err)
	}
	t.Log("re-sliced to 3 partitions under live load")

	// The two new groups have no pod yet: start them, as KEDA would. The
	// names come from the same derivation the coordinator uses, so a
	// change to that scheme fails here rather than silently starting a
	// worker nothing routes to.
	scaledSpec := s.Tables[0]
	scaledSpec.Workers = &spec.WorkerSpec{Number: 3}
	for _, name := range scaledSpec.WorkerGroupNames(s.Pipeline)[1:] {
		p.startWorker(name)
	}
	<-writeDone

	// Every id 200..259 inserted, none lost at the boundary.
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(260))
	assertCount(t, ctx, `SELECT count(DISTINCT id) FROM orders`, 260)

	// DML aimed at each partition's range after the flip, proving the new
	// owners actually receive their keys.
	dml(t, db, `UPDATE orders SET v = 'post-scale-low' WHERE id = 5`)
	dml(t, db, `UPDATE orders SET v = 'post-scale-mid' WHERE id = 130`)
	dml(t, db, `UPDATE orders SET v = 'post-scale-high' WHERE id = 250`)
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 5`, "post-scale-low")
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 130`, "post-scale-mid")
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 250`, "post-scale-high")

	dml(t, db, `DELETE FROM orders WHERE id = 60`)
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(259))
	assertMissing(t, ctx, `SELECT v FROM orders WHERE id = 60`)
	t.Log("scale-out ok: all 3 partitions routed, no loss, no duplicate")
}

// TestLiveRepartitionScaleIn proves the other direction: a table running on
// three workers is scaled back to one, and the surviving owner inherits the
// retired owners' key ranges with no loss and no duplicate.
func TestLiveRepartitionScaleIn(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	addr := reserveAddr(t)
	s := loadPipeline(t)
	s.Tables[0].Workers = &spec.WorkerSpec{Number: 3, Max: 8}
	if err := s.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	target := s.Tables[0].Target
	groups := s.Tables[0].WorkerGroupNames(s.Pipeline)

	db := mysqlConn(t)
	resetBinlog(t, db)
	dropIcebergTable(t, ctx)
	dropAll(t, db)
	seedOrders(t, db, 0, 200)

	p := bootScalable(t, ctx, addr, s, groups...)
	defer p.stop()
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(200))
	t.Log("snapshot converged with 3 partitions")

	for i := 200; i < 230; i++ {
		dml(t, db, fmt.Sprintf("INSERT INTO orders (id, v, amount) VALUES (%d, 'pre%d', %d.0)", i, i, i))
	}
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(230))

	if err := p.coord.ScaleTable(ctx, target, 1); err != nil {
		t.Fatalf("ScaleTable(%s, 1): %v", target, err)
	}
	t.Log("re-sliced down to 1 partition")

	// The surviving owner now owns the whole key domain: DML that used to
	// belong to the retired partitions must still land.
	dml(t, db, `UPDATE orders SET v = 'inherited-mid' WHERE id = 130`)
	dml(t, db, `UPDATE orders SET v = 'inherited-high' WHERE id = 220`)
	dml(t, db, `INSERT INTO orders (id, v, amount) VALUES (400, 'after-scale-in', 4.0)`)
	dml(t, db, `DELETE FROM orders WHERE id = 150`)

	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 130`, "inherited-mid")
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 220`, "inherited-high")
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 400`, "after-scale-in")
	assertMissing(t, ctx, `SELECT v FROM orders WHERE id = 150`)

	// 230 + 1 inserted - 1 deleted.
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(230))
	assertCount(t, ctx, `SELECT count(DISTINCT id) FROM orders`, 230)
	t.Log("scale-in ok: surviving owner inherited every range, no loss, no duplicate")
}

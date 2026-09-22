package e2e

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"strings"
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

	mu      sync.Mutex
	running map[string]bool
	stops   []context.CancelFunc
	done    chan error
}

func bootScalable(t *testing.T, ctx context.Context, addr string, s *spec.Spec, workers ...string) *scalablePipeline {
	t.Helper()
	p := &scalablePipeline{t: t, ctx: ctx, addr: addr, done: make(chan error, 64), running: map[string]bool{}}

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

// startWorker brings up one worker group, as a new pod would. It is
// idempotent: a name already running is left alone, so a re-slice can call it
// for a whole target layout and a worker retired by a previous scale is
// restarted when the layout brings its name back.
func (p *scalablePipeline) startWorker(name string) {
	p.t.Helper()
	p.mu.Lock()
	if p.running[name] {
		p.mu.Unlock()
		return
	}
	p.running[name] = true
	p.mu.Unlock()
	wCtx, wStop := context.WithCancel(p.ctx)
	p.track(wStop)
	go func() {
		err := worker.RunRemote(wCtx, worker.RemoteConfig{
			Coordinator: p.addr,
			Name:        name,
			Namespace:   "raw",
			Sink:        workerSink(),
			MaxRows:     100,
			MaxInterval: 2 * time.Second,
		})
		p.mu.Lock()
		delete(p.running, name)
		p.mu.Unlock()
		p.done <- err
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
			// A scale-in retires owners: the coordinator cancels their
			// session, and the worker process exits with the reset error.
			// That is the expected fate of a retired pod (KEDA would delete
			// it), not a failure. Anything else is a real error.
			if err != nil && !errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "session reset") {
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

// TestLiveRepartitionScaleOutAppend proves the re-slice is safe for an
// append-only table. There is no upsert, so nothing is overwritten and no
// carve-range snapshot is needed — the staged cycles just have to commit.
// Before the per-batch mode fix this failed the same way upsert did: the
// surviving owner committed directly while the coordinator expected a staged
// delivery, leaving its cycle open and blocking the new owners' cycles.
func TestLiveRepartitionScaleOutAppend(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	addr := reserveAddr(t)
	s := loadPipeline(t)
	s.Tables[0].WriteMode = spec.WriteModeAppend
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
	t.Log("append snapshot converged with 1 partition")

	// Inserts only: append mode has no upsert, so each row is a new append.
	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		for i := 200; i < 260; i++ {
			dml(t, db, fmt.Sprintf("INSERT INTO orders (id, v, amount) VALUES (%d, 'live%d', %d.0)", i, i, i))
			time.Sleep(40 * time.Millisecond)
		}
	}()

	time.Sleep(500 * time.Millisecond)
	if err := p.coord.ScaleTable(ctx, target, 3); err != nil {
		t.Fatalf("ScaleTable(%s, 3): %v", target, err)
	}
	t.Log("re-sliced to 3 partitions under live load (append)")
	scaledSpec := s.Tables[0]
	scaledSpec.Workers = &spec.WorkerSpec{Number: 3}
	for _, name := range scaledSpec.WorkerGroupNames(s.Pipeline)[1:] {
		p.startWorker(name)
	}
	<-writeDone

	// Every id 0..259 present exactly once: no loss at the boundary, and no
	// duplicate from a re-read (the failure mode an unsolicited snapshot
	// would cause in append mode).
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(260))
	assertCount(t, ctx, `SELECT count(DISTINCT id) FROM orders`, 260)
	t.Log("append scale-out ok: no loss, no duplicate")
}

// TestLiveRepartitionChaosScale hammers the re-slice: a mixed INSERT/UPDATE/
// DELETE writer keeps the source changing while the table is scaled up and
// down repeatedly (1→3→2→4→1→3→2), workers joining and leaving at each step.
// The sink must end up equal to the source exactly — every row, no stale
// value, no duplicate, no loss.
//
// The tidy tests exercise one flip with the load aimed after it. This one
// exercises back-to-back flips: workers retiring and rejoining, deletes
// crossing a moved boundary, and inserts that extend the key range under a
// running layout, so every re-slice resolves fresh boundaries.
func TestLiveRepartitionChaosScale(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
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
	t.Log("snapshot converged; starting chaos writer")

	stop := make(chan struct{})
	writeDone := make(chan struct{})
	// No pause: the source stays continuously busy across every re-slice.
	// The coordinator's barrier must converge on its own by pausing the
	// table's input during the drain.
	go func() {
		defer close(writeDone)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			var q string
			switch i % 3 {
			case 0:
				id := 300 + i/3 // monotonic: extends the key range
				q = fmt.Sprintf("INSERT INTO orders (id, v, amount) VALUES (%d, 'ins%d', %d.0)", id, i, id)
			case 1:
				id := (i / 3) % 200
				q = fmt.Sprintf("UPDATE orders SET v = 'upd%d' WHERE id = %d", i, id)
			case 2:
				id := 100 + (i/3)%50
				q = fmt.Sprintf("DELETE FROM orders WHERE id = %d", id)
			}
			if _, err := db.Exec(q); err != nil {
				t.Logf("chaos writer: %q: %v", q, err)
			}
			// 40ms keeps the source continuously busy without overwhelming
			// the e2e's RustFS, which returns 500s (commit retries exhausted)
			// above roughly this commit rate.
			time.Sleep(40 * time.Millisecond)
		}
	}()

	for _, n := range []int{3, 2, 4, 1, 3, 2} {
		err := p.coord.ScaleTable(ctx, target, n)
		if err != nil {
			t.Fatalf("ScaleTable(%s, %d): %v", target, n, err)
		}
		layout := s.Tables[0]
		layout.Workers = &spec.WorkerSpec{Number: n}
		for _, name := range layout.WorkerGroupNames(s.Pipeline) {
			p.startWorker(name)
		}
		t.Logf("re-sliced to %d partitions under chaos load", n)
		time.Sleep(1500 * time.Millisecond)
	}

	close(stop)
	<-writeDone

	// The sink must equal the source exactly. Read the source truth once (the
	// writer has stopped) and poll the sink until it converges.
	want := mysqlTable(t, db)
	t.Logf("source settled at %d rows; waiting for the sink to match", len(want))
	waitTrinoTable(t, ctx, want)
	t.Log("chaos scale ok: sink equals source exactly after repeated flips")
}

// mysqlTable reads the source's full (id → v) state.
func mysqlTable(t *testing.T, db *sql.DB) map[int64]string {
	t.Helper()
	rows, err := db.Query("SELECT id, v FROM orders")
	if err != nil {
		t.Fatalf("mysql read: %v", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[int64]string{}
	for rows.Next() {
		var id int64
		var v string
		if err := rows.Scan(&id, &v); err != nil {
			t.Fatalf("mysql scan: %v", err)
		}
		out[id] = v
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("mysql rows: %v", err)
	}
	return out
}

// trinoTable reads the sink's full (id → v) state.
func trinoTable(ctx context.Context) (map[int64]string, error) {
	rows, err := trinoQuery(ctx, "SELECT id, v FROM orders")
	if err != nil {
		return nil, err
	}
	out := map[int64]string{}
	for _, r := range rows {
		if len(r) != 2 {
			return nil, fmt.Errorf("row has %d columns, want 2", len(r))
		}
		id, ok := r[0].(int64)
		if !ok {
			return nil, fmt.Errorf("id column is %T, want int64", r[0])
		}
		v, ok := r[1].(string)
		if !ok {
			return nil, fmt.Errorf("v column is %T, want string", r[1])
		}
		out[id] = v
	}
	return out, nil
}

// waitTrinoTable polls the sink until its full (id → v) state equals want, or
// fails with a bounded diff after the deadline.
func waitTrinoTable(t *testing.T, ctx context.Context, want map[int64]string) {
	t.Helper()
	deadline := time.Now().Add(120 * time.Second)
	var lastErr error
	for {
		got, err := trinoTable(ctx)
		if err == nil && maps.Equal(got, want) {
			return
		}
		lastErr = err
		if time.Now().After(deadline) {
			got, _ = trinoTable(ctx)
			missing, extra, wrong := diffTables(want, got)
			t.Fatalf("sink never matched source: want %d rows, got %d; missing=%v extra=%v wrongValue=%v lastErr=%v",
				len(want), len(got), cap10(missing), cap10(extra), cap10(wrong), lastErr)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// diffTables reports the ids the sink is missing, has extra, or holds with a
// stale value.
func diffTables(want, got map[int64]string) (missing, extra, wrong []int64) {
	for id, v := range want {
		gv, ok := got[id]
		switch {
		case !ok:
			missing = append(missing, id)
		case gv != v:
			wrong = append(wrong, id)
		}
	}
	for id := range got {
		if _, ok := want[id]; !ok {
			extra = append(extra, id)
		}
	}
	return missing, extra, wrong
}

// cap10 bounds a diff slice for the failure message.
func cap10(ids []int64) []int64 {
	if len(ids) > 10 {
		return ids[:10]
	}
	return ids
}

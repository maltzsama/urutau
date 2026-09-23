package e2e

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go/table"

	"github.com/maltzsama/urutau/internal/coordinator"
	"github.com/maltzsama/urutau/internal/grpctls"
	"github.com/maltzsama/urutau/internal/maintenance"
	icebergsink "github.com/maltzsama/urutau/internal/sink/iceberg"
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
			// The stall detector's default (30s) is tight for the e2e
			// stack (minikube + port-forward); give a slow snapshot room,
			// and a slow commit room to drain before a re-slice flips.
			AckTimeout:        2 * time.Minute,
			ScaleDrainTimeout: 3 * time.Minute,
			OnReady:           func(c *coordinator.Coordinator) { ready <- c },
		})
	}()

	select {
	case c := <-ready:
		p.coord = c
	case <-time.After(2 * time.Minute):
		t.Fatal("coordinator never became ready")
	}

	// OnReady fires when routing is published, but the gRPC listener only
	// binds after the sink is opened, every table is ensured, and the resume
	// position is read — seconds on a warm catalog, minutes on the
	// port-forwarded e2e stack. Wait for the port so the workers do not burn
	// through their handshake backoff, and give that boot phase room.
	dialDeadline := time.Now().Add(3 * time.Minute)
	for {
		conn, err := net.DialTimeout("tcp", addr, 250*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			break
		}
		if time.Now().After(dialDeadline) {
			t.Fatalf("coordinator listener never came up at %s: %v", addr, err)
		}
		time.Sleep(100 * time.Millisecond)
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

// startMaintenanceWorker launches one ephemeral maintenance pass, exactly as
// the coordinator's scheduler would provision it: a Hello marked Maintenance,
// one assigned pass, then exit.
func (p *scalablePipeline) startMaintenanceWorker(name string) {
	p.t.Helper()
	wCtx, wStop := context.WithCancel(p.ctx)
	p.track(wStop)
	go func() {
		p.done <- worker.RunMaintenance(wCtx, worker.RemoteConfig{
			Coordinator: p.addr,
			Name:        name,
			Namespace:   "raw",
			Sink:        workerSink(),
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
			// A scale-in retires owners: the coordinator cancels their
			// session, and the worker process exits with the reset error.
			// That is the expected fate of a retired pod (KEDA would delete
			// it), not a failure. A maintenance pass rejected because a
			// previous one is still running ("already connected") is the
			// same — expected churn, and the test's own assertions are the
			// real check. Anything else is a real error.
			if err != nil && !errors.Is(err, context.Canceled) &&
				!strings.Contains(err.Error(), "session reset") &&
				!strings.Contains(err.Error(), "maintenance") {
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
	s.Tables[0].Workers = &spec.WorkerSpec{Number: 1, Max: 8}
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

// TestLiveRepartitionChaosScale hammers the re-slice under a continuously
// busy source: a mixed INSERT/UPDATE/DELETE writer keeps the table changing
// while it is scaled up and down repeatedly (1→3→2→4→1→3→2), workers joining
// and leaving at each step. The sink must end up equal to the source exactly
// — every row, no stale value, no duplicate, no loss.
//
// The tidy tests exercise one flip with the load aimed after it. This one
// exercises back-to-back flips: workers retiring and rejoining, deletes
// crossing a moved boundary, and inserts that extend the key range under a
// running layout, so every re-slice resolves fresh boundaries.
func TestLiveRepartitionChaosScale(t *testing.T) {
	runChaosScale(t, false)
}

// TestLiveRepartitionChaosScaleWithMaintenance is the same stress with table
// maintenance running alongside it: a compaction pass starts before each
// scale, so its RewriteDataFiles commits to the table the coordinator is
// draining and flipping, and it reads cdc.position while the re-slice
// advances it. Compaction must still run, the position must survive, and the
// sink must still equal the source.
func TestLiveRepartitionChaosScaleWithMaintenance(t *testing.T) {
	runChaosScale(t, true)
}

func runChaosScale(t *testing.T, withMaintenance bool) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	addr := reserveAddr(t)
	s := loadPipeline(t)
	s.Tables[0].Workers = &spec.WorkerSpec{Number: 1, Max: 8}
	if withMaintenance {
		// The 1s interval makes compaction due for every pass.
		s.Sink.Maintenance = &spec.Maintenance{
			Enabled:    true,
			Compaction: &spec.CompactionConfig{MinInputFiles: 2, Interval: "1s"},
		}
	}
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
	var before int
	if withMaintenance {
		before = tableFileCount(t, ctx, "orders")
	}
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
			// 150ms keeps the source continuously busy while staying inside
			// what the e2e's RustFS sustains (it is effectively single-core
			// and returns 500s above this rate, more so with compaction).
			time.Sleep(150 * time.Millisecond)
		}
	}()

	maintName := maintenance.WorkerName(s.Pipeline, target)
	for _, n := range []int{3, 2, 4, 1, 3, 2} {
		if withMaintenance {
			// A pass starts before each step, so compaction commits to the
			// same table while the coordinator drains and flips it.
			p.startMaintenanceWorker(maintName)
		}
		if err := p.coord.ScaleTable(ctx, target, n); err != nil {
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

	if withMaintenance {
		// Compaction must have run despite the churn, and the position must
		// survive (compaction carries cdc.position forward; the re-slice must
		// not regress it).
		waitCompactedFrom(t, ctx, "orders", before)
		if pos := tablePosition(t, ctx, "orders"); pos == "" {
			t.Fatal("cdc.position is empty after chaos + maintenance — the position was lost")
		}
	}

	// The sink must equal the source exactly.
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

// tableFileCount returns the number of data files a target table currently
// has, retrying a transient catalog/S3 error — the e2e's RustFS returns 500s
// under load, which is an artifact of the test stack, not of the engine.
func tableFileCount(t *testing.T, ctx context.Context, tableName string) int {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for {
		n, err := tryTableFileCount(ctx, tableName)
		if err == nil {
			return n
		}
		lastErr = err
		if time.Now().After(deadline) {
			t.Fatalf("data-file count %s: %v", tableName, lastErr)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func tryTableFileCount(ctx context.Context, tableName string) (int, error) {
	cat, err := icebergsink.NewCatalog(ctx, e2eIcebergCatalog())
	if err != nil {
		return 0, fmt.Errorf("catalog: %w", err)
	}
	tbl, err := cat.LoadTable(ctx, table.Identifier{"raw", tableName})
	if err != nil {
		return 0, fmt.Errorf("load table %s: %w", tableName, err)
	}
	tasks, err := tbl.Scan().PlanFiles(ctx)
	if err != nil {
		return 0, fmt.Errorf("plan files %s: %w", tableName, err)
	}
	return len(tasks), nil
}

// waitCompactedFrom polls until the table's data-file count drops below
// before — compaction has visibly run — or fails naming what it saw. A
// transient catalog error is retried, not fatal.
func waitCompactedFrom(t *testing.T, ctx context.Context, tableName string, before int) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	var lastErr error
	var lastCount int
	for time.Now().Before(deadline) {
		n, err := tryTableFileCount(ctx, tableName)
		if err != nil {
			lastErr = err
		} else {
			lastCount = n
			if n < before {
				t.Logf("maintenance ok: %s compacted to %d data files (from %d)", tableName, n, before)
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("%s still has %d data files (want fewer than %d) — compaction did not run during the re-slice (lastErr=%v)",
		tableName, lastCount, before, lastErr)
}

// tablePosition reads the target table's committed cdc.position (the fast
// resume path), or "" when absent, retrying a transient catalog error.
func tablePosition(t *testing.T, ctx context.Context, tableName string) string {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for {
		cat, err := icebergsink.NewCatalog(ctx, e2eIcebergCatalog())
		if err == nil {
			var pos string
			if pos, err = icebergsink.CommittedPosition(ctx, cat, table.Identifier{"raw", tableName}); err == nil {
				return pos
			}
		}
		lastErr = err
		if time.Now().After(deadline) {
			t.Fatalf("committed position %s: %v", tableName, lastErr)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// e2eIcebergCatalog is the e2e Polaris catalog config the test helpers read
// through.
func e2eIcebergCatalog() icebergsink.Config {
	return icebergsink.Config{
		URI:          env("URUTAU_E2E_CATALOG", "http://localhost:8181/api/catalog"),
		Warehouse:    env("URUTAU_E2E_WAREHOUSE", "quickstart_catalog"),
		ClientID:     "root",
		ClientSecret: "s3cr3t",
		Scope:        "PRINCIPAL_ROLE:ALL",
	}
}

// TestLiveRepartitionMultiTable proves issue #343 end to end with THREE
// tables streaming together: re-slicing one — OUT and IN — must not stall or
// lose any of them. Real pipelines carry several tables, and the pump is
// shared, so the regression was that pausing ONE table parked the pump and
// every OTHER table's batches queued behind it for the whole drain.
//
// orders is re-sliced (1→3→1); order_items and order_audit are bystanders.
// ALL THREE write continuously, so orders' drain runs against live load: its
// input is paused, its batches buffered and re-routed after the flip, and
// nothing may be lost or duplicated. While each re-slice is in flight the
// test waits for BOTH bystanders' sink counts to advance — the direct
// integration check that the pump kept serving them mid-drain.
//
// The deterministic proof that a pause does not PARK the pump is
// TestPumpDoesNotParkOnPausedTable — a unit test that fails with the old
// behaviour. The live layout (OwnerNames) is observed at each scale so a flip
// that silently keeps the old owner set fails here, not on a later
// convergence.
func TestLiveRepartitionMultiTable(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	addr := reserveAddr(t)
	s := loadPipeline(t)
	s.Tables[0].Workers = &spec.WorkerSpec{Number: 1, Max: 8}
	s.Tables = append(s.Tables,
		spec.Table{
			Source:            "shop.order_items",
			Target:            "raw.order_items",
			PrimaryKey:        []string{"order_id", "line_no"},
			CreateIfNotExists: true,
		},
		spec.Table{
			Source:            "shop.order_audit",
			Target:            "raw.order_audit",
			PrimaryKey:        []string{"id"},
			CreateIfNotExists: true,
		},
	)
	if err := s.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	ordersTarget := s.Tables[0].Target

	var boot []string
	for _, tbl := range s.Tables {
		boot = append(boot, tbl.WorkerGroupNames(s.Pipeline)...)
	}

	db := mysqlConn(t)
	resetBinlog(t, db)
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS order_items (
		order_id BIGINT NOT NULL,
		line_no  INT    NOT NULL,
		sku      VARCHAR(64) NOT NULL,
		qty      INT    NOT NULL,
		PRIMARY KEY (order_id, line_no))`); err != nil {
		t.Fatalf("create order_items: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS order_audit (
		id   BIGINT      NOT NULL PRIMARY KEY,
		note VARCHAR(64) NOT NULL)`); err != nil {
		t.Fatalf("create order_audit: %v", err)
	}
	for _, tbl := range []string{"order_items", "order_audit"} {
		if _, err := db.Exec("DELETE FROM " + tbl); err != nil {
			t.Fatalf("clear %s: %v", tbl, err)
		}
	}
	dropIcebergTable(t, ctx)
	dropIcebergNamed(t, ctx, "raw.order_items")
	dropIcebergNamed(t, ctx, "raw.order_audit")
	dropAll(t, db)
	seedOrders(t, db, 0, 300)
	for i := 0; i < 100; i++ {
		dml(t, db, fmt.Sprintf("INSERT INTO order_items (order_id, line_no, sku, qty) VALUES (%d, 1, 'sku%d', %d)", i, i, i%7+1))
	}
	for i := 0; i < 50; i++ {
		dml(t, db, fmt.Sprintf("INSERT INTO order_audit (id, note) VALUES (%d, 'seed%d')", i, i))
	}

	p := bootScalable(t, ctx, addr, s, boot...)
	defer p.stop()
	// The port-forwarded stack is slow; give the initial snapshot room.
	waitTrinoWithin(t, ctx, `SELECT count(*) FROM orders`, int64(300), 3*time.Minute)
	waitTrinoWithin(t, ctx, `SELECT count(*) FROM order_items`, int64(100), 3*time.Minute)
	waitTrinoWithin(t, ctx, `SELECT count(*) FROM order_audit`, int64(50), 3*time.Minute)
	t.Log("all three tables converged with 1 partition each")

	// The live layout starts at one owner for the re-sliced table.
	if got := p.coord.OwnerNames(ordersTarget); len(got) != 1 {
		t.Fatalf("boot owners = %v, want 1", got)
	}

	// All three tables write CONTINUOUSLY across the whole scale-in/out
	// sequence. orders is re-sliced: its input is paused for the drain, its
	// batches buffered and re-routed after the flip. order_items and
	// order_audit are bystanders: they must keep flowing on the same pump.
	var mu sync.Mutex
	ordersWritten, itemsWritten, auditWritten := 0, 0, 0
	stop := make(chan struct{})
	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			dml(t, db, fmt.Sprintf("INSERT INTO orders (id, v, amount) VALUES (%d, 'live%d', %d.0)", 1000+i, i, i))
			dml(t, db, fmt.Sprintf("INSERT INTO order_items (order_id, line_no, sku, qty) VALUES (%d, 1, 'chaos%d', %d)", 1000+i, i, i%7+1))
			dml(t, db, fmt.Sprintf("INSERT INTO order_audit (id, note) VALUES (%d, 'note%d')", 1000+i, i))
			mu.Lock()
			ordersWritten++
			itemsWritten++
			auditWritten++
			mu.Unlock()
			time.Sleep(50 * time.Millisecond)
		}
	}()

	// Scale OUT then IN. The scale runs in the background while the test
	// watches BOTH bystanders advance in the sink: if the pump parked on the
	// paused table, their commits would stall for the whole drain and this
	// wait would fail. Then the live layout must show exactly n owners, and a
	// fresh key must be routed by the new layout.
	extra := 0
	for _, n := range []int{3, 1} {
		before := map[string]int64{
			"order_items": trinoCount(t, ctx, "order_items"),
			"order_audit": trinoCount(t, ctx, "order_audit"),
		}
		errCh := make(chan error, 1)
		go func() { errCh <- p.coord.ScaleTable(ctx, ordersTarget, n) }()
		for _, bt := range []string{"order_items", "order_audit"} {
			waitTrinoAbove(t, ctx, "SELECT count(*) FROM "+bt, before[bt], 2*time.Minute)
		}
		if err := <-errCh; err != nil {
			t.Fatalf("ScaleTable(%s, %d): %v", ordersTarget, n, err)
		}
		if got := p.coord.OwnerNames(ordersTarget); len(got) != n {
			t.Fatalf("after scale to %d: owners = %v, want %d", n, got, n)
		}

		// Bring up the owners the new layout needs, then write a key the flip
		// moved: it must land under the new layout.
		scaled := s.Tables[0]
		scaled.Workers = &spec.WorkerSpec{Number: n}
		for _, name := range scaled.WorkerGroupNames(s.Pipeline) {
			p.startWorker(name)
		}
		id := 9000 + extra
		dml(t, db, fmt.Sprintf("INSERT INTO orders (id, v, amount) VALUES (%d, 'post-scale-%d', 1.0)", id, n))
		extra++
		waitTrinoWithin(t, ctx, fmt.Sprintf("SELECT v FROM orders WHERE id = %d", id), fmt.Sprintf("post-scale-%d", n), 2*time.Minute)
		t.Logf("re-sliced orders to %d under live load; owners=%v", n, p.coord.OwnerNames(ordersTarget))
	}
	close(stop)
	<-writeDone
	mu.Lock()
	ow, iw, aw := ordersWritten, itemsWritten, auditWritten
	mu.Unlock()

	// Every table converges exactly across both flips: no loss, no duplicate.
	totalOrders := int64(300 + ow + extra)
	waitTrinoWithin(t, ctx, `SELECT count(*) FROM orders`, totalOrders, 5*time.Minute)
	assertCount(t, ctx, `SELECT count(DISTINCT id) FROM orders`, totalOrders)
	waitTrinoWithin(t, ctx, `SELECT count(*) FROM order_items`, int64(100+iw), 5*time.Minute)
	assertCount(t, ctx, `SELECT count(DISTINCT order_id) FROM order_items`, int64(100+iw))
	waitTrinoWithin(t, ctx, `SELECT count(*) FROM order_audit`, int64(50+aw), 5*time.Minute)
	assertCount(t, ctx, `SELECT count(DISTINCT id) FROM order_audit`, int64(50+aw))
	t.Log("all three tables converged after scale-out and scale-in: no loss, no duplicate")
}

// waitTrinoWithin polls until query returns want, failing after within. The
// shared waitTrino's 60s deadline is too tight for a re-sliced table draining
// under load on the port-forwarded e2e stack.
func waitTrinoWithin(t *testing.T, ctx context.Context, query string, want any, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		rows, err := trinoQuery(ctx, query)
		if err == nil && len(rows) == 1 && len(rows[0]) == 1 && rows[0][0] == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("trino wait %q: rows=%v err=%v want %v within %s", query, rows, err, want, within)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// trinoCount returns a table's row count from the sink, retrying a transient
// Trino/catalog error (the port-forwarded stack is not always warm).
func trinoCount(t *testing.T, ctx context.Context, table string) int64 {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for {
		rows, err := trinoQuery(ctx, "SELECT count(*) FROM "+table)
		switch {
		case err != nil:
			lastErr = err
		case len(rows) != 1 || len(rows[0]) != 1:
			lastErr = fmt.Errorf("count %s: rows=%v", table, rows)
		default:
			if n, ok := rows[0][0].(int64); ok {
				return n
			}
			lastErr = fmt.Errorf("count %s: %T", table, rows[0][0])
		}
		if time.Now().After(deadline) {
			t.Fatalf("count %s: %v", table, lastErr)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// waitTrinoAbove polls until a scalar query exceeds base, failing after
// within. It is how the test observes a bystander table still landing rows in
// the sink WHILE another table's re-slice drains.
func waitTrinoAbove(t *testing.T, ctx context.Context, query string, base int64, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		rows, err := trinoQuery(ctx, query)
		if err == nil && len(rows) == 1 && len(rows[0]) == 1 {
			if n, ok := rows[0][0].(int64); ok && n > base {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("trino wait above %d: %q: rows=%v err=%v within %s", base, query, rows, err, within)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

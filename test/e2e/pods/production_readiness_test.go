package pods

import (
	"context"
	"database/sql"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestProductionReadinessWorkload runs the stochastic three-table workload
// (issue #384) against the real MySQL → Iceberg pipeline and checks
// MySQL source state → oracle → Iceberg logical state exactly.
//
// URUTAU_E2E_PROFILE picks smoke (default), full or full-100k; URUTAU_E2E_TABLES
// (e.g. "accounts,items") narrows the run to some tables, for debugging only:
// the coverage checks still expect all three. URUTAU_E2E_SEED replays
// a run's random choices (the timing, and so the regime boundaries, still
// varies). The diagnostics file (seed, observed distributions, final diffs)
// is written under URUTAU_E2E_ARTIFACTS, or the system temp dir, pass or fail.
func TestProductionReadinessWorkload(t *testing.T) {
	runProductionReadiness(t, prOptions{pipeline: "pod-pr-workload", serverID: "2320"})
}

// TestProductionReadinessChaos is the workload with the nondeterministic
// Chaos Mesh controller (issue #385) injecting faults into the live
// coordinator and worker Pods throughout the profile's whole live window,
// while KEDA scales the partitioned tables out and in (live re-slicing) and
// every maintenance operation runs, as in TestProductionReadinessMatrix. It
// passes on the matrix's conditions: every table converges to MySQL exactly,
// KEDA scaled out and back in, maintenance compacted, expired and cleaned,
// and every experiment was injected and removed. URUTAU_E2E_SEED seeds the
// fault stream too.
func TestProductionReadinessChaos(t *testing.T) {
	var m *matrixSampler
	runProductionReadiness(t, prOptions{
		pipeline: "pod-pr-chaos", serverID: "2321", chaos: true,
		kedaMax: 4, maintenance: matrixMaintenance, logLevel: "debug",
		onLive: func(ctx context.Context, r *prRun) {
			m = newMatrixSampler(r)
			m.start(ctx)
		},
		afterLive: func(ctx context.Context, r *prRun) {
			if err := m.plantOrphan(ctx); err != nil {
				r.t.Fatalf("plant orphan: %v", err)
			}
			r.t.Logf("planted orphan %s", m.orphan)
		},
		afterSettle: func(ctx context.Context, r *prRun) {
			m.stop()
			m.check(ctx)
		},
	})
}

// prOptions shapes one production-readiness run.
type prOptions struct {
	pipeline, serverID string
	chaos              bool           // the chaos controller over the live window
	kedaMax            int            // > 0: partitioned tables get workers.max for KEDA
	logLevel           string         // the pipeline's spec.logLevel
	maintenance        map[string]any // non-nil: the sink's maintenance block
	live               time.Duration  // > 0: overrides the profile's live window
	settle             time.Duration  // > 0: overrides the profile's settle timeout
	// onLive runs once the coordinator is up, before the live window
	// elapses; afterLive once the workload has stopped, before the settle;
	// afterSettle after the final comparison. All see the run.
	onLive      func(ctx context.Context, r *prRun)
	afterLive   func(ctx context.Context, r *prRun)
	afterSettle func(ctx context.Context, r *prRun)
}

// prRun is what a hook sees of a run in progress.
type prRun struct {
	t            *testing.T
	w            *workload
	chaos        *chaosController
	mysql, trino *sql.DB
	pipeline     string
	tables       []*prTable
	profile      workloadProfile
}

// runProductionReadiness is the shared body: the workload, optionally with
// the chaos controller, KEDA and maintenance, and the hooks.
func runProductionReadiness(t *testing.T, o prOptions) {
	t.Helper()
	requirePods(t)
	withChaos := o.chaos
	if withChaos {
		verifyChaosMeshReady(t)
	}
	profile, err := selectedProfile()
	if err != nil {
		t.Fatal(err)
	}
	if o.live > 0 {
		profile.Duration = o.live
	}
	if o.settle > 0 {
		profile.Settle = o.settle
	}
	seed, err := workloadSeed()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("production-readiness workload: profile=%s seed=%d (replay with URUTAU_E2E_SEED=%d)", profile.Name, seed, seed)

	// The live window and the settle, plus boot, seeding and teardown. The
	// documented -timeout for each profile sits above this budget.
	budget := profile.Duration + profile.Settle + 15*time.Minute
	if o.afterSettle != nil {
		budget += 15 * time.Minute // the hook's own checks (e.g. waiting for KEDA to scale in)
	}
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	mysql, trino := setupPodEnv(t)
	suffix := strconv.FormatInt(time.Now().UnixNano()%2176782336, 36)
	tables := selectTables(t, productionTables(suffix, profile))
	w := newWorkload(seed, profile, tables, mysql)
	pipeline, serverID := o.pipeline, o.serverID
	var chaos *chaosController
	var progress *progressSampler
	dir, err := runArtifactsDir(profile.Name, seed)
	if err != nil {
		t.Fatalf("artifacts dir: %v", err)
	}
	t.Logf("artifacts: %s", dir)
	t.Cleanup(func() {
		rep := w.report()
		if chaos != nil {
			rep.Chaos = chaos.report()
		}
		if progress != nil {
			rep.Progress, _ = progress.report()
		}
		if t.Failed() {
			rep.Failure = "see the test log"
		}
		if path, err := writeReport(rep); err != nil {
			t.Logf("write diagnostics: %v", err)
		} else {
			t.Logf("diagnostics: %s", path)
		}
	})

	if err := w.createTables(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// A failed run keeps its source tables, so the rows the diff names
		// can still be read back.
		if t.Failed() {
			t.Logf("keeping source tables %v for inspection", tableNames(tables))
			return
		}
		w.dropTables()
	})
	if err := w.seedInitial(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("seeded %d rows per table", profile.InitialRows)

	specs := make([]tableSpec, len(tables))
	for i, tb := range tables {
		specs[i] = tableSpec{Source: "shop." + tb.Name, Target: "raw." + tb.Target, PrimaryKey: tb.PK, Workers: tb.Workers, Cast: tb.Cast}
		if o.kedaMax > 0 && tb.Workers > 1 {
			specs[i].Max = o.kedaMax
		}
	}
	opts := crOptions{MaintenanceBlock: o.maintenance, LogLevel: o.logLevel}
	if profile.fullLoad() {
		// The race-instrumented image needs far more CPU and memory than the
		// production binary: the race detector alone adds ~1.5 GiB outside
		// the Go heap, and serializes CPU work. These are the OPERATOR's
		// explicit resources for the race e2e (spec, not a urutau default) —
		// the product's fail-closed 500m/1Gi floor still governs a real
		// pipeline whose spec omits them. Without them the race coordinator
		// OOM-kills at 2Gi and the events worker OOM-kills at 3Gi during a
		// snapshot (Sourcery finding on this PR).
		opts.CoordinatorMemory = "6Gi"
		opts.CoordinatorCPU = "4"
		// The harness worker's request is 1Gi; the race worker OOM-killed
		// with a 3Gi limit, so this raises the limit to 5Gi.
		opts.WorkerMemoryOverhead = "4Gi"
		opts.WorkerCPUOverhead = "1500m"
	}
	cr := buildCR(pipeline, testNS, raceImage(), "pod-e2e-source", "pod-e2e-catalog", serverID, specs, opts)
	applied := time.Now()
	applyPipeline(t, testNS, pipeline, cr)
	// Registered after applyPipeline, so it runs before the pipeline is torn
	// down: the logs are flushed, and a failure is dumped while the Pods,
	// the chaos resources and the tables still exist.
	logs := newLogCollector(dir, testNS, pipeline)
	if err := logs.start(); err != nil {
		t.Fatalf("log collector: %v", err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			dctx, dcancel := context.WithTimeout(context.Background(), 5*time.Minute)
			dumpDiagnostics(dctx, dir, testNS, pipeline, trino, tables)
			dumpStates(dctx, dir, w, trino)
			dcancel()
			t.Logf("diagnostics dumped under %s", dir)
		}
		logs.stop()
		// The trail outlives every replaced Pod, so it is kept pass or fail.
		tctx, tcancel := context.WithTimeout(context.Background(), 5*time.Minute)
		n, err := dumpTrail(tctx, dir, pipeline, applied)
		tcancel()
		t.Logf("run trail: %d coordinator run(s) under %s/trail (err %v)", n, dir, err)
	})

	// Live mutations start before the coordinator is up, so they overlap
	// the snapshot as well as the stream.
	w.start(ctx)
	waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 4*time.Minute)
	t.Logf("coordinator up; workload live for %s", profile.Duration)
	if withChaos {
		chaos = newChaosController(testNS, pipeline, seed, chaosProfileFor(profile), w.phase)
		chaos.start(ctx)
		t.Cleanup(func() {
			if chaos.stopCh != nil {
				select {
				case <-chaos.stopCh:
				default:
					chaos.stop()
				}
			}
		})
		t.Log("chaos controller started")
	}
	// Committed positions and per-table progress, sampled over the live
	// window and the settle. A restart pauses every table at once; a table
	// left behind for 5 minutes while others advance is starved.
	progress = newProgressSampler(w, trino, 15*time.Second, 5*time.Minute)
	progress.start(ctx)
	t.Cleanup(progress.stop) // an early failure must not leave it polling through teardown
	run := &prRun{t: t, w: w, chaos: chaos, mysql: mysql, trino: trino, pipeline: pipeline, tables: tables, profile: profile}
	if o.onLive != nil {
		o.onLive(ctx, run)
	}
	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-time.After(time.Until(w.liveFrom.Add(profile.Duration))):
	}
	if chaos != nil {
		// Faults end with the live window; the settle measures recovery.
		chaos.stop()
		rep := chaos.report()
		t.Logf("chaos stopped: %d experiment(s), injected by kind %v", len(rep.Events), rep.Counts)
	}
	if err := w.stop(ctx); err != nil {
		t.Fatalf("stop workload: %v", err)
	}
	t.Logf("workload stopped; expected source position %s", w.finalPos)
	if o.afterLive != nil {
		o.afterLive(ctx, run)
	}

	reconnect := func() error {
		return startPortForward(t, dataNS, "svc/trino", localTrinoPort, 8080, 30*time.Second)
	}
	checks, err := w.settle(ctx, trino, t.Logf, reconnect)
	progress.stop()
	if err != nil {
		t.Fatalf("settle: %v", err)
	}
	_, sampled := progress.report()
	for _, p := range append(sampled, progress.coverageProblems(tables)...) {
		t.Errorf("progress: %s", p)
	}
	// Position, independently of the rows.
	for _, p := range positionProblems(ctx, w, trino) {
		t.Errorf("position: %s", p)
	}
	// Every table reads back through Trino, snapshots and properties too.
	for _, tb := range tables {
		for _, q := range []string{
			"SELECT count(*) FROM " + tb.Target,
			`SELECT count(*) FROM "` + tb.Target + `$snapshots"`,
			`SELECT count(*) FROM "` + tb.Target + `$properties"`,
		} {
			var n int64
			if err := trino.QueryRowContext(ctx, q).Scan(&n); err != nil {
				t.Errorf("trino: %s: %v", q, err)
			}
		}
	}
	w.checks = checks
	for _, tb := range tables {
		rows, bytes, err := sinkCommits(ctx, trino, tb)
		if err != nil {
			t.Errorf("sink commit stats: %v", err)
			rows, bytes = newDist(), newDist()
		}
		w.sinkRows, w.sinkBytes = append(w.sinkRows, rows), append(w.sinkBytes, bytes)
	}

	for i, c := range checks {
		if !c.OracleVsSource.empty() {
			t.Errorf("%s: oracle diverged from MySQL (a workload bug): %s", tables[i].Name, c.OracleVsSource)
		}
		if !c.SourceVsSink.empty() {
			t.Errorf("%s: Iceberg != MySQL: %s", tables[i].Name, c.SourceVsSink)
		}
	}
	for _, p := range w.coverageProblems() {
		t.Errorf("coverage: %s", p)
	}
	if o.afterSettle != nil {
		o.afterSettle(ctx, run)
	}
	if chaos != nil {
		for _, p := range chaos.problems() {
			t.Errorf("chaos: %s", p)
		}
	}
	w.mu.Lock()
	for _, e := range w.errs {
		t.Errorf("workload: %s", e)
	}
	w.mu.Unlock()
	// Every container of the run, restarted and replaced ones included.
	logs.stop()
	for _, r := range logs.races() {
		t.Errorf("data race in %s", r)
	}
	assertNoRaces(t, testNS, pipeline+"-")
}

// selectTables applies URUTAU_E2E_TABLES, a comma-separated list of table
// kinds (accounts, items, events). Empty keeps all three.
func selectTables(t *testing.T, all []*prTable) []*prTable {
	t.Helper()
	v := os.Getenv("URUTAU_E2E_TABLES")
	if v == "" {
		return all
	}
	var out []*prTable
	for _, k := range strings.Split(v, ",") {
		found := false
		for _, tb := range all {
			if string(tb.Kind) == strings.TrimSpace(k) {
				out, found = append(out, tb), true
			}
		}
		if !found {
			t.Fatalf("URUTAU_E2E_TABLES: unknown table kind %q", k)
		}
	}
	return out
}

func tableNames(tables []*prTable) []string {
	out := make([]string, len(tables))
	for i, tb := range tables {
		out[i] = "shop." + tb.Name
	}
	return out
}

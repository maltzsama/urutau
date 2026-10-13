package pods

// TestBenchmark is the benchmark harness of issue #375: one configured run
// of the production-readiness workload against a CDC engine, measured.
//
// The harness owns the source tables, the load, the oracle and every
// measurement; the engine only has to carry shop.<table> to raw.<table>. With
// engine "urutau" it applies the CDCPipeline itself. With engine "external"
// it prints the tables and waits: whoever runs the other engine starts it
// against them, and the same configuration and seed give it the same
// workload.
//
// A run writes benchmark-<name>-<seed>.json under URUTAU_E2E_ARTIFACTS: the
// configuration, the seed, the versions and the node, what the workload
// actually produced, end-to-end latency, resource usage, the sink's commits,
// and whether Iceberg ended equal to MySQL. Numbers from a run whose final
// state does not match are not valid, and the file says so.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"
	"time"
)

// benchThroughput is what one table's stream produced over the measured
// part of the live window, and what the sink committed for it.
type benchThroughput struct {
	Table              string           `json:"table"`
	Kind               tableKind        `json:"kind"`
	Workers            int              `json:"workers"`
	Ops                map[string]int64 `json:"ops"`
	Transactions       int64            `json:"transactions"`
	MutationsPerSecond distSummary      `json:"mutationsPerSecond"`
	RowsPerTxn         distSummary      `json:"rowsPerTransaction"`
	BytesPerTxn        distSummary      `json:"payloadBytesPerTransaction"`
	PayloadBytes       distSummary      `json:"payloadBytesPerRow"`
	// MeanRowsPerSecond and MeanPayloadBytesPerSecond are totals over the
	// whole live window divided by its length.
	MeanRowsPerSecond         float64     `json:"meanRowsPerSecond"`
	MeanPayloadBytesPerSecond float64     `json:"meanPayloadBytesPerSecond"`
	SinkRowsPerCommit         distSummary `json:"sinkRowsPerCommit"`
	SinkBytesPerCommit        distSummary `json:"sinkBytesPerCommit"`
	SinkCommits               int64       `json:"sinkCommits"`
	SinkBytesWritten          float64     `json:"sinkBytesWritten"`
}

// benchEnvironment is where a run happened: enough to tell two results
// apart, or to set the same thing up again.
type benchEnvironment struct {
	Revision   string      `json:"revision,omitempty"` // URUTAU_BENCH_REVISION, e.g. the git commit
	Image      string      `json:"image,omitempty"`
	Kubernetes string      `json:"kubernetes,omitempty"`
	MySQL      string      `json:"mysql,omitempty"`
	Trino      string      `json:"trino,omitempty"`
	Nodes      []benchNode `json:"nodes,omitempty"`
	Pipeline   string      `json:"pipeline,omitempty"`
	Manifest   string      `json:"engineManifestFile,omitempty"`
}

type benchNode struct {
	Name   string `json:"name"`
	CPU    string `json:"cpu"`
	Memory string `json:"memory"`
	OS     string `json:"osImage"`
	Kernel string `json:"kernelVersion"`
	Arch   string `json:"architecture"`
}

// benchResults is the machine-readable result of one run.
type benchResults struct {
	Config      *benchConfig     `json:"config"`
	Seed        uint64           `json:"seed"`
	Environment benchEnvironment `json:"environment"`

	Started      time.Time `json:"started"`
	SeedSeconds  float64   `json:"seedSeconds"`
	LiveFrom     time.Time `json:"liveFrom"`
	MeasuredFrom time.Time `json:"measuredFrom"`
	LiveTo       time.Time `json:"liveTo"`
	// SettleSeconds is how long after the live window the sink took to equal
	// the source: the backlog recovery time.
	SettleSeconds float64 `json:"settleSeconds"`
	// SourcePosition is gtid_executed once the last mutation committed.
	SourcePosition string `json:"sourcePosition"`

	Throughput []benchThroughput `json:"throughput"`
	Latency    []latencyReport   `json:"endToEndLatency"`
	// LatencyResolutionSeconds is the probe's poll interval: a latency is an
	// upper bound by at most this much plus the probe's own query.
	LatencyResolutionSeconds float64        `json:"latencyResolutionSeconds"`
	LatencyProbeErrors       int            `json:"latencyProbeErrors"`
	LatencyProbeLastError    string         `json:"latencyProbeLastError,omitempty"`
	Resources                resourceReport `json:"resources"`

	// Valid is true only when Iceberg ended equal to MySQL and the oracle
	// agreed with MySQL. The numbers of an invalid run measure a pipeline
	// that lost or invented data.
	Valid    bool     `json:"valid"`
	Problems []string `json:"problems,omitempty"`
}

var benchNameRE = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,30}[a-z0-9])?$`)

const (
	benchLatencyPoll  = 2 * time.Second
	benchResourcePoll = 10 * time.Second
)

func TestBenchmark(t *testing.T) {
	requirePods(t)
	if os.Getenv("URUTAU_BENCH_CONFIG") == "" {
		t.Skip("URUTAU_BENCH_CONFIG not set; skipping the benchmark")
	}
	cfg, err := loadBenchConfig()
	if err != nil {
		t.Fatal(err)
	}
	if !benchNameRE.MatchString(cfg.Name) {
		t.Fatalf("benchmark name %q: want lowercase letters, digits and dashes (it names the pipeline)", cfg.Name)
	}
	profile := cfg.profile()
	seed := cfg.Seed
	if seed == 0 {
		seed = uint64(time.Now().UnixNano())
	}
	t.Logf("benchmark %s: engine=%s seed=%d (replay with \"seed\": %d)", cfg.Name, cfg.Engine, seed, seed)

	ctx, cancel := context.WithTimeout(context.Background(), profile.Duration+profile.Settle+30*time.Minute)
	defer cancel()

	mysql, trino := setupPodEnv(t)
	suffix := strconv.FormatInt(time.Now().UnixNano()%2176782336, 36)
	tables := cfg.shape(productionTables(suffix, profile))
	w := newWorkload(seed, profile, tables, mysql)
	dir, err := runArtifactsDir(profile.Name, seed)
	if err != nil {
		t.Fatalf("artifacts dir: %v", err)
	}

	res := &benchResults{Config: cfg, Seed: seed, Started: time.Now(), LatencyResolutionSeconds: benchLatencyPoll.Seconds()}
	res.Environment = benchEnvironmentOf(ctx, mysql, trino)
	var lat *latencyProbe
	var usage *resourceProbe
	t.Cleanup(func() {
		// Written pass or fail: a failed run's numbers explain the failure.
		if lat != nil {
			res.Latency, res.LatencyProbeErrors, res.LatencyProbeLastError = lat.report()
		}
		if usage != nil {
			res.Resources = usage.report()
		}
		res.Valid = !t.Failed() && len(res.Problems) == 0
		path, err := writeBenchResults(res)
		if err != nil {
			t.Logf("write results: %v", err)
			return
		}
		t.Logf("results: %s (valid=%v)", path, res.Valid)
	})
	problem := func(format string, args ...any) {
		msg := fmt.Sprintf(format, args...)
		res.Problems = append(res.Problems, msg)
		t.Error(msg)
	}

	if err := w.createTables(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("keeping source tables %v for inspection", tableNames(tables))
			return
		}
		w.dropTables()
	})
	seedStart := time.Now()
	if err := w.seedInitial(ctx); err != nil {
		t.Fatal(err)
	}
	res.SeedSeconds = time.Since(seedStart).Seconds()
	t.Logf("seeded %d rows per table in %.0fs", profile.InitialRows, res.SeedSeconds)

	podNS, podPrefix := cfg.PodNamespace, cfg.PodPrefix
	if podNS == "" {
		podNS = testNS
	}
	switch cfg.Engine {
	case "urutau":
		pipeline := "bench-" + cfg.Name
		if podPrefix == "" {
			podPrefix = pipeline + "-"
		}
		image := cfg.Image
		if image == "" {
			image = raceImage()
		}
		specs := make([]tableSpec, len(tables))
		for i, tb := range tables {
			specs[i] = tableSpec{Source: "shop." + tb.Name, Target: "raw." + tb.Target, PrimaryKey: tb.PK, Workers: tb.Workers, Cast: tb.Cast}
		}
		cr := buildCR(pipeline, testNS, image, "pod-e2e-source", "pod-e2e-catalog", "2330", specs, benchCROptions(profile))
		manifest := filepath.Join(dir, "engine-manifest.yaml")
		if err := os.WriteFile(manifest, []byte(cr), 0o644); err != nil {
			t.Fatalf("write manifest: %v", err)
		}
		res.Environment.Image, res.Environment.Pipeline, res.Environment.Manifest = image, pipeline, filepath.Base(manifest)
		applied := time.Now()
		applyPipeline(t, testNS, pipeline, cr)
		logs := newLogCollector(dir, testNS, pipeline)
		if err := logs.start(); err != nil {
			t.Fatalf("log collector: %v", err)
		}
		t.Cleanup(func() {
			logs.stop()
			tctx, tcancel := context.WithTimeout(context.Background(), 5*time.Minute)
			n, err := dumpTrail(tctx, dir, pipeline, applied)
			tcancel()
			t.Logf("run trail: %d coordinator run(s) under %s/trail (err %v)", n, dir, err)
		})
	case "external":
		t.Logf("engine external: start it now, MySQL shop.<table> → Iceberg raw.<table>, for:")
		for _, tb := range tables {
			t.Logf("  shop.%s → raw.%s  primary key %v", tb.Name, tb.Target, tb.PK)
		}
	}

	// Commits before measuredFrom are the snapshot and the first catch-up.
	measuredFrom := time.Now().Add(time.Duration(cfg.Warmup))
	lat = newLatencyProbe(trino, tables, benchLatencyPoll, measuredFrom)
	w.onCommit = lat.onCommit
	// Live mutations start before the engine is up, so they overlap its
	// snapshot as well as its stream, as in production.
	w.start(ctx)
	res.LiveFrom, res.MeasuredFrom = w.liveFrom, measuredFrom
	if cfg.Engine == "urutau" {
		waitPodsByPrefix(t, testNS, "bench-"+cfg.Name+"-coordinator-", 1, 4*time.Minute)
	}
	usage = newResourceProbe(podNS, podPrefix, benchResourcePoll)
	if err := usage.start(ctx); err != nil {
		t.Fatalf("resource probe: %v", err)
	}
	t.Cleanup(usage.stop)
	lat.start(ctx)
	t.Cleanup(func() { lat.stop(context.Background()) })
	t.Logf("workload live for %s (measured after %s)", profile.Duration, time.Duration(cfg.Warmup))

	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-time.After(time.Until(w.liveFrom.Add(profile.Duration))):
	}
	if err := w.stop(ctx); err != nil {
		t.Fatalf("stop workload: %v", err)
	}
	res.LiveTo, res.SourcePosition = w.liveTo, w.finalPos
	t.Logf("workload stopped; source position %s", w.finalPos)

	reconnect := func() error {
		return startPortForward(t, dataNS, "svc/trino", localTrinoPort, 8080, 30*time.Second)
	}
	checks, err := w.settle(ctx, trino, t.Logf, reconnect)
	res.SettleSeconds = time.Since(w.liveTo).Seconds()
	lat.stop(ctx)
	usage.stop()
	if err != nil {
		problem("settle: %v", err)
	}
	for i, c := range checks {
		if !c.OracleVsSource.empty() {
			problem("%s: oracle diverged from MySQL (a workload bug): %s", tables[i].Name, c.OracleVsSource)
		}
		if !c.SourceVsSink.empty() {
			problem("%s: Iceberg != MySQL: %s", tables[i].Name, c.SourceVsSink)
		}
	}
	w.mu.Lock()
	for _, e := range w.errs {
		problem("workload: %s", e)
	}
	w.mu.Unlock()
	if n := lat.pending(); n > 0 {
		problem("latency: %d measured transaction(s) never became visible in the sink", n)
	}

	window := w.liveTo.Sub(w.liveFrom).Seconds()
	for i, g := range w.gens {
		s := w.stats[i]
		th := benchThroughput{
			Table: g.t.Name, Kind: g.t.Kind, Workers: g.t.Workers, Ops: s.Ops, Transactions: s.Txns,
			MutationsPerSecond: s.mutationsPerSecond(measuredFrom, w.liveTo),
			RowsPerTxn:         s.rowsPerTxn.summary(), BytesPerTxn: s.bytesPerTxn.summary(), PayloadBytes: s.payload.summary(),
		}
		if window > 0 {
			th.MeanRowsPerSecond = s.rowsPerTxn.sum / window
			th.MeanPayloadBytesPerSecond = s.bytesPerTxn.sum / window
		}
		rows, bytes, err := sinkCommits(ctx, trino, g.t)
		if err != nil {
			problem("sink commit stats of %s: %v", g.t.Target, err)
		} else {
			th.SinkRowsPerCommit, th.SinkBytesPerCommit = rows.summary(), bytes.summary()
			th.SinkCommits, th.SinkBytesWritten = bytes.n, bytes.sum
		}
		res.Throughput = append(res.Throughput, th)
	}
}

// benchCROptions are the pipeline's resources for a benchmark: the full
// profile's when the load is the full profile's, the harness defaults below
// it.
func benchCROptions(p workloadProfile) crOptions {
	var opts crOptions
	if p.fullLoad() {
		opts.CoordinatorMemory, opts.CoordinatorCPU = "6Gi", "4"
		opts.WorkerMemoryOverhead, opts.WorkerCPUOverhead = "4Gi", "1500m"
	}
	return opts
}

// benchEnvironmentOf records the versions and the nodes of the run. Best
// effort: a value it cannot read is left empty.
func benchEnvironmentOf(ctx context.Context, mysql, trino *sql.DB) benchEnvironment {
	env := benchEnvironment{Revision: os.Getenv("URUTAU_BENCH_REVISION")}
	_ = mysql.QueryRowContext(ctx, "SELECT VERSION()").Scan(&env.MySQL)
	_ = trino.QueryRowContext(ctx, "SELECT version()").Scan(&env.Trino)
	if out, err := kubectlCmd("", "version", "-o", "json"); err == nil {
		var v struct {
			ServerVersion struct {
				GitVersion string `json:"gitVersion"`
			} `json:"serverVersion"`
		}
		if json.Unmarshal([]byte(out), &v) == nil {
			env.Kubernetes = v.ServerVersion.GitVersion
		}
	}
	if out, err := kubectlCmd("", "get", "nodes", "-o", "json"); err == nil {
		var l struct {
			Items []struct {
				Metadata struct {
					Name string `json:"name"`
				} `json:"metadata"`
				Status struct {
					Capacity map[string]string `json:"capacity"`
					NodeInfo struct {
						OSImage       string `json:"osImage"`
						KernelVersion string `json:"kernelVersion"`
						Architecture  string `json:"architecture"`
					} `json:"nodeInfo"`
				} `json:"status"`
			} `json:"items"`
		}
		if json.Unmarshal([]byte(out), &l) == nil {
			for _, n := range l.Items {
				env.Nodes = append(env.Nodes, benchNode{
					Name: n.Metadata.Name, CPU: n.Status.Capacity["cpu"], Memory: n.Status.Capacity["memory"],
					OS: n.Status.NodeInfo.OSImage, Kernel: n.Status.NodeInfo.KernelVersion, Arch: n.Status.NodeInfo.Architecture,
				})
			}
		}
	}
	return env
}

func writeBenchResults(res *benchResults) (string, error) {
	dir := os.Getenv("URUTAU_E2E_ARTIFACTS")
	if dir == "" {
		dir = filepath.Join(os.TempDir(), "urutau-e2e")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	b, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, "benchmark-"+res.Config.Name+"-"+strconv.FormatUint(res.Seed, 10)+".json")
	return path, os.WriteFile(path, b, 0o644)
}

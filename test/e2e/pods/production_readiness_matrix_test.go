package pods

// Issue #386: KEDA scale-out and scale-in, live partition re-slicing,
// Chaos Mesh faults and the production Iceberg maintenance path, all over the
// same live MySQL → Iceberg workload.

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// matrixMaintenance is the sink maintenance block for the matrix run: every
// operation on, on intervals short enough to run many times in the window.
var matrixMaintenance = map[string]any{
	"enabled":        true,
	"compaction":     map[string]any{"interval": "20s", "minInputFiles": 2},
	"snapshotExpiry": map[string]any{"interval": "30s", "retainLast": 3, "maxAge": "1m"},
	// The shortest window validation accepts (spec.MinOrphanCleanupOlderThan).
	"orphanCleanup": map[string]any{"interval": "30s", "olderThan": "1h"},
}

// TestProductionReadinessMatrix is #386. The partitioned tables autoscale
// with KEDA (workers.max 4, baseline 2) on the coordinator's backlog; every
// maintenance operation runs through the coordinator's ephemeral maintenance
// Pods; the chaos controller injects faults throughout, and, every time a
// worker StatefulSet's replica count changes (a re-slice starting), it
// injects a worker pod-kill and a worker <-> coordinator network partition
// right then, so faults overlap partition transitions by construction.
//
// It passes when, besides the workload's exact convergence: KEDA scaled a
// table out during the live window and back in afterwards; each table got a
// compaction snapshot carrying cdc.position; snapshots seen early in the run
// were expired; orphan cleanup ran every 30 s under live commits without
// touching a live file, and left a planted file younger than its window
// alone; and every fault was injected and removed. That cleanup removes a
// real orphan is proven where a file can be backdated
// (TestOrphanCleanupRemovesAKnownOrphanOnly, internal/sink/iceberg); an S3
// object cannot be.
func TestProductionReadinessMatrix(t *testing.T) {
	var m *matrixSampler
	runProductionReadiness(t, prOptions{
		pipeline: "pod-pr-matrix", serverID: "2322", chaos: true,
		kedaMax: 4, maintenance: matrixMaintenance, live: 8 * time.Minute,
		// Eight minutes of load on four workers per table, chaos restarts,
		// and staged tables committing one cycle at a time (#414): the
		// backlog took ~36 minutes to drain in a run that then converged
		// exactly.
		settle: 60 * time.Minute,
		onLive: func(ctx context.Context, r *prRun) {
			m = newMatrixSampler(r)
			if err := m.plantOrphan(ctx); err != nil {
				r.t.Fatalf("plant orphan: %v", err)
			}
			r.t.Logf("planted orphan %s", m.orphan)
			m.start(ctx)
		},
		afterSettle: func(ctx context.Context, r *prRun) {
			m.stop()
			m.check(ctx)
		},
	})
}

// stsSample is one observation of a worker StatefulSet's replica count.
type stsSample struct {
	At       time.Time
	Replicas int
}

// matrixSampler watches the run: worker replicas (KEDA), compaction and
// expiry evidence in each table's snapshots, and reacts to transitions.
type matrixSampler struct {
	r        *prRun
	baseline map[string]int // worker StatefulSet → declared workers.number

	mu          sync.Mutex
	samples     map[string][]stsSample
	transitions int
	compacted   map[string]bool // table → saw a replace snapshot
	compactPos  map[string]bool // table → a replace snapshot carried cdc.position
	early       map[string]map[string]bool
	orphan      string

	stopCh chan struct{}
	wg     sync.WaitGroup
}

func newMatrixSampler(r *prRun) *matrixSampler {
	return &matrixSampler{
		r: r, baseline: map[string]int{}, samples: map[string][]stsSample{},
		compacted: map[string]bool{}, compactPos: map[string]bool{}, early: map[string]map[string]bool{},
	}
}

// s3 runs an aws-cli command against the in-cluster RustFS from a one-shot
// Pod, the same image and credentials the bucket-init Job uses.
func s3(ctx context.Context, script string) (string, error) {
	name := "pr-s3-" + strconv.FormatInt(time.Now().UnixNano()%2176782336, 36)
	return kubectlCmd("", "-n", dataNS, "run", name, "--rm", "-i", "--quiet", "--restart=Never",
		"--image=amazon/aws-cli:latest",
		"--env=AWS_ACCESS_KEY_ID=urutau", "--env=AWS_SECRET_ACCESS_KEY=urutau_dev_secret", "--env=AWS_DEFAULT_REGION=us-east-1",
		"--command", "--", "sh", "-c", script)
}

const s3Endpoint = "--endpoint-url http://rustfs.e2e.svc.cluster.local:9000"

// plantOrphan writes a file no snapshot references into the accounts
// table's data directory. It is younger than olderThan for the whole run, so
// orphan cleanup must leave it, exactly as it must leave a staged file
// waiting for its commit.
func (m *matrixSampler) plantOrphan(ctx context.Context) error {
	var target string
	for _, tb := range m.r.tables {
		if tb.Kind == accountsKind {
			target = tb.Target
		}
	}
	if target == "" {
		return fmt.Errorf("no accounts table in the run")
	}
	// The table exists once the coordinator has booted it.
	deadline := time.Now().Add(3 * time.Minute)
	for {
		var n int
		err := m.r.trino.QueryRowContext(ctx, `SELECT count(*) FROM "`+target+`$snapshots"`).Scan(&n)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("table %s never appeared: %w", target, err)
		}
		time.Sleep(5 * time.Second)
	}
	m.orphan = fmt.Sprintf("s3://warehouse/raw/%s/data/urutau-e2e-orphan-%d.parquet", target, time.Now().UnixNano())
	_, err := s3(ctx, fmt.Sprintf("echo orphan | aws %s s3 cp - %s && aws %s s3 ls %s", s3Endpoint, m.orphan, s3Endpoint, m.orphan))
	return err
}

// orphanPresent reports whether the planted file still exists.
func (m *matrixSampler) orphanPresent(ctx context.Context) (bool, error) {
	out, err := s3(ctx, fmt.Sprintf("aws %s s3 ls %s || true", s3Endpoint, m.orphan))
	if err != nil {
		return false, err
	}
	return strings.Contains(out, "urutau-e2e-orphan-"), nil
}

func (m *matrixSampler) start(ctx context.Context) {
	for _, tb := range m.r.tables {
		if tb.Workers > 1 {
			m.baseline["worker:"+tb.Target] = tb.Workers
		}
	}
	m.stopCh = make(chan struct{})
	m.wg.Add(2)
	go func() {
		defer m.wg.Done()
		m.sampleReplicas(ctx)
	}()
	go func() {
		defer m.wg.Done()
		m.sampleSnapshots(ctx)
	}()
}

func (m *matrixSampler) stop() {
	close(m.stopCh)
	m.wg.Wait()
}

// workerReplicas reads every worker StatefulSet's spec.replicas, keyed by
// the table it serves.
func (m *matrixSampler) workerReplicas() (map[string]int, error) {
	out, err := kubectlCmd("", "-n", testNS, "get", "statefulsets", "-l", "app=urutau-worker,urutau.io/pipeline="+m.r.pipeline, "-o",
		`jsonpath={range .items[*]}{.metadata.labels.urutau\.io/table}{"\t"}{.spec.replicas}{"\n"}{end}`)
	if err != nil {
		return nil, err
	}
	got := map[string]int{}
	for _, line := range strings.Split(out, "\n") {
		f := strings.Split(line, "\t")
		if len(f) == 2 && f[0] != "" {
			n, _ := strconv.Atoi(f[1])
			got["worker:"+strings.TrimPrefix(f[0], "raw.")] = n
		}
	}
	return got, nil
}

// sampleReplicas records replicas every 5 s and, when a count changes,
// injects a worker pod-kill and a network partition at once.
func (m *matrixSampler) sampleReplicas(ctx context.Context) {
	last := map[string]int{}
	for {
		if got, err := m.workerReplicas(); err == nil {
			now := time.Now()
			m.mu.Lock()
			for k, n := range got {
				m.samples[k] = append(m.samples[k], stsSample{At: now, Replicas: n})
			}
			m.mu.Unlock()
			for k, n := range got {
				if prev, ok := last[k]; ok && prev != n && m.r.chaos != nil {
					m.mu.Lock()
					m.transitions++
					react := m.transitions <= 6
					m.mu.Unlock()
					if react {
						trigger := fmt.Sprintf("re-slice %s %d->%d", k, prev, n)
						m.r.chaos.injectNow(ctx, chaosWorkerKill, trigger)
						m.r.chaos.injectNow(ctx, chaosNetworkPartition, trigger)
					}
				}
				last[k] = n
			}
		}
		select {
		case <-m.stopCh:
			return
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}

// snapshotIDs lists a table's current snapshots with their operation and
// whether they carry cdc.position.
func snapshotIDs(ctx context.Context, trino *sql.DB, target string) (map[string]string, map[string]bool, error) {
	rows, err := trino.QueryContext(ctx, `SELECT CAST(snapshot_id AS VARCHAR), operation, element_at(summary, 'cdc.position') IS NOT NULL FROM "`+target+`$snapshots"`)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = rows.Close() }()
	ops, withPos := map[string]string{}, map[string]bool{}
	for rows.Next() {
		var id, op string
		var hasPos bool
		if err := rows.Scan(&id, &op, &hasPos); err != nil {
			return nil, nil, err
		}
		ops[id], withPos[id] = op, hasPos
	}
	return ops, withPos, rows.Err()
}

// sampleSnapshots polls each table's snapshots every 20 s: it notes any
// compaction (a replace snapshot, and whether it carried cdc.position), and
// keeps the snapshot set seen 90 s into the window, which expiry must have
// pruned by the end. Compaction is noted as it happens because expiry may
// later remove the replace snapshot itself.
func (m *matrixSampler) sampleSnapshots(ctx context.Context) {
	earlyAt := time.Now().Add(90 * time.Second)
	for {
		for _, tb := range m.r.tables {
			ops, withPos, err := snapshotIDs(ctx, m.r.trino, tb.Target)
			if err != nil {
				continue // the table may not exist yet, or Trino may be busy
			}
			m.mu.Lock()
			for id, op := range ops {
				if op == "replace" {
					m.compacted[tb.Target] = true
					if withPos[id] {
						m.compactPos[tb.Target] = true
					}
				}
			}
			if _, done := m.early[tb.Target]; !done && time.Now().After(earlyAt) && len(ops) > 0 {
				ids := map[string]bool{}
				for id := range ops {
					ids[id] = true
				}
				m.early[tb.Target] = ids
			}
			m.mu.Unlock()
		}
		select {
		case <-m.stopCh:
			return
		case <-ctx.Done():
			return
		case <-time.After(20 * time.Second):
		}
	}
}

// check asserts the #386 criteria once the run has converged.
func (m *matrixSampler) check(ctx context.Context) {
	t := m.r.t
	m.mu.Lock()
	samples := m.samples
	m.mu.Unlock()

	liveTo := m.r.w.liveTo
	scaledOut := false
	for k, base := range m.baseline {
		peak := 0
		for _, s := range samples[k] {
			if s.At.Before(liveTo) && s.Replicas > base {
				scaledOut = true
			}
			peak = max(peak, s.Replicas)
		}
		t.Logf("KEDA %s: baseline %d, peak %d, %d samples", k, base, peak, len(samples[k]))
	}
	if !scaledOut {
		t.Error("matrix: KEDA never scaled a table out during the live window")
	}

	// Scale-in follows the HPA's stabilization window once the backlog is
	// gone; the workload has stopped and the tables have converged.
	deadline := time.Now().Add(12 * time.Minute)
	for {
		got, err := m.workerReplicas()
		back := err == nil
		for k, base := range m.baseline {
			back = back && got[k] == base
		}
		if back {
			t.Log("KEDA scaled every table back in to its baseline")
			break
		}
		if time.Now().After(deadline) {
			t.Errorf("matrix: KEDA did not scale back in to the baseline within 12m (replicas %v, baseline %v, err %v)", got, m.baseline, err)
			break
		}
		time.Sleep(10 * time.Second)
	}

	// A re-slice during the live window moved partition ownership under
	// live CDC; the reactive faults overlapped it.
	reactive := 0
	if m.r.chaos != nil {
		for _, ev := range m.r.chaos.report().Events {
			if strings.HasPrefix(ev.Trigger, "re-slice") && ev.Injected {
				reactive++
			}
		}
	}
	if reactive == 0 {
		t.Error("matrix: no fault was injected during a partition transition")
	}

	m.mu.Lock()
	for _, tb := range m.r.tables {
		if !m.compacted[tb.Target] {
			t.Errorf("matrix: %s was never compacted (no replace snapshot seen)", tb.Target)
		} else if !m.compactPos[tb.Target] {
			t.Errorf("matrix: %s compaction snapshots did not carry cdc.position", tb.Target)
		}
	}
	early := m.early
	m.mu.Unlock()

	for _, tb := range m.r.tables {
		ids, ok := early[tb.Target]
		if !ok {
			t.Errorf("matrix: %s: no early snapshot set recorded", tb.Target)
			continue
		}
		now, _, err := snapshotIDs(ctx, m.r.trino, tb.Target)
		if err != nil {
			t.Errorf("matrix: %s snapshots: %v", tb.Target, err)
			continue
		}
		expired := 0
		for id := range ids {
			if _, still := now[id]; !still {
				expired++
			}
		}
		if expired == 0 {
			t.Errorf("matrix: %s: none of the %d snapshots seen early was expired", tb.Target, len(ids))
		} else {
			t.Logf("%s: %d of %d early snapshots expired", tb.Target, expired, len(ids))
		}
	}

	present, err := m.orphanPresent(ctx)
	switch {
	case err != nil:
		t.Errorf("matrix: orphan check: %v", err)
	case !present:
		t.Errorf("matrix: %s, younger than olderThan, was deleted by orphan cleanup", m.orphan)
	default:
		t.Logf("%s, younger than olderThan, survived orphan cleanup; the tables still equal MySQL", m.orphan)
	}
}

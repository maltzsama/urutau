package pods

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	// Registers the "clickhouse" database/sql driver on this stdlib pool,
	// exactly as test/e2e/clickhouse_test.go does. The pipeline reaches the
	// same server over the native protocol via the sink driver; the harness's
	// read side uses this stdlib client so the oracle is independent of the
	// sink's own code path.
	_ "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/maltzsama/urutau/internal/faultinject"
)

// ClickHouse is the analytics sink the commit-boundary matrix writes to. It
// has no staged cycle and no multi-statement transaction: each partition
// owner commits its own sub-batches directly, and the batch's position
// travels on every row of its one INSERT. The direct-path boundaries apply;
// the staged ones do not (the worker sink driver does not implement
// sink.StagingWriter), so only the direct set plus the sink's own window is
// exercised here.

const (
	// localClickHousePort is the fixed local port for the ClickHouse native
	// port-forward. 19000 is clear of the other forwards (mysql 13306, trino
	// 18080, redpanda 19092, coordinator metrics 19091).
	localClickHousePort = 19000
	// clickhouseCatalogSecret holds the ClickHouse DSN. It is a separate
	// Secret from the Iceberg one: the operator mounts whichever the CR
	// references as URUTAU_SINK_URI.
	clickhouseCatalogSecret = "pod-e2e-catalog-ch"
)

// clickhouseSourceDSN is the in-cluster DSN the pipeline (coordinator and
// workers) uses to reach the ClickHouse Service on the native protocol.
const clickhouseSourceDSN = "clickhouse://clickhouse.e2e.svc.cluster.local:9000?password=clickpass"

// chiQuerier is the read side of a ClickHouse client: enough to fold the
// versioned data table to the oracle shape and to read a committed position.
// *sql.DB satisfies it.
type chiQuerier interface {
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

// openClickHouse dials the in-cluster ClickHouse through a port-forward.
func openClickHouse(t *testing.T, port int) *sql.DB {
	t.Helper()
	db, err := sql.Open("clickhouse", fmt.Sprintf("clickhouse://127.0.0.1:%d?password=clickpass", port))
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Ping(); err != nil {
		t.Fatalf("ping clickhouse: %v", err)
	}
	return db
}

// setupPodEnvClickHouse is setupPodEnv for a MySQL→ClickHouse pipeline: the
// source Secret, the ClickHouse catalog Secret, a port-forward to the
// in-cluster ClickHouse Service, and an open ClickHouse client. A missing
// ClickHouse Service fails the port-forward wait, so the scenario fails setup
// rather than silently skipping.
func setupPodEnvClickHouse(t *testing.T) (source *sql.DB, ch chiQuerier) {
	t.Helper()
	ensureNamespace(t, testNS)
	ensureSecret(t, testNS, "pod-e2e-source", map[string]string{
		"uri": "mysql://repl:replpass@mysql.e2e.svc.cluster.local:3306/shop",
	})
	ensureSecret(t, testNS, clickhouseCatalogSecret, map[string]string{
		"uri": clickhouseSourceDSN,
	})
	ensureTrailSecret(t)
	portForward(t, dataNS, "svc/mysql", localMySQLPort, 3306)
	portForward(t, dataNS, "svc/clickhouse", localClickHousePort, 9000)
	return openMySQL(t, localMySQLPort), openClickHouse(t, localClickHousePort)
}

// clickHouseTable is the physical identifier of a sink table: the sink's
// namespace (raw) plus the target name, both backtick-quoted so a name with a
// dot or keyword is still one identifier.
func clickHouseTable(target string) string {
	return "`raw`.`" + target + "`"
}

// readClickHouseSink folds the sink's versioned rows to the oracle shape
// (id → state): the latest version per primary key, dropped when that version
// is a delete tombstone. ClickHouse stores every commit as rows with a
// strictly increasing `seq`, and a delete as a tombstone (is_deleted=1, key
// set, other columns zero); ReplacingMergeTree FINAL would collapse them, but
// folding on seq here is the same decision without depending on the merge
// clock, and it is the exact kernel the commit-boundary contract protects.
func readClickHouseSink(t *testing.T, ch chiQuerier, target string) map[int64]rowState {
	t.Helper()
	q := "SELECT id, v, amount, seq, is_deleted FROM " + clickHouseTable(target)
	rows, err := ch.Query(q)
	if err != nil {
		t.Fatalf("read clickhouse %s: %v", target, err)
	}
	defer func() { _ = rows.Close() }()
	type versioned struct {
		state   rowState
		seq     uint64
		deleted bool
	}
	latest := map[int64]versioned{}
	for rows.Next() {
		var id int64
		var st rowState
		var seq uint64
		var deleted uint8
		if err := rows.Scan(&id, &st.V, &st.Amount, &seq, &deleted); err != nil {
			t.Fatalf("scan clickhouse %s: %v", target, err)
		}
		if cur, ok := latest[id]; !ok || seq > cur.seq {
			latest[id] = versioned{state: st, seq: seq, deleted: deleted == 1}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows clickhouse %s: %v", target, err)
	}
	out := make(map[int64]rowState, len(latest))
	for id, v := range latest {
		if !v.deleted {
			out[id] = v.state
		}
	}
	return out
}

// waitClickHouseSettled polls until the ClickHouse sink's folded state equals
// the source's exactly, or fails with the residual diff. It is the
// ClickHouse-sink sibling of waitSettledTables (whose poll reads Trino).
func waitClickHouseSettled(t *testing.T, source *sql.DB, ch chiQuerier, target string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		src := readOrders(t, source)
		sink := readClickHouseSink(t, ch, target)
		missing, extra, wrong := diffSinkSource(src, sink)
		if len(missing) == 0 && len(extra) == 0 && len(wrong) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("clickhouse sink %s never settled to source: source=%d sink=%d missing=%v extra=%v wrongValue=%v",
				target, len(src), len(sink), cap10(missing), cap10(extra), cap10(wrong))
		}
		time.Sleep(time.Second)
	}
}

// clickhouseBoundaries is the ClickHouse sink's deterministic boundary set.
// ClickHouse has no staged cycle (each partition owner commits its own
// sub-batches directly — see the sink's writer), so the staged boundaries do
// not apply. The direct-path boundaries do, plus the sink-specific window
// between the data INSERT and the per-partition control-table position write.
var clickhouseBoundaries = []boundaryCase{
	{point: faultinject.WorkerBatchReceived},
	{point: faultinject.WorkerCommitBefore},
	{point: faultinject.WorkerClickHouseDataBeforePosition},
	{point: faultinject.WorkerCommittedBeforeAck},
	{point: faultinject.CoordinatorAckBeforeRecord, coordinator: true},
}

// startClickHouseBoundaryPipeline starts a MySQL→ClickHouse pipeline over
// shop.orders with workers owners and waits until it has converged; it
// returns the sink target name.
func startClickHouseBoundaryPipeline(t *testing.T, source *sql.DB, ch chiQuerier, pipeline, serverID, base string, workers int) string {
	t.Helper()
	target := uniqueTarget(base)
	cr := buildCR(pipeline, testNS, raceImage(), "pod-e2e-source", clickhouseCatalogSecret, serverID,
		[]tableSpec{{Source: "shop.orders", Target: "raw." + target, PrimaryKey: []string{"id"}, Workers: workers}},
		crOptions{SinkType: "clickhouse"})
	applyPipeline(t, testNS, pipeline, cr)
	waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 5*time.Minute)
	waitPodsByPrefix(t, testNS, workerSTSs(t, testNS, pipeline)[0]+"-", workers, 5*time.Minute)
	waitClickHouseSettled(t, source, ch, target, 5*time.Minute)
	return target
}

// runClickHouseBoundaryCases drives one MySQL→ClickHouse pipeline through
// each case in turn, exactly as runBoundaryCases does for Iceberg: arm the
// point, write load until it fires (the Pod restarts), then stop the load and
// require the ClickHouse sink to converge to the source exactly.
func runClickHouseBoundaryCases(t *testing.T, source *sql.DB, ch chiQuerier, pipeline, target string, workers int, cases []boundaryCase) {
	t.Helper()
	for _, bc := range cases {
		t.Run(string(bc.point), func(t *testing.T) {
			coord := waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 8*time.Minute)[0]
			stss := workerSTSs(t, testNS, pipeline)
			// The first ordinal: on a non-staged sink the single owner (or
			// each partition owner) reaches the worker-side boundaries, so
			// any one of them exercises the point.
			pod := waitPodsByPrefix(t, testNS, stss[0]+"-", workers, 8*time.Minute)[0]
			if bc.coordinator {
				pod = coord
			}

			before := podRestarts(t, testNS, pod)
			logs := followLogs(t, testNS, pod)
			armFault(t, testNS, pod, bc.point, "raw."+target)
			reportClickHouseBoundaryOnFailure(t, source, ch, pipeline, bc.point, target)
			stop := startWriter(t, source, 250*time.Millisecond)
			line := waitFaultFired(t, testNS, pod, logs, bc.point, before, 4*time.Minute)
			stop()
			t.Logf("fired: %s", strings.TrimSpace(line))
			if !strings.Contains(line, "table=raw."+target) {
				t.Fatalf("diagnostic line does not name the table: %q", line)
			}

			waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 8*time.Minute)
			waitPodsByPrefix(t, testNS, stss[0]+"-", workers, 8*time.Minute)
			waitClickHouseSettled(t, source, ch, target, 10*time.Minute)
			assertSinkEqualsSource(t, readOrders(t, source), readClickHouseSink(t, ch, target))
		})
	}
}

// reportClickHouseBoundaryOnFailure registers, for a failed ClickHouse
// boundary case, the boundary, the source's executed position and the sink's
// committed position, logged; and the cluster state dumped under
// URUTAU_E2E_ARTIFACTS. The ClickHouse sink has no Iceberg snapshot history,
// so only the cluster objects are dumped (dumpDiagnostics with a nil Trino).
func reportClickHouseBoundaryOnFailure(t *testing.T, source *sql.DB, ch chiQuerier, pipeline string, point faultinject.Point, target string) {
	t.Helper()
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		var executed string
		if err := source.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&executed); err != nil {
			executed = "error: " + err.Error()
		}
		t.Logf("boundary %s failed: source gtid_executed=%s", point, executed)
		var pos sql.NullString
		if err := ch.QueryRow("SELECT argMax(position, seq) FROM " + clickHouseTable(target)).Scan(&pos); err != nil {
			t.Logf("boundary %s: clickhouse sink %s position read: %v", point, target, err)
		} else {
			t.Logf("boundary %s: clickhouse sink %s committed position=%q", point, target, pos.String)
		}
		base := os.Getenv("URUTAU_E2E_ARTIFACTS")
		if base == "" {
			base = filepath.Join(os.TempDir(), "urutau-e2e")
		}
		dir := filepath.Join(base, fmt.Sprintf("commit-boundary-ch-%s-%d", point, time.Now().Unix()))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Logf("artifacts: %v", err)
			return
		}
		dumpDiagnostics(ctx, dir, testNS, pipeline, nil, nil)
		t.Logf("boundary %s: diagnostics under %s", point, dir)
	})
}

// TestCommitBoundaryClickHouse fires every direct-path boundary of a
// MySQL→ClickHouse pipeline — the ClickHouse sink has no staged cycle — and
// checks the pipeline recovers to an exact ClickHouse sink. It is the
// ClickHouse half of the deterministic crash-recovery matrix.
func TestCommitBoundaryClickHouse(t *testing.T) {
	requirePods(t)
	source, ch := setupPodEnvClickHouse(t)
	const pipeline = "pod-fault-ch"
	seedOrders(t, source, 50)
	target := startClickHouseBoundaryPipeline(t, source, ch, pipeline, "2401", "pod_fault_ch", 1)
	runClickHouseBoundaryCases(t, source, ch, pipeline, target, 1, clickhouseBoundaries)
	assertNoRaces(t, testNS, pipeline+"-")
}

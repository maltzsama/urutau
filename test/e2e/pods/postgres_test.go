package pods

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	// Registers the "pgx" database/sql driver on this stdlib pool, exactly as
	// test/e2e/postgres*_test.go do. The pipeline reaches the same server over
	// pgx's own connection; the harness's read side uses this stdlib client so
	// the oracle is independent of the source's own code path.
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/maltzsama/urutau/internal/faultinject"
	"github.com/maltzsama/urutau/internal/source/postgres"
	"github.com/maltzsama/urutau/position"
)

// PostgreSQL is the logical-replication source the commit-boundary matrix
// reads from. It is not a sink: the invariant under test is source-side — the
// logical replication slot's confirmed_flush_lsn must never pass the sink's
// committed position. The pipeline is PostgreSQL → Iceberg (the staging sink),
// so the direct and staged paths both apply; on top of them sits the one
// source-side window the sink points cannot name: after the sink commit is
// durable and before the reader reports it back to the server (advancing the
// slot).

const (
	// localPostgresPort is the fixed local port for the Postgres port-forward.
	// 15432 is clear of the other forwards (mysql 13306, trino 18080,
	// redpanda 19092, coordinator metrics 19091, clickhouse 19000, couchbase
	// 8091/11210).
	localPostgresPort = 15432
)

// postgresSourceDSN is the in-cluster DSN the pipeline (coordinator and
// workers) uses to reach the PostgreSQL Service. This is the exact
// "postgres://user:pass@host:port/db?sslmode=disable" shape the compose e2e
// uses, with the Service DNS name substituted for the host.
const postgresSourceDSN = "postgres://repl:replpass@postgres.e2e.svc.cluster.local:5432/shop?sslmode=disable"

// openPostgres dials the in-cluster Postgres through a port-forward. The pgx
// stdlib driver takes $n placeholders, so the harness's write helpers below
// use those (the MySQL-shaped "?" helpers must not be used here).
func openPostgres(t *testing.T, port int) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", fmt.Sprintf("postgres://repl:replpass@127.0.0.1:%d/shop?sslmode=disable", port))
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Ping(); err != nil {
		t.Fatalf("ping postgres: %v", err)
	}
	return db
}

// setupPodEnvPostgres is setupPodEnv for a PostgreSQL→Iceberg pipeline: the
// source Secret carrying the Postgres DSN, the Iceberg catalog Secret, a
// port-forward to the in-cluster Postgres Service (and Trino for the sink
// oracle), and open drivers. A missing Postgres Service fails the
// port-forward wait, so the scenario fails setup rather than silently
// skipping.
func setupPodEnvPostgres(t *testing.T) (source, trino *sql.DB) {
	t.Helper()
	ensureNamespace(t, testNS)
	ensureSecret(t, testNS, "pod-e2e-source", map[string]string{
		"uri": postgresSourceDSN,
	})
	ensureSecret(t, testNS, "pod-e2e-catalog", map[string]string{
		"uri":          "http://polaris.e2e.svc.cluster.local:8181/api/catalog",
		"clientId":     "root",
		"clientSecret": "s3cr3t",
		"scope":        "PRINCIPAL_ROLE:ALL",
	})
	ensureTrailSecret(t)
	portForward(t, dataNS, "svc/postgres", localPostgresPort, 5432)
	portForward(t, dataNS, "svc/trino", localTrinoPort, 8080)
	return openPostgres(t, localPostgresPort), openTrino(t, localTrinoPort)
}

// readPostgresOrders reads (id → state) from shop.orders in PostgreSQL. It is
// the Postgres-source sibling of readOrders (the same projection the oracle
// uses for every source, so the sink comparison is source-agnostic).
func readPostgresOrders(t *testing.T, db *sql.DB) map[int64]rowState {
	t.Helper()
	return readOrders(t, db)
}

// seedPostgresOrders clears shop.orders and inserts count rows 0..count-1
// with a deterministic v/amount, using $n placeholders (pgx). The oracle is
// fully known before the engine runs.
func seedPostgresOrders(t *testing.T, db *sql.DB, count int) {
	t.Helper()
	if _, err := db.Exec("DELETE FROM orders"); err != nil {
		t.Fatalf("clear orders: %v", err)
	}
	for i := 0; i < count; i++ {
		if _, err := db.Exec("INSERT INTO orders (id, v, amount) VALUES ($1, $2, $3)",
			i, fmt.Sprintf("seed%d", i), float64(i)); err != nil {
			t.Fatalf("seed orders %d: %v", i, err)
		}
	}
}

// startPostgresWriter runs a mixed INSERT/UPDATE/DELETE load against
// shop.orders until the returned stop function is called. It mirrors
// startWriterOn, but with $n placeholders — pgx does not translate "?". The
// source is the oracle, so the exact operations do not need to be tracked.
func startPostgresWriter(t *testing.T, db *sql.DB, table string, interval time.Duration) (stop func()) {
	t.Helper()
	run := writerRuns.Add(1) - 1
	stopCh := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; ; i++ {
			select {
			case <-stopCh:
				return
			default:
			}
			var q string
			var args []any
			switch i % 3 {
			case 0: // monotonic insert: extends the key range
				id := 1000 + run*100000 + int64(i/3)
				q, args = "INSERT INTO "+table+" (id, v, amount) VALUES ($1, $2, $3)",
					[]any{id, fmt.Sprintf("ins%d-%d", run, i), float64(id)}
			case 1: // update an existing key
				id := int64((i / 3) % 200)
				q, args = "UPDATE "+table+" SET v = $1 WHERE id = $2",
					[]any{fmt.Sprintf("upd%d-%d", run, i), id}
			case 2: // delete a key, which must not be resurrected
				id := int64(100 + (i/3)%50)
				q, args = "DELETE FROM "+table+" WHERE id = $1", []any{id}
			}
			if _, err := db.Exec(q, args...); err != nil {
				t.Logf("writer %q: %v", q, err)
			}
			time.Sleep(interval)
		}
	}()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			close(stopCh)
			<-done
		})
	}
	t.Cleanup(stop)
	return stop
}

// dropPostgresSlot drops a logical replication slot if it exists, so slots do
// not accumulate across runs and exhaust max_replication_slots.
func dropPostgresSlot(t *testing.T, db *sql.DB, slot string) {
	t.Helper()
	if _, err := db.Exec(
		`SELECT pg_catalog.pg_drop_replication_slot($1) WHERE EXISTS
		 (SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = $1)`, slot); err != nil {
		t.Logf("drop slot %s: %v", slot, err)
	}
}

// postgresBoundaries is the shared source-side boundary every Postgres
// pipeline adds: the coordinator reports the sink's committed position back to
// the server (a standby status update) only after the sink commit is durable,
// so a crash in that window must leave the slot's confirmed_flush_lsn at or
// behind the sink's committed position, never ahead of it.
var postgresBoundaries = []boundaryCase{
	{point: faultinject.PostgresSlotConfirmBefore, coordinator: true},
}

// postgresDirectBoundaries is the direct-path set (one owner) plus the
// source-side slot-confirm window. PostgreSQL → Iceberg commits directly when
// a table has a single owner.
var postgresDirectBoundaries = append(append([]boundaryCase{}, directBoundaries...), postgresBoundaries...)

// postgresStagedBoundaries is the staged-path set (multiple owners on the
// staging sink) plus the same source-side window.
var postgresStagedBoundaries = append(append([]boundaryCase{}, stagedBoundaries...), postgresBoundaries...)

// startPostgresBoundaryPipeline starts a PostgreSQL→Iceberg pipeline over
// public.orders with workers owners and waits until it has converged; it
// returns the sink target name.
func startPostgresBoundaryPipeline(t *testing.T, source, trino *sql.DB, pipeline, slot, base string, workers int) string {
	t.Helper()
	target := uniqueTarget(base)
	cr := buildCR(pipeline, testNS, raceImage(), "pod-e2e-source", "pod-e2e-catalog", "",
		[]tableSpec{{Source: "public.orders", Target: "raw." + target, PrimaryKey: []string{"id"}, Workers: workers}},
		crOptions{SourceKind: "postgres", SlotName: slot})
	applyPipeline(t, testNS, pipeline, cr)
	waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 5*time.Minute)
	waitPodsByPrefix(t, testNS, workerSTSs(t, testNS, pipeline)[0]+"-", workers, 5*time.Minute)
	waitSettled(t, source, trino, target, 5*time.Minute)
	return target
}

// runPostgresBoundaryCases drives one PostgreSQL→Iceberg pipeline through each
// case in turn, exactly as runBoundaryCases does for MySQL: arm the point,
// write load until it fires (the Pod restarts), then stop the load and require
// the sink to converge to the source exactly. After every recovery it also
// asserts the source-side invariant: the slot's confirmed_flush_lsn never
// passes the sink's committed position.
func runPostgresBoundaryCases(t *testing.T, source, trino *sql.DB, pipeline, slot, target string, workers int, cases []boundaryCase) {
	t.Helper()
	for _, bc := range cases {
		t.Run(string(bc.point), func(t *testing.T) {
			coord := waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 8*time.Minute)[0]
			stss := workerSTSs(t, testNS, pipeline)
			pod := waitPodsByPrefix(t, testNS, stss[0]+"-", workers, 8*time.Minute)[0]
			if bc.coordinator {
				pod = coord
			}

			// The reader confirms the sink's global confirmed position; it has
			// no per-table context, so the slot point is armed without a table
			// filter. Every other point is a per-table commit boundary.
			armTable := "raw." + target
			if bc.point == faultinject.PostgresSlotConfirmBefore {
				armTable = ""
			}

			before := podRestarts(t, testNS, pod)
			logs := followLogs(t, testNS, pod)
			armFault(t, testNS, pod, bc.point, armTable)
			reportPostgresBoundaryOnFailure(t, source, trino, pipeline, slot, bc.point, target)
			stop := startPostgresWriter(t, source, "orders", 250*time.Millisecond)
			line := waitFaultFired(t, testNS, pod, logs, bc.point, before, 4*time.Minute)
			stop()
			t.Logf("fired: %s", strings.TrimSpace(line))
			// The slot point names the slot and LSN instead of a table; the
			// per-table points must name the target table.
			if bc.point != faultinject.PostgresSlotConfirmBefore && !strings.Contains(line, "table=raw."+target) {
				t.Fatalf("diagnostic line does not name the table: %q", line)
			}

			waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 8*time.Minute)
			waitPodsByPrefix(t, testNS, stss[0]+"-", workers, 8*time.Minute)
			waitSettled(t, source, trino, target, 10*time.Minute)
			assertSinkEqualsSource(t, readPostgresOrders(t, source), readOrdersSink(t, trino, target))
			assertSlotNotPastSink(t, source, trino, slot, target)
		})
	}
}

// assertSlotNotPastSink is the source-side invariant of the PostgreSQL half of
// the matrix: after recovery the logical replication slot's confirmed_flush_lsn
// must be at or behind the sink's committed cdc.position. A slot ahead of the
// sink would let the server recycle WAL for events the sink never committed —
// data loss on the next resume.
func assertSlotNotPastSink(t *testing.T, source, trino *sql.DB, slot, target string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	confirmed, err := postgres.ConfirmedLSN(ctx, source, slot)
	if err != nil {
		t.Fatalf("read slot %s confirmed_flush_lsn: %v", slot, err)
	}
	posStr, err := committedPosition(ctx, trino, target)
	if err != nil {
		t.Fatalf("read sink %s cdc.position: %v", target, err)
	}
	if posStr == "" {
		t.Fatalf("sink %s has no committed cdc.position to compare slot %s against", target, slot)
	}
	sink, err := position.ParseLSN(posStr)
	if err != nil {
		t.Fatalf("parse sink %s cdc.position %q: %v", target, posStr, err)
	}
	if confirmed.Compare(sink) > 0 {
		t.Fatalf("slot %s confirmed_flush_lsn %s is ahead of the sink %s committed position %s — the slot passed the committed position",
			slot, confirmed.String(), target, sink.String())
	}
	t.Logf("slot %s confirmed=%s <= sink %s committed=%s", slot, confirmed.String(), target, sink.String())
}

// reportPostgresBoundaryOnFailure registers, for a failed boundary case, the
// boundary, the slot's confirmed_flush_lsn and the sink's committed position,
// logged; and the cluster state dumped under URUTAU_E2E_ARTIFACTS.
func reportPostgresBoundaryOnFailure(t *testing.T, source, trino *sql.DB, pipeline, slot string, point faultinject.Point, target string) {
	t.Helper()
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		if lsn, err := postgres.ConfirmedLSN(ctx, source, slot); err != nil {
			t.Logf("boundary %s: slot %s confirmed_flush_lsn read: %v", point, slot, err)
		} else {
			t.Logf("boundary %s: slot %s confirmed_flush_lsn=%s", point, slot, lsn.String())
		}
		if pos, err := committedPosition(ctx, trino, target); err != nil {
			t.Logf("boundary %s: sink %s cdc.position read: %v", point, target, err)
		} else {
			t.Logf("boundary %s: sink %s cdc.position=%q", point, target, pos)
		}
		base := os.Getenv("URUTAU_E2E_ARTIFACTS")
		if base == "" {
			base = filepath.Join(os.TempDir(), "urutau-e2e")
		}
		dir := filepath.Join(base, fmt.Sprintf("commit-boundary-pg-%s-%d", point, time.Now().Unix()))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Logf("artifacts: %v", err)
			return
		}
		dumpDiagnostics(ctx, dir, testNS, pipeline, trino, []*prTable{{Name: target, Target: target}})
		t.Logf("boundary %s: diagnostics under %s", point, dir)
	})
}

// TestCommitBoundaryPostgresSource fires the deterministic commit-boundary
// matrix against a PostgreSQL→Iceberg pipeline on both commit paths and checks
// that (a) the sink recovers to the exact source state and (b) the logical
// replication slot's confirmed_flush_lsn never passes the sink's committed
// position. A missing Postgres service fails setup, never skips.
func TestCommitBoundaryPostgresSource(t *testing.T) {
	requirePods(t)

	t.Run("direct", func(t *testing.T) {
		source, trino := setupPodEnvPostgres(t)
		const pipeline = "pod-fault-pg-direct"
		const slot = "urutau_pg_direct"
		seedPostgresOrders(t, source, 50)
		t.Cleanup(func() { dropPostgresSlot(t, source, slot) })
		target := startPostgresBoundaryPipeline(t, source, trino, pipeline, slot, "pod_fault_pg_direct", 1)
		runPostgresBoundaryCases(t, source, trino, pipeline, slot, target, 1, postgresDirectBoundaries)
		assertNoRaces(t, testNS, pipeline+"-")
	})

	t.Run("staged", func(t *testing.T) {
		source, trino := setupPodEnvPostgres(t)
		const pipeline = "pod-fault-pg-staged"
		const slot = "urutau_pg_staged"
		seedPostgresOrders(t, source, 50)
		t.Cleanup(func() { dropPostgresSlot(t, source, slot) })
		target := startPostgresBoundaryPipeline(t, source, trino, pipeline, slot, "pod_fault_pg_staged", 2)
		runPostgresBoundaryCases(t, source, trino, pipeline, slot, target, 2, postgresStagedBoundaries)
		assertNoRaces(t, testNS, pipeline+"-")
	})
}

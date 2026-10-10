package pods

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gocb "github.com/couchbase/gocb/v2"

	"github.com/maltzsama/urutau/internal/faultinject"
)

// Couchbase is the key-document sink the commit-boundary matrix writes to. It
// is not a staging sink (it does not implement sink.StagingWriter), so BOTH of
// its commit modes take the direct path: the worker commits each sub-batch
// itself and there is no coordinator cycle. Its atomic mode only wraps that
// single commit — data documents plus the control document — in one gocb
// transaction, so the direct worker/coordinator boundaries apply to both
// modes; fast mode adds one internal window between the data writes and the
// control write.

const (
	// The compose note ("host ports are 1:1") matters here: gocb's
	// couchbase:// scheme derives the management port from the seed host, so
	// the harness forwards 8091 and 11210 at their own numbers. Discovery then
	// follows the node's advertised in-cluster name (the Service DNS), which
	// resolves for the in-cluster harness and the pipeline alike.
	localCouchbaseMgmtPort = 8091
	localCouchbaseKVPort   = 11210

	// couchbaseCatalogSecret holds the SDK connection truth. It is a separate
	// Secret from the Iceberg/ClickHouse ones: the operator mounts whichever
	// the CR references, and Couchbase needs credentials alongside the URI.
	couchbaseCatalogSecret = "pod-e2e-catalog-cb"

	couchbaseBucket = "lakehouse"
	couchbaseScope  = "raw"
	couchbaseUser   = "urutau"
	couchbasePass   = "urutaupass"
	// couchbaseControlKey is the sink's reserved control document; data keys
	// are JSON arrays ("[1]") so they never collide with this "_" prefix.
	couchbaseControlKey = "_urutau::position"

	couchbaseReadyTimeout = 90 * time.Second
)

// couchbaseSourceDSN is the in-cluster DSN the pipeline (coordinator and
// workers) uses to reach the Couchbase Service. No port: gocb derives the
// management (8091) and KV (11210) ports from the couchbase:// scheme and
// follows the node's advertised hostname, which the setup Job sets to this
// same Service DNS name.
const couchbaseSourceDSN = "couchbase://couchbase.e2e.svc.cluster.local"

// couchbaseReader is the read side of a Couchbase client: a bucket handle the
// oracle scan reads through. It is a second gocb client, independent of the
// pipeline's, so the write path is never trusted on its own report.
type couchbaseReader struct {
	bucket *gocb.Bucket
}

var (
	couchbaseMgmtReady = &gocb.WaitUntilReadyOptions{
		DesiredState: gocb.ClusterStateOnline,
		ServiceTypes: []gocb.ServiceType{gocb.ServiceTypeManagement},
	}
	couchbaseKVReady = &gocb.WaitUntilReadyOptions{
		DesiredState: gocb.ClusterStateOnline,
		ServiceTypes: []gocb.ServiceType{gocb.ServiceTypeKeyValue},
	}
)

// openCouchbase dials the in-cluster Couchbase through the 1:1 port-forwards,
// then waits on management and KV — the same readiness contract the sink
// declares. A wrong bucket or credential fails the wait, so a misconfigured
// cluster fails setup rather than silently skipping.
func openCouchbase(t *testing.T) *couchbaseReader {
	t.Helper()
	cluster, err := gocb.Connect("couchbase://127.0.0.1", gocb.ClusterOptions{
		Username: couchbaseUser,
		Password: couchbasePass,
	})
	if err != nil {
		t.Fatalf("connect couchbase: %v", err)
	}
	t.Cleanup(func() { _ = cluster.Close(nil) })
	if err := cluster.WaitUntilReady(couchbaseReadyTimeout, couchbaseMgmtReady); err != nil {
		t.Fatalf("couchbase cluster not ready: %v", err)
	}
	b := cluster.Bucket(couchbaseBucket)
	if err := b.WaitUntilReady(couchbaseReadyTimeout, couchbaseKVReady); err != nil {
		t.Fatalf("couchbase bucket %q not ready: %v", couchbaseBucket, err)
	}
	return &couchbaseReader{bucket: b}
}

// setupPodEnvCouchbase is setupPodEnv for a MySQL→Couchbase pipeline: the
// source Secret, the Couchbase catalog Secret (URI + cluster credentials), the
// 1:1 Couchbase port-forwards, and an open reader. A missing Couchbase Service
// fails the port-forward wait, and a wrong bucket/credential fails the
// readiness wait — setup fails, never silently skips.
func setupPodEnvCouchbase(t *testing.T) (source *sql.DB, cb *couchbaseReader) {
	t.Helper()
	ensureNamespace(t, testNS)
	ensureSecret(t, testNS, "pod-e2e-source", map[string]string{
		"uri": "mysql://repl:replpass@mysql.e2e.svc.cluster.local:3306/shop",
	})
	ensureSecret(t, testNS, couchbaseCatalogSecret, map[string]string{
		"uri":          couchbaseSourceDSN,
		"clientId":     couchbaseUser,
		"clientSecret": couchbasePass,
	})
	ensureTrailSecret(t)
	portForward(t, dataNS, "svc/mysql", localMySQLPort, 3306)
	portForward(t, dataNS, "svc/couchbase", localCouchbaseMgmtPort, 8091)
	portForward(t, dataNS, "svc/couchbase", localCouchbaseKVPort, 11210)
	return openMySQL(t, localMySQLPort), openCouchbase(t)
}

// readCouchbaseSink folds the sink's documents to the oracle shape (id →
// state). Couchbase is native upsert-by-key and physical remove-by-key: an
// update replaces the document in place and a delete removes it outright — no
// tombstone, no versioning, no merge. So every data document is the current
// live row, and an absent key is a deleted row. A full collection scan is used
// (data keys are JSON arrays; the control document's "_" key is skipped) so a
// delete that failed to apply shows up as an extra key rather than being
// missed by probing only the source's keys.
func readCouchbaseSink(cb *couchbaseReader, coll string) (map[int64]rowState, error) {
	res, err := cb.bucket.Scope(couchbaseScope).Collection(coll).Scan(gocb.RangeScan{}, nil)
	if err != nil {
		return nil, fmt.Errorf("scan couchbase %s: %w", coll, err)
	}
	defer func() { _ = res.Close() }()
	out := map[int64]rowState{}
	for item := res.Next(); item != nil; item = res.Next() {
		id := item.ID()
		// The control document lives in the same collection; the sink
		// reserves the "_" prefix for it (and its metadata sub-object). Data
		// keys start with "[", so this cannot hide a data document.
		if strings.HasPrefix(id, "_") {
			continue
		}
		var doc struct {
			ID     int64    `json:"id"`
			V      string   `json:"v"`
			Amount *float64 `json:"amount"`
		}
		if err := item.Content(&doc); err != nil {
			return nil, fmt.Errorf("decode couchbase %s doc %s: %w", coll, id, err)
		}
		st := rowState{V: doc.V}
		if doc.Amount != nil {
			st.Amount = sql.NullFloat64{Float64: *doc.Amount, Valid: true}
		}
		out[doc.ID] = st
	}
	if err := res.Err(); err != nil {
		return nil, fmt.Errorf("scan couchbase %s: %w", coll, err)
	}
	return out, nil
}

// mustReadCouchbaseSink is readCouchbaseSink failing the test on error, for the
// final exact-map assertion (the settle loop polls through readCouchbaseSink
// directly so a collection not yet created is retried, not fatal).
func mustReadCouchbaseSink(t *testing.T, cb *couchbaseReader, coll string) map[int64]rowState {
	t.Helper()
	out, err := readCouchbaseSink(cb, coll)
	if err != nil {
		t.Fatalf("read couchbase %s: %v", coll, err)
	}
	return out
}

// waitCouchbaseSettled polls until the Couchbase sink's folded state equals the
// source's exactly, or fails with the residual diff. It is the Couchbase-sink
// sibling of waitSettledTables (whose poll reads Trino).
func waitCouchbaseSettled(t *testing.T, source *sql.DB, cb *couchbaseReader, coll string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		src := readOrders(t, source)
		sink, err := readCouchbaseSink(cb, coll)
		if err != nil {
			// The collection is created at pipeline boot; a scan that races
			// that creation is retried, not fatal.
			lastErr = err
		} else {
			missing, extra, wrong := diffSinkSource(src, sink)
			if len(missing) == 0 && len(extra) == 0 && len(wrong) == 0 {
				return
			}
			lastErr = fmt.Errorf("source=%d sink=%d missing=%v extra=%v wrongValue=%v",
				len(src), len(sink), cap10(missing), cap10(extra), cap10(wrong))
		}
		if time.Now().After(deadline) {
			t.Fatalf("couchbase sink %s never settled to source: %v", coll, lastErr)
		}
		time.Sleep(time.Second)
	}
}

// couchbaseControlPosition reads the control document's scalar and per-owner
// positions, for the failed-boundary report. It returns an error instead of
// failing the test: it runs inside a t.Cleanup, and a t.Fatalf there would
// abort the very diagnostics it exists to produce.
func couchbaseControlPosition(cb *couchbaseReader, coll string) (string, map[string]string, bool, error) {
	res, err := cb.bucket.Scope(couchbaseScope).Collection(coll).Get(couchbaseControlKey, nil)
	if err != nil {
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			return "", nil, false, nil
		}
		return "", nil, false, err
	}
	var doc struct {
		Position  string            `json:"position"`
		Positions map[string]string `json:"positions"`
	}
	if err := res.Content(&doc); err != nil {
		return "", nil, false, err
	}
	return doc.Position, doc.Positions, true, nil
}

// couchbaseBoundaries is the boundary set both commit modes share: Couchbase
// is a direct-path sink (not a sink.StagingWriter), so the direct worker and
// coordinator points apply. The staged boundaries do not — there is no
// coordinator cycle.
var couchbaseBoundaries = []boundaryCase{
	{point: faultinject.WorkerBatchReceived},
	{point: faultinject.WorkerCommitBefore},
	{point: faultinject.WorkerCommittedBeforeAck},
	{point: faultinject.CoordinatorAckBeforeRecord, coordinator: true},
}

// couchbaseFastBoundaries is the direct set plus fast mode's one internal
// window: the data documents are durable, the control document is not yet
// written. Atomic mode has no such window — data and control commit together
// in one transaction — so it runs couchbaseBoundaries alone.
var couchbaseFastBoundaries = append([]boundaryCase{
	{point: faultinject.WorkerCouchbaseDataBeforeControl},
}, couchbaseBoundaries...)

// startCouchbaseBoundaryPipeline starts a MySQL→Couchbase pipeline in the
// given commit mode over shop.orders with workers owners and waits until it
// has converged; it returns the sink collection name.
func startCouchbaseBoundaryPipeline(t *testing.T, source *sql.DB, cb *couchbaseReader, pipeline, serverID, base string, workers int, commitMode string) string {
	t.Helper()
	target := uniqueTarget(base)
	cr := buildCR(pipeline, testNS, raceImage(), "pod-e2e-source", couchbaseCatalogSecret, serverID,
		[]tableSpec{{Source: "shop.orders", Target: "raw." + target, PrimaryKey: []string{"id"}, Workers: workers}},
		crOptions{SinkType: "couchbase", SinkNamespace: couchbaseBucket, CommitMode: commitMode})
	applyPipeline(t, testNS, pipeline, cr)
	waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 5*time.Minute)
	waitPodsByPrefix(t, testNS, workerSTSs(t, testNS, pipeline)[0]+"-", workers, 5*time.Minute)
	waitCouchbaseSettled(t, source, cb, target, 5*time.Minute)
	return target
}

// runCouchbaseBoundaryCases drives one MySQL→Couchbase pipeline through each
// case in turn, exactly as runBoundaryCases does for Iceberg: arm the point,
// write load until it fires (the Pod restarts), then stop the load and require
// the Couchbase sink to converge to the source exactly.
func runCouchbaseBoundaryCases(t *testing.T, source *sql.DB, cb *couchbaseReader, pipeline, target string, workers int, cases []boundaryCase) {
	t.Helper()
	for _, bc := range cases {
		t.Run(string(bc.point), func(t *testing.T) {
			coord := waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 8*time.Minute)[0]
			stss := workerSTSs(t, testNS, pipeline)
			// The first ordinal: on a direct sink the single owner (or each
			// partition owner) reaches the worker-side boundaries, so any one
			// of them exercises the point.
			pod := waitPodsByPrefix(t, testNS, stss[0]+"-", workers, 8*time.Minute)[0]
			if bc.coordinator {
				pod = coord
			}

			before := podRestarts(t, testNS, pod)
			logs := followLogs(t, testNS, pod)
			armFault(t, testNS, pod, bc.point, "raw."+target)
			reportCouchbaseBoundaryOnFailure(t, source, cb, pipeline, bc.point, target)
			stop := startWriter(t, source, 250*time.Millisecond)
			line := waitFaultFired(t, testNS, pod, logs, bc.point, before, 4*time.Minute)
			stop()
			t.Logf("fired: %s", strings.TrimSpace(line))
			if !strings.Contains(line, "table=raw."+target) {
				t.Fatalf("diagnostic line does not name the table: %q", line)
			}

			waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 8*time.Minute)
			waitPodsByPrefix(t, testNS, stss[0]+"-", workers, 8*time.Minute)
			waitCouchbaseSettled(t, source, cb, target, 10*time.Minute)
			assertSinkEqualsSource(t, readOrders(t, source), mustReadCouchbaseSink(t, cb, target))
		})
	}
}

// reportCouchbaseBoundaryOnFailure registers, for a failed Couchbase boundary
// case, the boundary, the source's executed position and the sink's control
// document, logged; and the cluster state dumped under URUTAU_E2E_ARTIFACTS.
func reportCouchbaseBoundaryOnFailure(t *testing.T, source *sql.DB, cb *couchbaseReader, pipeline string, point faultinject.Point, target string) {
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
		pos, owners, ok, err := couchbaseControlPosition(cb, target)
		switch {
		case err != nil:
			t.Logf("boundary %s: couchbase sink %s control read: %v", point, target, err)
		case ok:
			t.Logf("boundary %s: couchbase sink %s control position=%q owners=%v", point, target, pos, owners)
		default:
			t.Logf("boundary %s: couchbase sink %s has no control document", point, target)
		}
		base := os.Getenv("URUTAU_E2E_ARTIFACTS")
		if base == "" {
			base = filepath.Join(os.TempDir(), "urutau-e2e")
		}
		dir := filepath.Join(base, fmt.Sprintf("commit-boundary-cb-%s-%d", point, time.Now().Unix()))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Logf("artifacts: %v", err)
			return
		}
		dumpDiagnostics(ctx, dir, testNS, pipeline, nil, nil)
		t.Logf("boundary %s: diagnostics under %s", point, dir)
	})
}

// TestCommitBoundaryCouchbase fires the commit-boundary matrix against a
// MySQL→Couchbase pipeline in each commit mode. Both modes take the direct
// path (Couchbase is not a staging sink); fast mode exercises its extra
// data-before-control window. A missing Couchbase service fails setup, never
// skips.
func TestCommitBoundaryCouchbase(t *testing.T) {
	requirePods(t)

	t.Run("fast", func(t *testing.T) {
		source, cb := setupPodEnvCouchbase(t)
		const pipeline = "pod-fault-cb-fast"
		seedOrders(t, source, 50)
		target := startCouchbaseBoundaryPipeline(t, source, cb, pipeline, "2501", "pod_fault_cb_fast", 1, "fast")
		runCouchbaseBoundaryCases(t, source, cb, pipeline, target, 1, couchbaseFastBoundaries)
		assertNoRaces(t, testNS, pipeline+"-")
	})

	t.Run("atomic", func(t *testing.T) {
		source, cb := setupPodEnvCouchbase(t)
		const pipeline = "pod-fault-cb-atomic"
		seedOrders(t, source, 50)
		target := startCouchbaseBoundaryPipeline(t, source, cb, pipeline, "2502", "pod_fault_cb_atomic", 1, "atomic")
		runCouchbaseBoundaryCases(t, source, cb, pipeline, target, 1, couchbaseBoundaries)
		assertNoRaces(t, testNS, pipeline+"-")
	})
}

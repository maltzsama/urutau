package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"

	urutauiceberg "github.com/maltzsama/urutau/internal/sink/iceberg"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/spec"
)

const maintTableName = "maintenance"

// TestMaintainerCompactsAndPreservesPosition proves the issue #96 finding
// end to end: a compaction pass started via Maintainer.RunOnce rewrites many
// small files into few, AND carries cdc.position forward onto the rewrite
// snapshot's properties — so CommittedPosition reads it from the fast path
// (the table property) rather than falling back to walkBackPosition's
// newest-snapshot-with-the-property scan. Run with URUTAU_E2E=1 against the
// stack in docker-compose.yml.
func TestMaintainerCompactsAndPreservesPosition(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cfg := urutauiceberg.Config{
		URI:          env("URUTAU_E2E_CATALOG", "http://localhost:8181/api/catalog"),
		Warehouse:    env("URUTAU_E2E_WAREHOUSE", "quickstart_catalog"),
		ClientID:     env("URUTAU_E2E_CLIENT_ID", "root"),
		ClientSecret: env("URUTAU_E2E_CLIENT_SECRET", "s3cr3t"),
		Scope:        "PRINCIPAL_ROLE:ALL",
	}
	cat, err := urutauiceberg.NewCatalog(ctx, cfg)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	if err := urutauiceberg.EnsureNamespace(ctx, cat, table.Identifier{nsName}); err != nil {
		t.Fatalf("namespace: %v", err)
	}
	ident := table.Identifier{nsName, maintTableName}
	_ = cat.DropTable(ctx, ident)
	tbl, err := cat.CreateTable(ctx, ident, spikeIcebergSchema,
		catalog.WithProperties(iceberg.Properties{"format-version": "2"}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Write 6 small, separately-committed data files — the same
	// one-file-per-commit shape rewrite_data_files_test.go's SmallFiles
	// case uses, which is exactly what CDC batches produce naturally (a
	// small commit per worker flush interval).
	const nFiles = 6
	var lastPos string
	for i := range nFiles {
		lastPos = fmt.Sprintf("p%d", i)
		must(t, urutauiceberg.Append(ctx, tbl, spikeRecord(int64(i+1), lastPos), props(lastPos)))
		tbl = reload(t, ctx, cat, ident)
	}

	tasksBefore, err := tbl.Scan().PlanFiles(ctx)
	if err != nil {
		t.Fatalf("plan files before: %v", err)
	}
	if len(tasksBefore) != nFiles {
		t.Fatalf("data files before compaction = %d, want %d (one per commit)", len(tasksBefore), nFiles)
	}

	// MinInputFiles: 2 so 6 tiny files are well above threshold — this test
	// is about the mechanism firing and the position surviving it, not
	// about tuning the bin-packing threshold.
	m := urutauiceberg.NewMaintainer(cat, ident, spec.Maintenance{
		Enabled:    true,
		Compaction: &spec.CompactionConfig{MinInputFiles: 2},
	}, nil, func() string { return lastPos }, nil)

	// RunOnce is one-shot — the exact call the ephemeral maintenance worker
	// makes — so the pass is done when it returns; no ticker to wait out.
	if err := m.RunOnce(ctx, []sink.MaintenanceOp{sink.MaintenanceCompaction}); err != nil {
		t.Fatalf("RunOnce compaction: %v", err)
	}

	tbl = reload(t, ctx, cat, ident)
	tasksAfter, err := tbl.Scan().PlanFiles(ctx)
	if err != nil {
		t.Fatalf("plan files after: %v", err)
	}
	if len(tasksAfter) >= nFiles {
		t.Fatalf("data files after compaction = %d, want fewer than %d — compaction did not run", len(tasksAfter), nFiles)
	}
	t.Logf("compaction: %d files -> %d files", nFiles, len(tasksAfter))

	// The finding under test: cdc.position must be readable via the FAST
	// PATH (the table property), not require CommittedPosition's
	// walk-back — proving RewriteDataFilesOptions.SnapshotProps actually
	// carried it onto the rewrite snapshot.
	if got := tbl.Properties()["cdc.position"]; got != lastPos {
		t.Fatalf("table property cdc.position = %q after compaction, want %q (compaction must carry the position forward, not drop it)", got, lastPos)
	}
	gotPos, err := urutauiceberg.CommittedPosition(ctx, cat, ident)
	if err != nil {
		t.Fatalf("CommittedPosition: %v", err)
	}
	if gotPos != lastPos {
		t.Fatalf("CommittedPosition = %q, want %q", gotPos, lastPos)
	}

	rows := trinoRows(t, ctx, fmt.Sprintf(`SELECT count(*) FROM %s`, maintTableName))
	if got := rows[0][0]; fmt.Sprint(got) != fmt.Sprint(int64(nFiles)) {
		t.Fatalf("row count after compaction = %v, want %d (compaction must not lose or duplicate rows)", got, nFiles)
	}
}

// TestMaintainerExpiresSnapshotsRespectingMaxAge proves the other issue #96
// finding: MaxAge is a real safety window, not just a snapshot-count knob —
// a snapshot younger than MaxAge is never expired even when RetainLast
// alone would have allowed it, and it stays retrievable through Trino
// history queries.
func TestMaintainerExpiresSnapshotsRespectingMaxAge(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cfg := urutauiceberg.Config{
		URI:          env("URUTAU_E2E_CATALOG", "http://localhost:8181/api/catalog"),
		Warehouse:    env("URUTAU_E2E_WAREHOUSE", "quickstart_catalog"),
		ClientID:     env("URUTAU_E2E_CLIENT_ID", "root"),
		ClientSecret: env("URUTAU_E2E_CLIENT_SECRET", "s3cr3t"),
		Scope:        "PRINCIPAL_ROLE:ALL",
	}
	cat, err := urutauiceberg.NewCatalog(ctx, cfg)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	if err := urutauiceberg.EnsureNamespace(ctx, cat, table.Identifier{nsName}); err != nil {
		t.Fatalf("namespace: %v", err)
	}
	ident := table.Identifier{nsName, "maintenance-expiry"}
	_ = cat.DropTable(ctx, ident)
	tbl, err := cat.CreateTable(ctx, ident, spikeIcebergSchema,
		catalog.WithProperties(iceberg.Properties{"format-version": "2"}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	const nCommits = 5
	for i := range nCommits {
		must(t, urutauiceberg.Append(ctx, tbl, spikeRecord(int64(i+1), "v"), props(fmt.Sprintf("p%d", i))))
		tbl = reload(t, ctx, cat, ident)
	}
	if got := len(tbl.Metadata().Snapshots()); got != nCommits {
		t.Fatalf("snapshots before expiry = %d, want %d", got, nCommits)
	}

	// MaxAge: 1h means every snapshot just created (seconds old) is
	// protected regardless of RetainLast — the safety-window contract.
	probe := &expiryProbe{}
	m := urutauiceberg.NewMaintainer(cat, ident, spec.Maintenance{
		Enabled:        true,
		SnapshotExpiry: &spec.SnapshotExpiryConfig{RetainLast: 1, MaxAge: "1h"},
	}, nil, nil, probe)

	// RunOnce is one-shot — the exact call the ephemeral maintenance worker
	// makes.
	if err := m.RunOnce(ctx, []sink.MaintenanceOp{sink.MaintenanceSnapshotExpiry}); err != nil {
		t.Fatalf("RunOnce snapshot expiry: %v", err)
	}

	// The pass must have actually SUCCEEDED — otherwise "snapshot count
	// unchanged" would pass trivially because the operation never ran, not
	// because MaxAge protected anything.
	if probe.successes == 0 {
		t.Fatalf("snapshot expiry never completed a successful run (last error: %v)", probe.lastErr)
	}

	tbl = reload(t, ctx, cat, ident)
	if got := len(tbl.Metadata().Snapshots()); got != nCommits {
		t.Fatalf("snapshots after expiry with MaxAge=1h = %d, want unchanged %d — MaxAge must protect recent snapshots regardless of RetainLast", got, nCommits)
	}
}

// expiryProbe is a MaintainerMetrics that only tracks whether a run
// succeeded, so the test can distinguish "MaxAge protected everything" from
// "the ticker never got a clean run at all."
type expiryProbe struct {
	successes int
	lastErr   error
}

func (p *expiryProbe) CompactionRun(string, int, int, int64, int64, error) {}
func (p *expiryProbe) OrphanCleanupRun(string, int, int64, error)          {}
func (p *expiryProbe) SnapshotExpiryRun(_ string, _ int, err error) {
	if err == nil {
		p.successes++
	} else {
		p.lastErr = err
	}
}

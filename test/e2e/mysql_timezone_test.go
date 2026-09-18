package e2e

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/runner"
	"github.com/maltzsama/urutau/spec"
)

// TestMySQLTimezoneParity drives the real stack with
// `timezone=America/Sao_Paulo` on the source URI (issue #139). A row written
// before the runner (DBLog snapshot path) and a row written after (live CDC
// path), carrying the SAME source wall clock, must read back identically in
// Iceberg — for both a naive DATETIME and an instant TIMESTAMP. Before the
// fix, the snapshot used parseTime=true (time.Time) while the CDC returned
// raw strings, so the two paths could disagree.
func TestMySQLTimezoneParity(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	s := loadTimezonePipeline(t)
	db := mysqlConn(t)

	resetBinlog(t, db)
	dropIcebergTableNamed(t, ctx, "raw", "events")
	if _, err := db.Exec("DELETE FROM events"); err != nil {
		t.Fatalf("clear events: %v", err)
	}

	// Row 1 exists before the runner → snapshot path.
	dml(t, db, `INSERT INTO events (id, happened_at, recorded_at) VALUES (1, '2023-01-08 12:30:45', '2023-01-08 12:30:45')`)

	runCtx, stop := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() {
		runErr <- runner.Run(runCtx, s, testConfig())
	}()
	defer func() {
		stop()
		if err := <-runErr; err != nil && err != context.Canceled {
			t.Errorf("runner: %v", err)
		}
	}()

	waitTrino(t, ctx, `SELECT count(*) FROM events`, int64(1))

	// Row 2 arrives live → CDC path, same wall clock as row 1.
	dml(t, db, `INSERT INTO events (id, happened_at, recorded_at) VALUES (2, '2023-01-08 12:30:45', '2023-01-08 12:30:45')`)
	waitTrino(t, ctx, `SELECT count(*) FROM events`, int64(2))

	// Parity: snapshot and CDC must agree for both temporal kinds.
	snapDT := trinoScalar(t, ctx, `SELECT CAST(happened_at AS VARCHAR) FROM events WHERE id = 1`)
	cdcDT := trinoScalar(t, ctx, `SELECT CAST(happened_at AS VARCHAR) FROM events WHERE id = 2`)
	if snapDT != cdcDT {
		t.Fatalf("DATETIME parity: snapshot=%v cdc=%v", snapDT, cdcDT)
	}
	snapTS := trinoScalar(t, ctx, `SELECT CAST(recorded_at AS VARCHAR) FROM events WHERE id = 1`)
	cdcTS := trinoScalar(t, ctx, `SELECT CAST(recorded_at AS VARCHAR) FROM events WHERE id = 2`)
	if snapTS != cdcTS {
		t.Fatalf("TIMESTAMP parity: snapshot=%v cdc=%v", snapTS, cdcTS)
	}

	// The naive DATETIME was interpreted in -03:00, so the instant (stored as
	// a zone-less Iceberg timestamp) is 15:30:45 UTC.
	if got, _ := snapDT.(string); !strings.HasPrefix(got, "2023-01-08 15:30:45") {
		t.Fatalf("DATETIME value = %v, want 2023-01-08 15:30:45 (12:30:45 in -03:00)", snapDT)
	}

	t.Logf("timezone parity ok: DATETIME=%v TIMESTAMP=%v", snapDT, snapTS)
}

// loadTimezonePipeline loads examples/mysql-iceberg-timezone.yaml, overriding
// the catalog URI/warehouse when the CI stack differs from the file defaults.
func loadTimezonePipeline(t *testing.T) *spec.Spec {
	t.Helper()
	return loadExampleSpec(t, "mysql-iceberg-timezone.yaml", func(s *spec.Spec) {
		s.Sink.URI = env("URUTAU_E2E_CATALOG", s.Sink.URI)
		s.Sink.Warehouse = env("URUTAU_E2E_WAREHOUSE", s.Sink.Warehouse)
	})
}

// trinoScalar runs a query expected to return exactly one cell.
func trinoScalar(t *testing.T, ctx context.Context, q string) any {
	t.Helper()
	rows := trinoRows(t, ctx, q)
	if len(rows) != 1 || len(rows[0]) != 1 {
		t.Fatalf("trino %q: got %v, want one scalar", q, rows)
	}
	return rows[0][0]
}

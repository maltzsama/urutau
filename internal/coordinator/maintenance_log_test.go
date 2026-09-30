package coordinator

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/spec"
)

// Issue #457: a staged table was never compacted nor expired for a whole
// run, and nothing recorded why: a pass's outcome reached only the metrics
// and the dashboard, and the maintenance worker's own log went only to the
// dashboard's buffer, both gone with the run. The coordinator's log records
// every pass: its table, the operations, each one's outcome or error, and
// how long the worker held the table.
func TestSessionLogsEachPassOutcome(t *testing.T) {
	m, _ := testScheduler(t)
	var buf bytes.Buffer
	m.c.log = slog.New(slog.NewTextHandler(&buf, nil))
	m.cfg = &spec.Maintenance{
		Enabled:        true,
		Compaction:     &spec.CompactionConfig{Interval: "1h"},
		SnapshotExpiry: &spec.SnapshotExpiryConfig{Interval: "1h"},
	}
	m.register("w", "raw.orders")

	stream := &fakeSessionStream{in: []*pb.WorkerMessage{{
		Msg: &pb.WorkerMessage_MaintenanceResult{MaintenanceResult: &pb.MaintenanceResult{
			Ops: []*pb.MaintenanceOpResult{
				{Op: &pb.MaintenanceOpResult_Compaction{Compaction: &pb.CompactionResult{FilesRemoved: 3, FilesAdded: 1}}},
				{Op: &pb.MaintenanceOpResult_Expiry{Expiry: &pb.ExpiryResult{Error: "commit rejected"}}},
			},
		}},
	}}}
	if err := m.session(stream, &pb.Hello{WorkerName: "w", Maintenance: true}); err != nil {
		t.Fatalf("session: %v", err)
	}

	var pass string
	for _, line := range strings.Split(buf.String(), "\n") {
		if strings.Contains(line, "maintenance pass") {
			pass = line
		}
	}
	for _, want := range []string{"table=raw.orders", "took=", "files_removed=3", "commit rejected"} {
		if !strings.Contains(pass, want) {
			t.Fatalf("pass log %q does not record %q", pass, want)
		}
	}
	if !strings.Contains(pass, "level=WARN") {
		t.Fatalf("pass log %q: a pass with a failed operation must be a warning", pass)
	}
}

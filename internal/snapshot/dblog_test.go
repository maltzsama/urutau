package snapshot

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/change"
	"github.com/maltzsama/urutau/internal/position"
)

const testUUID = "3e11fa47-71ca-11e1-9e33-c80aa9429562"

// gt builds a cumulative GTID set with a single interval, e.g. "1-5".
func gt(interval string) *position.GTID {
	return position.MustGTID(testUUID + ":" + interval)
}

// fakeSource is a deterministic in-memory table of integer PK rows.
type fakeSource struct {
	rows []map[string]any // ordered by id
}

func (f *fakeSource) PK() []string { return []string{"id"} }

func (f *fakeSource) Bounds(ctx context.Context) ([][]any, error) {
	// One bound every 2 rows: [1, 3, 5, ...].
	var bounds [][]any
	for i := 0; i < len(f.rows); i += 2 {
		bounds = append(bounds, []any{f.rows[i]["id"]})
	}
	return bounds, nil
}

func (f *fakeSource) Scan(ctx context.Context, ch Chunk, fn func(map[string]any) error) error {
	for _, row := range f.rows {
		id := row["id"].(int64)
		if ch.Low != nil && id < ch.Low[0].(int64) {
			continue
		}
		if ch.High != nil && id >= ch.High[0].(int64) {
			continue
		}
		if err := fn(row); err != nil {
			return err
		}
	}
	return nil
}

// fakeRelay records the orchestrator's calls in order.
type fakeRelay struct {
	ops    []string
	rows   map[uint32][]change.Change
	relPos map[uint32]string
}

func (r *fakeRelay) Release(table string, id uint32, at position.Position) {
	r.ops = append(r.ops, fmt.Sprintf("release:%d", id))
	r.relPos[id] = at.String()
}
func (r *fakeRelay) AddWindowRows(target string, id uint32, rows []change.Change) error {
	r.ops = append(r.ops, fmt.Sprintf("add:%d:%d", id, len(rows)))
	r.rows[id] = rows
	return nil
}
func (r *fakeRelay) GateOn(table string, id uint32) {
	r.ops = append(r.ops, fmt.Sprintf("gateon:%d", id))
}
func (r *fakeRelay) GateFlush() {
	r.ops = append(r.ops, "gateflush")
}

// fakeReader converges to master after convergeAfter Synced() calls. The
// positions are interface values, so the same fake exercises GTID sets and
// LSNs alike — the orchestrator must not care.
type fakeReader struct {
	master        position.Position
	early         position.Position
	calls         int
	convergeAfter int
	marks, clears int
}

func (r *fakeReader) Synced() position.Position {
	r.calls++
	if r.calls > r.convergeAfter {
		return r.master
	}
	if r.early != nil {
		return r.early
	}
	return nil
}

func (r *fakeReader) Master(ctx context.Context) (position.Position, error) { return r.master, nil }
func (r *fakeReader) OpenWindow(ctx context.Context, chunkID uint32)        { r.marks++ }
func (r *fakeReader) ClearWindow()                                          { r.clears++ }

// rowsFor builds a deterministic source: ids 1..n with v = "v<id>".
func rowsFor(n int) *fakeSource {
	f := &fakeSource{}
	for id := 1; id <= n; id++ {
		f.rows = append(f.rows, map[string]any{"id": int64(id), "v": fmt.Sprintf("v%d", id)})
	}
	return f
}

func TestSnapshotTableHappyPath(t *testing.T) {
	// 6 rows, bound every 2 → chunks [1,3), [3,5), [5,∞). The reader
	// converges on its 4th Synced call: chunk 0's low is the pre-snapshot
	// position, later chunks read an already-caught-up low.
	src := rowsFor(6)
	relay := &fakeRelay{rows: map[uint32][]change.Change{}, relPos: map[uint32]string{}}
	reader := &fakeReader{master: gt("1-9"), early: gt("1-1"), convergeAfter: 3}

	err := SnapshotTable(context.Background(), src, reader, relay, "raw.orders",
		SnapshotConfig{CaughtUpPoll: time.Millisecond})
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// Order per chunk: gate on → add → gate flush → release.
	want := []string{
		"gateon:0", "add:0:2", "gateflush", "release:0",
		"gateon:1", "add:1:2", "gateflush", "release:1",
		"gateon:2", "add:2:2", "gateflush", "release:2",
	}
	if len(relay.ops) != len(want) {
		t.Fatalf("ops = %v, want %v", relay.ops, want)
	}
	for i := range want {
		if relay.ops[i] != want[i] {
			t.Fatalf("op %d = %q, want %q (all: %v)", i, relay.ops[i], want[i], relay.ops)
		}
	}
	if reader.marks != 3 || reader.clears != 3 {
		t.Fatalf("marks/clears = %d/%d, want 3/3 (one window per chunk)", reader.marks, reader.clears)
	}

	// Window rows: inserts keyed by id. Chunk 0's rows carry the
	// pre-snapshot low watermark; later chunks read an already-converged
	// synced position.
	for chunkID, rows := range relay.rows {
		wantLow := gt("1-1")
		if chunkID > 0 {
			wantLow = gt("1-9")
		}
		for j, row := range rows {
			if row.Op != change.OpInsert {
				t.Fatalf("chunk %d row %d: op %v, want insert", chunkID, j, row.Op)
			}
			wantID := int64(chunkID*2) + int64(j) + 1
			if len(row.Key) != 1 || row.Key[0] != wantID {
				t.Fatalf("chunk %d row %d: key %v, want [%d]", chunkID, j, row.Key, wantID)
			}
			if row.After["v"] != fmt.Sprintf("v%d", wantID) {
				t.Fatalf("chunk %d row %d: v %v", chunkID, j, row.After["v"])
			}
			if row.Position != wantLow.String() {
				t.Fatalf("chunk %d row %d: position %q, want %q", chunkID, j, row.Position, wantLow)
			}
		}
	}

	// Releases only after caught-up: the release position is the master.
	for id, pos := range relay.relPos {
		if pos != gt("1-9").String() {
			t.Fatalf("release %d at %q, want caught-up position %q", id, pos, gt("1-9"))
		}
	}
}

// TestSnapshotTableWithLSNPositions runs the same happy path with LSN
// positions — the proof that the orchestrator is source-agnostic.
func TestSnapshotTableWithLSNPositions(t *testing.T) {
	src := rowsFor(4)
	relay := &fakeRelay{rows: map[uint32][]change.Change{}, relPos: map[uint32]string{}}
	reader := &fakeReader{master: position.MustLSN("0/20"), early: position.MustLSN("0/10"), convergeAfter: 3}

	err := SnapshotTable(context.Background(), src, reader, relay, "raw.orders",
		SnapshotConfig{CaughtUpPoll: time.Millisecond})
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(relay.ops) != 8 {
		t.Fatalf("ops = %v, want two full chunk cycles", relay.ops)
	}
	if relay.relPos[0] != "0/20" || relay.relPos[1] != "0/20" {
		t.Fatalf("releases at %v, want caught-up 0/20", relay.relPos)
	}
	// Chunk 0 rows carry the early watermark; chunk 1 rows the converged one.
	if relay.rows[0][0].Position != "0/10" {
		t.Errorf("chunk 0 position = %q, want 0/10", relay.rows[0][0].Position)
	}
	if relay.rows[1][0].Position != "0/20" {
		t.Errorf("chunk 1 position = %q, want 0/20", relay.rows[1][0].Position)
	}
}

func TestSnapshotTableEmptySource(t *testing.T) {
	relay := &fakeRelay{rows: map[uint32][]change.Change{}, relPos: map[uint32]string{}}
	reader := &fakeReader{master: gt("1-1"), early: gt("1-1")}

	err := SnapshotTable(context.Background(), &fakeSource{}, reader, relay, "raw.orders", SnapshotConfig{})
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(relay.ops) != 0 {
		t.Fatalf("ops on empty source: %v", relay.ops)
	}
}

func TestSnapshotTableWindowTimeoutIsPathology(t *testing.T) {
	src := rowsFor(4)
	relay := &fakeRelay{rows: map[uint32][]change.Change{}, relPos: map[uint32]string{}}
	// The reader never converges: synced stays behind master forever.
	reader := &fakeReader{master: gt("1-9"), early: gt("1-1"), convergeAfter: 1 << 30}

	err := SnapshotTable(context.Background(), src, reader, relay, "raw.orders",
		SnapshotConfig{WindowTimeout: 50 * time.Millisecond, CaughtUpPoll: 5 * time.Millisecond})
	if err == nil {
		t.Fatal("stuck window must surface as an error, never close on a timer")
	}
	if !strings.Contains(err.Error(), "window stuck open") {
		t.Fatalf("error %v, want window-stuck pathology", err)
	}
	// The first chunk's window was marked and cleared by the error path,
	// but never released.
	if reader.marks != 1 || reader.clears != 1 {
		t.Fatalf("marks/clears = %d/%d, want 1/1 on a stuck window", reader.marks, reader.clears)
	}
	for _, op := range relay.ops {
		if strings.HasPrefix(op, "release:") {
			t.Fatalf("release observed on stuck window: %v", relay.ops)
		}
	}
}

func TestSnapshotTableLagConvergesBeforeRelease(t *testing.T) {
	src := rowsFor(2)
	relay := &fakeRelay{rows: map[uint32][]change.Change{}, relPos: map[uint32]string{}}
	// Synced converges on the 4th call (low, poll×k, at) — proving Release
	// waits for the caught-up proof against the fixed master high, never a
	// timer.
	reader := &fakeReader{master: gt("1-5"), early: gt("1-1"), convergeAfter: 3}

	err := SnapshotTable(context.Background(), src, reader, relay, "raw.orders",
		SnapshotConfig{CaughtUpPoll: time.Millisecond})
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(relay.ops) != 4 {
		t.Fatalf("ops = %v, want one full chunk cycle", relay.ops)
	}
	if relay.ops[3] != "release:0" {
		t.Fatalf("op 3 = %q, want release:0", relay.ops[3])
	}
	if relay.relPos[0] != gt("1-5").String() {
		t.Fatalf("release at %q, want converged %q", relay.relPos[0], gt("1-5"))
	}
}

// movingMasterReader keeps Master() advancing so a caught-up reader can never
// contain the live master: the proof must target the FIXED high watermark
// passed in, never the moving master — or a busy source would hold the window
// open until the timeout pathology.
type movingMasterReader struct {
	synced *position.GTID
	mu     sync.Mutex
	n      int
}

func (r *movingMasterReader) Synced() position.Position { return r.synced }
func (r *movingMasterReader) Master(ctx context.Context) (position.Position, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.n++
	return gt(fmt.Sprintf("1-%d", 100+r.n)), nil
}
func (r *movingMasterReader) OpenWindow(ctx context.Context, chunkID uint32) {}
func (r *movingMasterReader) ClearWindow()                                   {}

func TestWaitCaughtUpTargetsFixedHighNotMovingMaster(t *testing.T) {
	// The reader is caught up to "1-10" and high is "1-10": the proof must
	// pass immediately, even though Master() keeps returning a set far ahead
	// that the reader can never contain.
	r := &movingMasterReader{synced: gt("1-10")}
	if err := WaitCaughtUp(context.Background(), r, gt("1-10"),
		SnapshotConfig{CaughtUpPoll: time.Millisecond, WindowTimeout: 2 * time.Second}); err != nil {
		t.Fatalf("fixed high must end the wait even as the live master advances: %v", err)
	}
}

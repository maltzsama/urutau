// Package faultinject is test instrumentation for the commit-boundary crash
// matrix (issue #354): named points in the commit path where a test can make
// the process die at exactly that step, so each crash window is reproducible
// instead of a matter of timing.
//
// The points are live only in binaries built with the faultinject tag (the
// race-instrumented pod e2e image, build/Dockerfile.race). Everywhere else
// enabled is false and At is an empty function: no file is read and the
// commit path behaves exactly as without it.
//
// In an enabled binary a point is armed by a file (default /tmp/urutau-fault,
// overridable with URUTAU_FAULT_FILE) of key=value lines:
//
//	point=worker.committed-before-ack   required: which boundary
//	table=raw.orders                    optional: only this target table
//	skip=3                              optional: let this many matching hits pass
//
// When an armed point is reached the process removes the file (so the fault
// is one-shot and the restarted container is not armed), writes one
// "urutau: FAULT INJECTED" line naming the point and the call site's context
// (table, batch, positions) to stderr, and ends itself without running any
// deferred cleanup, as in a real crash (see kill for why that is not SIGKILL
// alone when the process is a container's PID 1).
//
// The boundaries themselves are documented in
// website/docs/architecture/commit-boundaries.md.
package faultinject

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"sync"
)

// Point names one commit boundary.
type Point string

// The commit boundaries. Direct path: D1-D5; staged path: S1-S5 (see the doc
// page for what is durable at each).
const (
	// WorkerBatchReceived: a Flight batch reached the worker and was not
	// yet applied (D1, S1).
	WorkerBatchReceived Point = "worker.batch-received"
	// WorkerCommitBefore: a collapsed batch is about to be committed
	// directly (D2).
	WorkerCommitBefore Point = "worker.commit-before"
	// IcebergUpsertBetweenDeleteAndAppend: an upsert's equality deletes are
	// committed, its appends and position are not (D3).
	IcebergUpsertBetweenDeleteAndAppend Point = "iceberg.upsert-between-delete-and-append"
	// WorkerCommittedBeforeAck: the direct commit (data and position) is
	// durable, the ack was not sent (D4).
	WorkerCommittedBeforeAck Point = "worker.committed-before-ack"
	// CoordinatorAckBeforeRecord: the coordinator received an ack and has
	// not recorded it (D5).
	CoordinatorAckBeforeRecord Point = "coordinator.ack-before-record"
	// WorkerStagedBeforeShip: a staged sub-batch's data files are written,
	// its descriptor was not shipped (S2).
	WorkerStagedBeforeShip Point = "worker.staged-before-ship"
	// WorkerStagedShippedBeforeAck: the descriptor was shipped, the ack was
	// not sent (S3).
	WorkerStagedShippedBeforeAck Point = "worker.staged-shipped-before-ack"
	// CoordinatorCycleBeforeCommit: every owner delivered a cycle,
	// CommitStaged was not called (S4).
	CoordinatorCycleBeforeCommit Point = "coordinator.cycle-before-commit"
	// CoordinatorCycleCommittedBeforeRecord: CommitStaged is durable, the
	// confirmed position was not recorded (S5).
	CoordinatorCycleCommittedBeforeRecord Point = "coordinator.cycle-committed-before-record"
)

// Points lists every boundary, for tests and for validating an armed name.
var Points = []Point{
	WorkerBatchReceived,
	WorkerCommitBefore,
	IcebergUpsertBetweenDeleteAndAppend,
	WorkerCommittedBeforeAck,
	CoordinatorAckBeforeRecord,
	WorkerStagedBeforeShip,
	WorkerStagedShippedBeforeAck,
	CoordinatorCycleBeforeCommit,
	CoordinatorCycleCommittedBeforeRecord,
}

// DefaultFile is where an enabled binary looks for the arm file.
const DefaultFile = "/tmp/urutau-fault"

// At marks that the process reached boundary p. kv is the call site's
// context as alternating key, value pairs; "table" is also what an arm
// file's table= filter matches. In a binary built without the faultinject
// tag it does nothing.
func At(p Point, kv ...any) {
	if !enabled {
		return
	}
	path := os.Getenv("URUTAU_FAULT_FILE")
	if path == "" {
		path = DefaultFile
	}
	global.hit(path, p, kv, os.Stderr, kill)
}

// arm is a parsed arm file.
type arm struct {
	point Point
	table string
	skip  int
}

// parseArm reads an arm file's key=value lines. Blank lines and lines
// starting with # are ignored; an unknown key or point is an error, so a typo
// fails loudly instead of silently never firing.
func parseArm(r io.Reader) (arm, error) {
	var a arm
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			return arm{}, fmt.Errorf("faultinject: line %q is not key=value", line)
		}
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		switch k {
		case "point":
			a.point = Point(v)
		case "table":
			a.table = v
		case "skip":
			n, err := strconv.Atoi(v)
			if err != nil || n < 0 {
				return arm{}, fmt.Errorf("faultinject: skip %q is not a non-negative integer", v)
			}
			a.skip = n
		default:
			return arm{}, fmt.Errorf("faultinject: unknown key %q", k)
		}
	}
	if err := sc.Err(); err != nil {
		return arm{}, err
	}
	if a.point == "" {
		return arm{}, errors.New("faultinject: point= is required")
	}
	if !known(a.point) {
		return arm{}, fmt.Errorf("faultinject: unknown point %q", a.point)
	}
	return a, nil
}

func known(p Point) bool {
	for _, q := range Points {
		if q == p {
			return true
		}
	}
	return false
}

// state counts the matching hits of the current arm file, for skip=. A
// changed arm file restarts the count.
type state struct {
	mu    sync.Mutex
	armed string
	hits  int
}

var global state

// hit is At's logic with its effects injected, so tests run it in any build.
// It fires (removes the file, reports, calls die) when the arm file names p,
// the table matches, and skip hits have already passed.
func (s *state) hit(path string, p Point, kv []any, out io.Writer, die func()) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			_, _ = fmt.Fprintf(out, "urutau: faultinject: read %s: %v\n", path, err)
		}
		return
	}
	a, err := parseArm(strings.NewReader(string(raw)))
	if err != nil {
		// A malformed arm file must be visible, not a fault that never fires.
		_, _ = fmt.Fprintf(out, "urutau: faultinject: %s: %v\n", path, err)
		return
	}
	if a.point != p {
		return
	}
	if a.table != "" && a.table != kvString(kv, "table") {
		return
	}

	s.mu.Lock()
	if s.armed != string(raw) {
		s.armed, s.hits = string(raw), 0
	}
	s.hits++
	fire := s.hits > a.skip
	s.mu.Unlock()
	if !fire {
		return
	}

	// One-shot: remove before dying, so the restarted process is not armed.
	_ = os.Remove(path)
	_, _ = fmt.Fprintf(out, "urutau: FAULT INJECTED point=%s%s\n", p, formatKV(kv))
	if f, ok := out.(interface{ Sync() error }); ok {
		_ = f.Sync()
	}
	die()
}

// kvString returns the value of key in kv as a string, or "".
func kvString(kv []any, key string) string {
	for i := 0; i+1 < len(kv); i += 2 {
		if k, ok := kv[i].(string); ok && k == key {
			return fmt.Sprint(kv[i+1])
		}
	}
	return ""
}

// formatKV renders kv as " key=value" pairs; an odd trailing value is kept
// under "!BADKEY" rather than dropped.
func formatKV(kv []any) string {
	var b strings.Builder
	for i := 0; i < len(kv); i += 2 {
		if i+1 >= len(kv) {
			fmt.Fprintf(&b, " !BADKEY=%v", kv[i])
			break
		}
		fmt.Fprintf(&b, " %v=%v", kv[i], kv[i+1])
	}
	return b.String()
}

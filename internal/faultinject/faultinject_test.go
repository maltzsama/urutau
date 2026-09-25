package faultinject

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fire runs one hit against an arm file holding content (or no file when
// content is ""), and reports whether it fired and what it wrote.
func fire(t *testing.T, s *state, content string, p Point, kv ...any) (fired bool, out string, fileLeft bool) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "arm")
	if content != "" {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var buf bytes.Buffer
	s.hit(path, p, kv, &buf, func() { fired = true })
	_, err := os.Stat(path)
	return fired, buf.String(), err == nil
}

func TestHitFiresOnArmedPointAndIsOneShot(t *testing.T) {
	var s state
	fired, out, left := fire(t, &s, "point=worker.committed-before-ack\n",
		WorkerCommittedBeforeAck, "table", "raw.orders", "seq", uint64(42), "position", "u:1-17")
	if !fired {
		t.Fatal("armed point did not fire")
	}
	if left {
		t.Fatal("arm file must be removed before dying, or the restarted process re-fires")
	}
	want := "urutau: FAULT INJECTED point=worker.committed-before-ack table=raw.orders seq=42 position=u:1-17\n"
	if out != want {
		t.Fatalf("diagnostic line = %q, want %q", out, want)
	}
}

func TestHitNoFileNoFire(t *testing.T) {
	var s state
	fired, out, _ := fire(t, &s, "", WorkerCommittedBeforeAck)
	if fired || out != "" {
		t.Fatalf("no arm file: fired=%v out=%q, want nothing", fired, out)
	}
}

func TestHitOtherPointDoesNotFire(t *testing.T) {
	var s state
	fired, _, left := fire(t, &s, "point=worker.committed-before-ack\n", WorkerBatchReceived)
	if fired || !left {
		t.Fatalf("a different point fired=%v fileLeft=%v, want no fire and the file kept", fired, left)
	}
}

func TestHitTableFilter(t *testing.T) {
	arm := "point=worker.commit-before\ntable=raw.orders\n"
	var s state
	if fired, _, _ := fire(t, &s, arm, WorkerCommitBefore, "table", "raw.payments"); fired {
		t.Fatal("fired for a table the arm file does not name")
	}
	if fired, _, _ := fire(t, &s, arm, WorkerCommitBefore, "table", "raw.orders"); !fired {
		t.Fatal("did not fire for the named table")
	}
}

func TestHitSkipLetsHitsPass(t *testing.T) {
	path := filepath.Join(t.TempDir(), "arm")
	if err := os.WriteFile(path, []byte("point=worker.batch-received\nskip=2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var s state
	fires := 0
	for i := 0; i < 3; i++ {
		s.hit(path, WorkerBatchReceived, nil, &bytes.Buffer{}, func() { fires++ })
	}
	if fires != 1 {
		t.Fatalf("skip=2 over 3 hits fired %d times, want once (on the third)", fires)
	}
}

func TestHitNewArmFileRestartsSkipCount(t *testing.T) {
	path := filepath.Join(t.TempDir(), "arm")
	var s state
	fires := 0
	die := func() { fires++ }
	_ = os.WriteFile(path, []byte("point=worker.batch-received\nskip=1\n"), 0o600)
	s.hit(path, WorkerBatchReceived, nil, &bytes.Buffer{}, die) // skipped
	_ = os.WriteFile(path, []byte("point=worker.batch-received\nskip=1\ntable=t\n"), 0o600)
	s.hit(path, WorkerBatchReceived, []any{"table", "t"}, &bytes.Buffer{}, die) // new arm: skipped again
	if fires != 0 {
		t.Fatalf("a rewritten arm file must restart the skip count; fired %d times", fires)
	}
}

func TestHitMalformedArmIsReportedNotFired(t *testing.T) {
	for _, arm := range []string{
		"point=no.such.point\n",
		"table=raw.orders\n",
		"point=worker.batch-received\nskip=-1\n",
		"point=worker.batch-received\nbogus=1\n",
		"point worker.batch-received\n",
	} {
		var s state
		fired, out, _ := fire(t, &s, arm, WorkerBatchReceived)
		if fired {
			t.Errorf("malformed arm %q fired", arm)
		}
		if !strings.Contains(out, "faultinject") {
			t.Errorf("malformed arm %q was not reported: %q", arm, out)
		}
	}
}

func TestParseArmIgnoresCommentsAndBlanks(t *testing.T) {
	a, err := parseArm(strings.NewReader("# arm\n\n point = coordinator.cycle-before-commit \n"))
	if err != nil {
		t.Fatal(err)
	}
	if a.point != CoordinatorCycleBeforeCommit {
		t.Fatalf("point = %q", a.point)
	}
}

func TestFormatKVOddLength(t *testing.T) {
	if got := formatKV([]any{"table", "t", "orphan"}); got != " table=t !BADKEY=orphan" {
		t.Fatalf("formatKV = %q", got)
	}
}

// TestAtIsInertWithoutTag guards the production contract: in a build without
// the faultinject tag, an armed file must not fire (and At must not even
// read it). The test binary itself is such a build unless -tags faultinject
// is passed.
func TestAtIsInertWithoutTag(t *testing.T) {
	if enabled {
		t.Skip("built with -tags faultinject")
	}
	path := filepath.Join(t.TempDir(), "arm")
	if err := os.WriteFile(path, []byte("point=worker.batch-received\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("URUTAU_FAULT_FILE", path)
	At(WorkerBatchReceived, "table", "t") // would SIGKILL the test binary if live
	if _, err := os.Stat(path); err != nil {
		t.Fatal("At consumed the arm file in a build without the tag")
	}
}

package iceberg

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"

	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/spec"
)

// The compile-time assertions in sink.go already prove *Sink implements
// sink.Maintainable and *Maintainer implements sink.Maintainer. This proves
// it at the value the orchestration actually type-asserts against — a
// sink.Sink interface value, the exact shape runner.go/coordinator.go hold.
func TestSinkSatisfiesMaintainableThroughTheInterface(t *testing.T) {
	var s sink.Sink = &Sink{}
	if _, ok := s.(sink.Maintainable); !ok {
		t.Error("iceberg.Sink held as a sink.Sink must still assert to sink.Maintainable — the orchestration's exact check")
	}
}

// discardLogger is a *slog.Logger that writes nowhere — the tests assert on
// behavior (call counts, failure thresholds), not log output.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(discardWriter{}, nil))
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// A nil CompactionConfig must fall through to iceberg-go's own
// compaction.DefaultConfig() untouched.
func TestCompactionConfigFromDefaults(t *testing.T) {
	cfg := compactionConfigFrom(nil)
	def := compaction.DefaultConfig()
	if cfg.TargetFileSizeBytes != def.TargetFileSizeBytes || cfg.MinInputFiles != def.MinInputFiles {
		t.Errorf("nil config = %+v, want iceberg-go's own default %+v", cfg, def)
	}
}

func TestCompactionConfigFromTargetFileSize(t *testing.T) {
	cfg := compactionConfigFrom(&spec.CompactionConfig{TargetFileSize: "128Mi"})
	want := int64(128 * 1024 * 1024)
	if cfg.TargetFileSizeBytes != want {
		t.Fatalf("TargetFileSizeBytes = %d, want %d", cfg.TargetFileSizeBytes, want)
	}
	// The 75%/180% ratios mirror cmd/iceberg/compact.go's own derivation
	// from a custom target size, so a spec-driven target size gets the same
	// min/max bounds a CLI-driven one would.
	if wantMin := want * 3 / 4; cfg.MinFileSizeBytes != wantMin {
		t.Errorf("MinFileSizeBytes = %d, want %d (75%% of target)", cfg.MinFileSizeBytes, wantMin)
	}
	if wantMax := want * 9 / 5; cfg.MaxFileSizeBytes != wantMax {
		t.Errorf("MaxFileSizeBytes = %d, want %d (180%% of target)", cfg.MaxFileSizeBytes, wantMax)
	}
}

// A malformed TargetFileSize must not reach here in practice (Validate
// rejects it first), but compactionConfigFrom must not panic or produce a
// zero/negative size if it somehow does — it silently keeps the default.
func TestCompactionConfigFromBadTargetFileSizeKeepsDefault(t *testing.T) {
	def := compactionConfigFrom(nil)
	cfg := compactionConfigFrom(&spec.CompactionConfig{TargetFileSize: "not-a-size"})
	if cfg.TargetFileSizeBytes != def.TargetFileSizeBytes {
		t.Errorf("bad TargetFileSize changed the default: got %d, want %d", cfg.TargetFileSizeBytes, def.TargetFileSizeBytes)
	}
}

func TestCompactionConfigFromMinInputFiles(t *testing.T) {
	cfg := compactionConfigFrom(&spec.CompactionConfig{MinInputFiles: 10})
	if cfg.MinInputFiles != 10 {
		t.Fatalf("MinInputFiles = %d, want 10", cfg.MinInputFiles)
	}
}

func TestDurationOr(t *testing.T) {
	if got := durationOr("", 5*time.Minute); got != 5*time.Minute {
		t.Errorf("empty string: got %v, want default", got)
	}
	if got := durationOr("not-a-duration", 5*time.Minute); got != 5*time.Minute {
		t.Errorf("malformed: got %v, want default", got)
	}
	if got := durationOr("0s", 5*time.Minute); got != 5*time.Minute {
		t.Errorf("zero duration: got %v, want default (a zero interval would busy-loop)", got)
	}
	if got := durationOr("10m", 5*time.Minute); got != 10*time.Minute {
		t.Errorf("valid: got %v, want 10m", got)
	}
}

func TestIntOr(t *testing.T) {
	if got := intOr(0, 1); got != 1 {
		t.Errorf("zero: got %d, want default", got)
	}
	if got := intOr(-1, 1); got != 1 {
		t.Errorf("negative: got %d, want default", got)
	}
	if got := intOr(3, 1); got != 3 {
		t.Errorf("positive: got %d, want 3", got)
	}
}

func TestIdentString(t *testing.T) {
	if got := identString(table.Identifier{"raw", "orders"}); got != "raw.orders" {
		t.Errorf("got %q, want %q", got, "raw.orders")
	}
	if got := identString(table.Identifier{"solo"}); got != "solo" {
		t.Errorf("got %q, want %q", got, "solo")
	}
}

// Run must not start a ticker for a disabled/nil Maintenance, or for a
// nil sub-config — a table with only SnapshotExpiry set must never touch
// compaction or orphan cleanup. Proven by giving Run a run func that would
// panic if called, and cancelling ctx immediately: Run must return promptly
// (nothing to wait on) rather than hang on a ticker for an operation that
// should never have started.
func TestRunSkipsDisabledOperations(t *testing.T) {
	m := &Maintainer{cfg: spec.Maintenance{Enabled: false}, log: discardLogger()}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() { m.Run(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return promptly for a disabled Maintenance")
	}
}

// runTicker fires on the ticker, not before — and stops immediately when
// ctx is cancelled, without waiting out a long interval.
func TestRunTickerFiresAndStopsOnCancel(t *testing.T) {
	m := &Maintainer{log: discardLogger()}
	var calls atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	failures := 0
	go func() {
		m.runTicker(ctx, "test", 10*time.Millisecond, func(context.Context) error {
			calls.Add(1)
			return nil
		}, &failures)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond) // several ticks at 10ms
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runTicker did not stop after ctx cancellation")
	}
	if calls.Load() < 2 {
		t.Errorf("expected multiple ticks in 50ms at a 10ms interval, got %d", calls.Load())
	}
}

// After maxConsecutiveFailures consecutive failures, runTicker stops on its
// own — it must not spam a persistently broken catalog forever.
func TestRunTickerStopsAfterConsecutiveFailures(t *testing.T) {
	m := &Maintainer{log: discardLogger()}
	var calls atomic.Int32
	failures := 0
	done := make(chan struct{})

	go func() {
		m.runTicker(context.Background(), "test", 5*time.Millisecond, func(context.Context) error {
			calls.Add(1)
			return errors.New("boom")
		}, &failures)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runTicker did not stop after consecutive failures")
	}
	if got := calls.Load(); got != int32(maxConsecutiveFailures) {
		t.Errorf("run() called %d times, want exactly maxConsecutiveFailures (%d)", got, maxConsecutiveFailures)
	}
}

// A success resets the failure counter — an intermittent failure must not
// accumulate toward the stop threshold across unrelated successful ticks.
func TestRunTickerResetsFailuresOnSuccess(t *testing.T) {
	m := &Maintainer{log: discardLogger()}
	var calls atomic.Int32
	failures := 0
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		m.runTicker(ctx, "test", 5*time.Millisecond, func(context.Context) error {
			n := calls.Add(1)
			// Fail, succeed, fail, succeed, ... — never two failures in a
			// row, so the threshold must never trip within this run.
			if n%2 == 1 {
				return errors.New("intermittent")
			}
			return nil
		}, &failures)
		close(done) // happens-before: failures is safe to read once this fires
	}()

	time.Sleep(80 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runTicker did not stop after ctx cancellation")
	}

	if calls.Load() < int32(maxConsecutiveFailures)+2 {
		t.Skip("not enough ticks landed in the window to distinguish reset from no-reset; flaky under load")
	}
	// If failures reset on success, run() keeps being called past
	// maxConsecutiveFailures total invocations (interleaved fail/success
	// never accumulates two in a row). If it did NOT reset, calls would
	// have stopped at exactly maxConsecutiveFailures failing calls.
	if failures >= maxConsecutiveFailures {
		t.Errorf("failures counter = %d at end, want < %d (a success must reset it)", failures, maxConsecutiveFailures)
	}
}

// THE SCENARIO THIS FILE EXISTS TO PIN: a long-running operation (a
// compaction pass over many groups can legitimately take minutes) must not
// make its sibling operation fail repeatedly against it. Before runMu, two
// operations on the same Maintainer had no coordination at all and would
// race the catalog's branch pointer directly — this proves they now queue
// instead: a slow op1 blocks op2's tick until op1 releases runMu, and op2
// runs exactly once immediately after, not zero times (queued has no
// effect) and not with a failure recorded (raced would fail then retry).
func TestRunTickerQueuesBehindSiblingOperation(t *testing.T) {
	m := &Maintainer{log: discardLogger()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	op1Started := make(chan struct{})
	op1Release := make(chan struct{})
	var op1Calls, op2Calls atomic.Int32
	var op1Failures, op2Failures int

	done1 := make(chan struct{})
	go func() {
		m.runTicker(ctx, "op1", 10*time.Millisecond, func(context.Context) error {
			if op1Calls.Add(1) == 1 {
				close(op1Started)
				<-op1Release // hold runMu until the test says to release
			}
			return nil
		}, &op1Failures)
		close(done1)
	}()

	<-op1Started // op1 now holds runMu and is blocked inside its run func

	done2 := make(chan struct{})
	go func() {
		m.runTicker(ctx, "op2", 10*time.Millisecond, func(context.Context) error {
			op2Calls.Add(1)
			return nil
		}, &op2Failures)
		close(done2)
	}()

	// op2's ticker fires repeatedly while op1 holds the lock; every one of
	// those ticks must queue on lockOrDone, not run and not fail.
	time.Sleep(60 * time.Millisecond)
	if got := op2Calls.Load(); got != 0 {
		t.Fatalf("op2 ran %d times while op1 held the lock, want 0 (it must queue, not race)", got)
	}
	if op2Failures != 0 {
		t.Fatalf("op2 recorded %d failures while queued, want 0 (queueing must not look like a failed attempt)", op2Failures)
	}

	close(op1Release) // let op1 finish and release runMu

	// op2 must now run — poll rather than sleep-and-hope, since the exact
	// moment op1 releases and op2's next tick lands is not deterministic.
	deadline := time.Now().Add(2 * time.Second)
	for op2Calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := op2Calls.Load(); got == 0 {
		t.Fatal("op2 never ran after op1 released the lock")
	}

	cancel()
	for _, d := range []chan struct{}{done1, done2} {
		select {
		case <-d:
		case <-time.After(2 * time.Second):
			t.Fatal("a runTicker goroutine did not stop after cancellation")
		}
	}
}

// A tick queued behind a sibling must not block shutdown: cancelling ctx
// while lockOrDone is waiting must return promptly, not hang until the
// sibling eventually releases the lock.
func TestRunTickerCancelWhileQueued(t *testing.T) {
	m := &Maintainer{log: discardLogger()}
	ctx, cancel := context.WithCancel(context.Background())

	// Hold runMu directly (not via runTicker) so nothing ever releases it
	// during this test — the only way out for the queued ticker is ctx
	// cancellation, which is exactly what is under test.
	m.runMu.Lock()
	t.Cleanup(func() { m.runMu.Unlock() })

	failures := 0
	done := make(chan struct{})
	go func() {
		m.runTicker(ctx, "queued", 5*time.Millisecond, func(context.Context) error {
			t.Error("run() must not be called while queued behind a permanently-held lock")
			return nil
		}, &failures)
		close(done)
	}()

	time.Sleep(30 * time.Millisecond) // let at least one tick queue on lockOrDone
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runTicker did not stop promptly when cancelled while queued behind a held lock")
	}
}

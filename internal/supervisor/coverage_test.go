package supervisor

// Coverage for the StageSupervisor's pure lifecycle logic — backoff, the
// circuit breaker, dead marking — plus the Run arms reachable without a real
// plugin binary (cancelled ctx, open breaker, spawn failure).

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/pipeline"
)

func TestNewStageSupervisorDefaultsLogger(t *testing.T) {
	s := NewStageSupervisor(pipeline.StageConfig{}, nil)
	if s.logger == nil {
		t.Fatal("nil logger must default to slog.Default()")
	}
	if s.Stage() != nil {
		t.Fatal("a fresh supervisor has no stage")
	}
	if err := s.stopStage(context.Background()); err != nil {
		t.Fatalf("stopStage(nil stage) = %v", err)
	}
}

func TestBackoffProgression(t *testing.T) {
	s := NewStageSupervisor(pipeline.StageConfig{}, nil)
	if got := s.currentBackoff(); got != 0 {
		t.Fatalf("initial backoff = %v, want 0", got)
	}

	s.advanceBackoff() // backoff = 1
	d := s.currentBackoff()
	if d < 750*time.Millisecond || d > 1250*time.Millisecond {
		t.Fatalf("backoff(1) = %v, want ~1s with jitter", d)
	}

	s.backoff = 10 // beyond the cap
	capped := s.currentBackoff()
	if capped < 45*time.Second || capped > 75*time.Second {
		t.Fatalf("capped backoff = %v, want ~60s with jitter", capped)
	}

	s.resetBackoff()
	if got := s.currentBackoff(); got != 0 {
		t.Fatalf("backoff after reset = %v, want 0", got)
	}
}

func TestCircuitBreaker(t *testing.T) {
	s := NewStageSupervisor(pipeline.StageConfig{}, nil)
	if s.isCircuitOpen() {
		t.Fatal("a fresh breaker must be closed")
	}
	for range circuitBreakerThreshold {
		s.recordFailure()
	}
	if !s.isCircuitOpen() {
		t.Fatalf("breaker must open after %d failures", circuitBreakerThreshold)
	}

	// Failures outside the window are pruned and do not count.
	old := NewStageSupervisor(pipeline.StageConfig{}, nil)
	old.failures = []time.Time{time.Now().Add(-2 * circuitBreakerWindow)}
	old.recordFailure() // prunes the stale one, adds one fresh
	if old.isCircuitOpen() {
		t.Fatal("a single fresh failure must not open the breaker")
	}
	if len(old.failures) != 1 {
		t.Fatalf("pruned failures = %d, want 1", len(old.failures))
	}
}

func TestMarkDeadIsOnce(t *testing.T) {
	s := NewStageSupervisor(pipeline.StageConfig{}, nil)
	first := errors.New("first")
	s.markDead(first)
	s.markDead(errors.New("second"))

	select {
	case <-s.Dead():
	default:
		t.Fatal("Dead must be closed after markDead")
	}
	if err := s.DeadErr(); !errors.Is(err, first) {
		t.Fatalf("DeadErr = %v, want first", err)
	}
}

func TestRunReturnsOnCancelledContext(t *testing.T) {
	s := NewStageSupervisor(pipeline.StageConfig{}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run(cancelled) = %v, want context.Canceled", err)
	}
}

func TestRunReturnsWhenCircuitOpen(t *testing.T) {
	s := NewStageSupervisor(pipeline.StageConfig{}, nil)
	for range circuitBreakerThreshold {
		s.recordFailure()
	}
	if err := s.Run(context.Background()); err == nil {
		t.Fatal("Run with an open breaker must fail")
	}
	select {
	case <-s.Dead():
	default:
		t.Fatal("an open breaker must mark the supervisor dead")
	}
}

func TestStartOnceReportsSpawnFailure(t *testing.T) {
	s := NewStageSupervisor(pipeline.StageConfig{Bin: "/nonexistent/urutau-plugin"}, nil)
	if err := s.startOnce(context.Background()); err == nil {
		t.Fatal("spawning a missing binary must fail")
	}
}

func TestRunRetriesThenStopsOnDeadline(t *testing.T) {
	s := NewStageSupervisor(pipeline.StageConfig{Bin: "/nonexistent/urutau-plugin"}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := s.Run(ctx)
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want the context error after a failed spawn", err)
	}
	if len(s.failures) == 0 {
		t.Fatal("a failed spawn must be recorded as a failure")
	}
}

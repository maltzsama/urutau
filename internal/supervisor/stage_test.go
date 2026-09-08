package supervisor

import (
	"log/slog"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/pipeline"
)

func TestStageSupervisorBackoff(t *testing.T) {
	s := NewStageSupervisor(pipeline.StageConfig{}, slog.Default())

	// Initial backoff should be 0.
	if s.currentBackoff() != 0 {
		t.Errorf("initial backoff: got %v, want 0", s.currentBackoff())
	}

	// After one failure, backoff should be > 0.
	s.recordFailure()
	s.advanceBackoff()
	b := s.currentBackoff()
	if b <= 0 || b > 2*time.Second {
		t.Errorf("backoff after 1 failure: got %v, want (0, 2s]", b)
	}

	// After more failures, backoff should increase.
	for range 5 {
		s.recordFailure()
		s.advanceBackoff()
	}
	b6 := s.currentBackoff()
	if b6 <= b {
		t.Errorf("backoff should increase: got %v after 6 failures, was %v after 1", b6, b)
	}

	// Reset should bring backoff to 0.
	s.resetBackoff()
	if s.currentBackoff() != 0 {
		t.Errorf("reset backoff: got %v, want 0", s.currentBackoff())
	}
}

func TestStageSupervisorCircuitBreaker(t *testing.T) {
	s := NewStageSupervisor(pipeline.StageConfig{}, slog.Default())

	// Not open initially.
	if s.isCircuitOpen() {
		t.Error("circuit should not be open initially")
	}

	// Record failures within window.
	for range circuitBreakerThreshold {
		s.recordFailure()
	}

	// Should be open now.
	if !s.isCircuitOpen() {
		t.Error("circuit should be open after threshold failures")
	}
}

func TestStageSupervisorDead(t *testing.T) {
	s := NewStageSupervisor(pipeline.StageConfig{}, slog.Default())

	// Dead channel should not be closed initially.
	select {
	case <-s.Dead():
		t.Error("Dead channel should not be closed initially")
	default:
	}

	// Mark dead.
	s.markDead(someError{})

	// Dead channel should be closed now.
	select {
	case <-s.Dead():
	case <-time.After(time.Second):
		t.Error("Dead channel should be closed after markDead")
	}

	// DeadErr should return the error.
	if s.DeadErr() == nil {
		t.Error("DeadErr should return non-nil error")
	}
}

func TestPipelineSupervisorHealth(t *testing.T) {
	p := NewPipelineSupervisor(PipelineConfig{}, slog.Default())

	h := p.Health()
	// Before any stage is started, health reports unhealthy (stage is nil).
	if h.SourceHealthy {
		t.Error("source should not be healthy before start")
	}
	if h.SinkHealthy {
		t.Error("sink should not be healthy before start")
	}
	// But no errors yet.
	if h.SourceErr != nil {
		t.Errorf("source error: %v", h.SourceErr)
	}
	if h.SinkErr != nil {
		t.Errorf("sink error: %v", h.SinkErr)
	}
}

type someError struct{}

func (someError) Error() string { return "test error" }

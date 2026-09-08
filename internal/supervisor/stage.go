// Package supervisor manages external plugin subprocesses with restart
// backoff, circuit breaker protection, and graceful shutdown. It owns the
// full lifecycle: spawn → connect → run → restart → shutdown.
package supervisor

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/maltzsama/urutau/internal/pipeline"
)

const (
	minBackoff = 1 * time.Second
	maxBackoff = 60 * time.Second
	jitterPct  = 0.25

	// circuitBreakerThreshold is the number of failures within the
	// window that trips the breaker.
	circuitBreakerThreshold = 3
	// circuitBreakerWindow is the rolling window for failure counting.
	circuitBreakerWindow = 5 * time.Minute
)

// StageSupervisor manages the lifecycle of one plugin subprocess with
// exponential backoff and circuit breaker protection.
type StageSupervisor struct {
	cfg    pipeline.StageConfig
	logger *slog.Logger

	mu       sync.Mutex
	stage    *pipeline.Stage
	failures []time.Time // timestamps of recent failures
	backoff  int         // current backoff exponent (0 = no delay)

	dead     chan struct{}
	deadErr  error
	deadOnce sync.Once
}

// NewStageSupervisor creates a supervisor for one plugin stage.
func NewStageSupervisor(cfg pipeline.StageConfig, logger *slog.Logger) *StageSupervisor {
	if logger == nil {
		logger = slog.Default()
	}
	return &StageSupervisor{
		cfg:    cfg,
		logger: logger,
		dead:   make(chan struct{}),
	}
}

// Dead returns a channel closed when the stage becomes unhealthy
// (circuit breaker tripped or non-recoverable error).
func (s *StageSupervisor) Dead() <-chan struct{} { return s.dead }

// DeadErr returns the reason the stage died.
func (s *StageSupervisor) DeadErr() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deadErr
}

// Stage returns the current pipeline.Stage (may be nil before first start
// or after shutdown).
func (s *StageSupervisor) Stage() *pipeline.Stage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stage
}

// Run starts the plugin and keeps it alive. It blocks until the context
// is cancelled, the circuit breaker trips, or a non-recoverable error
// occurs. On each failure, it applies exponential backoff with jitter
// before retrying.
func (s *StageSupervisor) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if s.isCircuitOpen() {
			err := fmt.Errorf("circuit breaker open: %d failures in %s", circuitBreakerThreshold, circuitBreakerWindow)
			s.markDead(err)
			return err
		}

		delay := s.currentBackoff()
		if delay > 0 {
			s.logger.Info("waiting before restart", "delay", delay)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}

		if err := s.startOnce(ctx); err != nil {
			s.recordFailure()
			s.logger.Error("plugin stage failed", "err", err, "bin", s.cfg.Bin)
			s.advanceBackoff()
			continue
		}

		// Stage is running. Wait for it to die or be stopped.
		s.resetBackoff()
		select {
		case <-ctx.Done():
			return s.stopStage(context.Background())
		case <-s.stage.Exited():
			err := fmt.Errorf("plugin exited unexpectedly")
			s.recordFailure()
			s.logger.Error("plugin exited", "err", err, "bin", s.cfg.Bin)
			s.advanceBackoff()
		}
	}
}

// startOnce spawns the plugin process, connects the Flight client, and
// starts the heartbeat loop. It does NOT retry — the caller handles retry.
func (s *StageSupervisor) startOnce(ctx context.Context) error {
	stage, err := pipeline.Spawn(ctx, s.cfg)
	if err != nil {
		return fmt.Errorf("spawn: %w", err)
	}

	if err := stage.Connect(ctx); err != nil {
		_ = stage.Stop(context.Background())
		return fmt.Errorf("connect: %w", err)
	}

	s.mu.Lock()
	s.stage = stage
	s.mu.Unlock()

	s.logger.Info("plugin stage started", "bin", s.cfg.Bin)
	return nil
}

func (s *StageSupervisor) stopStage(ctx context.Context) error {
	s.mu.Lock()
	stg := s.stage
	s.mu.Unlock()
	if stg == nil {
		return nil
	}
	return stg.Stop(ctx)
}

// Shutdown gracefully stops the plugin.
func (s *StageSupervisor) Shutdown(ctx context.Context) error {
	return s.stopStage(ctx)
}

// ── backoff ────────────────────────────────────────────────────────────

func (s *StageSupervisor) currentBackoff() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.backoff == 0 {
		return 0
	}
	base := float64(minBackoff) * math.Pow(2, float64(s.backoff-1))
	if base > float64(maxBackoff) {
		base = float64(maxBackoff)
	}
	jitter := base * jitterPct * (rand.Float64()*2 - 1)
	return time.Duration(base + jitter)
}

func (s *StageSupervisor) advanceBackoff() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.backoff++
}

func (s *StageSupervisor) resetBackoff() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.backoff = 0
}

// ── circuit breaker ────────────────────────────────────────────────────

func (s *StageSupervisor) recordFailure() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failures = append(s.failures, time.Now())
	// Prune old failures outside the window.
	cutoff := time.Now().Add(-circuitBreakerWindow)
	n := 0
	for _, t := range s.failures {
		if t.After(cutoff) {
			s.failures[n] = t
			n++
		}
	}
	s.failures = s.failures[:n]
}

func (s *StageSupervisor) isCircuitOpen() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	cutoff := time.Now().Add(-circuitBreakerWindow)
	count := 0
	for _, t := range s.failures {
		if t.After(cutoff) {
			count++
		}
	}
	return count >= circuitBreakerThreshold
}

func (s *StageSupervisor) markDead(err error) {
	s.deadOnce.Do(func() {
		s.mu.Lock()
		s.deadErr = err
		s.mu.Unlock()
		close(s.dead)
	})
}

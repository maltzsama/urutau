package supervisor

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/maltzsama/urutau/internal/pipeline"
)

// PipelineSupervisor manages all plugin subprocesses for a pipeline. It
// spawns source and sink stages, monitors their health, and orchestrates
// graceful shutdown when any stage dies or the context is cancelled.
type PipelineSupervisor struct {
	source *StageSupervisor
	sink   *StageSupervisor
	logger *slog.Logger
	stages []*StageSupervisor

	readyOnce sync.Once
	ready     chan struct{} // closed when both stages are connected
}

// PipelineConfig carries the complete configuration for a pipeline's
// external plugin stages.
type PipelineConfig struct {
	Source pipeline.StageConfig
	Sink   pipeline.StageConfig
}

// NewPipelineSupervisor creates a supervisor for a pipeline with one
// source and one sink plugin stage.
func NewPipelineSupervisor(cfg PipelineConfig, logger *slog.Logger) *PipelineSupervisor {
	if logger == nil {
		logger = slog.Default()
	}
	src := NewStageSupervisor(cfg.Source, logger)
	snk := NewStageSupervisor(cfg.Sink, logger)
	return &PipelineSupervisor{
		source: src,
		sink:   snk,
		logger: logger,
		stages: []*StageSupervisor{src, snk},
		ready:  make(chan struct{}),
	}
}

// Source returns the source stage supervisor.
func (p *PipelineSupervisor) Source() *StageSupervisor { return p.source }

// Sink returns the sink stage supervisor.
func (p *PipelineSupervisor) Sink() *StageSupervisor { return p.sink }

// Run starts all stages and blocks until any stage dies or the context
// is cancelled. When any stage fails, all other stages are shut down.
func (p *PipelineSupervisor) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Start readiness monitor.
	go p.monitorReady(ctx)

	// Start all stages concurrently.
	errCh := make(chan error, len(p.stages))
	for _, s := range p.stages {
		go func(stg *StageSupervisor) {
			errCh <- stg.Run(ctx)
		}(s)
	}

	// Wait for the first stage to finish (death or shutdown).
	var firstErr error
	for range p.stages {
		select {
		case err := <-errCh:
			if firstErr == nil {
				firstErr = err
			}
			cancel()
		case <-ctx.Done():
		}
	}

	// Graceful shutdown with timeout.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	p.shutdownAll(shutdownCtx)

	return firstErr
}

// monitorReady watches both stages and closes the ready channel once
// both have a connected client.
func (p *PipelineSupervisor) monitorReady(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if p.source.Stage() != nil && p.source.Stage().Client != nil &&
				p.sink.Stage() != nil && p.sink.Stage().Client != nil {
				p.readyOnce.Do(func() { close(p.ready) })
				return
			}
		}
	}
}

// shutdownAll stops all stages. Errors are logged but do not propagate.
func (p *PipelineSupervisor) shutdownAll(ctx context.Context) {
	var wg sync.WaitGroup
	for _, s := range p.stages {
		wg.Add(1)
		go func(stg *StageSupervisor) {
			defer wg.Done()
			if err := stg.Shutdown(ctx); err != nil {
				p.logger.Warn("stage shutdown error", "err", err)
			}
		}(s)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		p.logger.Info("all stages shut down")
	case <-ctx.Done():
		p.logger.Warn("shutdown timed out")
	}
}

// Wait blocks until the pipeline context is done or any stage dies.
// It returns the first error encountered.
func (p *PipelineSupervisor) Wait(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, len(p.stages))
	for _, s := range p.stages {
		go func(stg *StageSupervisor) {
			<-stg.Dead()
			errCh <- stg.DeadErr()
		}(s)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return fmt.Errorf("stage died: %w", err)
	}
}

// StageHealth reports the health status of all stages.
type StageHealth struct {
	SourceHealthy bool
	SourceErr     error
	SinkHealthy   bool
	SinkErr       error
}

// Health returns the current health of all stages.
// A stage is considered healthy if it has been started and has not died.
func (p *PipelineSupervisor) Health() StageHealth {
	return StageHealth{
		SourceHealthy: p.source.Stage() != nil && p.source.DeadErr() == nil,
		SourceErr:     p.source.DeadErr(),
		SinkHealthy:   p.sink.Stage() != nil && p.sink.DeadErr() == nil,
		SinkErr:       p.sink.DeadErr(),
	}
}

// WaitForReady blocks until both source and sink stages are connected,
// or returns an error if the context is cancelled.
func (p *PipelineSupervisor) WaitForReady(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.ready:
		return nil
	}
}

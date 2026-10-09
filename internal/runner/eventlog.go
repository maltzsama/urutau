package runner

import (
	"context"

	"github.com/maltzsama/urutau/internal/eventlog"
	"github.com/maltzsama/urutau/spec"
)

// openEventlog starts the audit trail (if configured): job_started marks the
// boot, and a startup failure still seals the trail with job_stopped. The
// returned onFail emits that seal; call it only when the runner is nil (boot
// failed). Returns a nil run and onFail when logging is off.
func openEventlog(ctx context.Context, s *spec.Spec, cfg Config) (*eventlog.Run, func(), error) {
	if cfg.Eventlog == nil {
		return nil, nil, nil
	}
	ec := *cfg.Eventlog
	// Apply the shared key convention: the trail lives under the pipeline
	// name so a reader can discover it by listing.
	if ec.Pipeline == "" {
		ec.Pipeline = s.Pipeline
	}
	ev, err := eventlog.New(ctx, ec)
	if err != nil {
		return nil, nil, err
	}
	_ = ev.Emit(ctx, eventlog.KindJobStarted, map[string]any{
		"pipeline": s.Pipeline, "source": s.Source.Kind, "tables": len(s.Tables),
	})
	// Seal the run's first event now: Emit only enqueues, and a crash before
	// the flusher's next tick must still leave job_started in the trail.
	_ = ev.Flush(ctx)
	onFail := func() {
		_ = ev.Emit(ctx, eventlog.KindJobStopped, map[string]any{"reason": "startup_failed"})
		_ = ev.Close()
	}
	return ev, onFail, nil
}

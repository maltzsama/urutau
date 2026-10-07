package worker

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/maltzsama/urutau/internal/observability"
)

// startMetrics serves /metrics when configured. The server handle is kept so
// stopMetrics can shut it down instead of leaking the listener, and a serve
// error is logged instead of silently swallowed (issue #559).
func (w *Worker) startMetrics() {
	if w.cfg.MetricsAddr == "" {
		return
	}
	srv := observability.NewServer(w.cfg.MetricsAddr, w.metrics.Handler(nil))
	w.metricsSrv = srv
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Default().Error("worker: metrics server", "addr", w.cfg.MetricsAddr, "err", err)
		}
	}()
}

// stopMetrics shuts the /metrics server down, if one was started.
func (w *Worker) stopMetrics() {
	if w.metricsSrv == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = w.metricsSrv.Shutdown(ctx)
}

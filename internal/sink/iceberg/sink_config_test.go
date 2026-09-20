package iceberg

import (
	"context"
	"log/slog"
	"testing"
	"time"
)

// captureHandler records log messages so the fallback warnings can be asserted.
type captureHandler struct{ msgs []string }

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.msgs = append(h.msgs, r.Message)
	return nil
}
func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(string) slog.Handler      { return h }

// #199: an invalid (non-empty) value is replaced by the default AND logged; an
// empty value is unset and silent.
func TestConfigFallbackWarnsOnInvalid(t *testing.T) {
	h := &captureHandler{}
	log := slog.New(h)

	if got := durationOr("not-a-duration", time.Minute, log, "maxAge"); got != time.Minute {
		t.Fatalf("invalid duration = %v, want the default", got)
	}
	if len(h.msgs) == 0 {
		t.Fatal("an invalid duration must warn")
	}

	h.msgs = nil
	if got := durationOr("", time.Minute, log, "maxAge"); got != time.Minute {
		t.Fatalf("empty duration = %v, want the default", got)
	}
	if len(h.msgs) != 0 {
		t.Fatalf("an empty duration must not warn, got %v", h.msgs)
	}

	h.msgs = nil
	if got := intOr(-1, 5, log, "retainLast"); got != 5 {
		t.Fatalf("negative int = %d, want the default", got)
	}
	if len(h.msgs) == 0 {
		t.Fatal("a negative value must warn")
	}
}

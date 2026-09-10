package coordinator

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/maltzsama/urutau/core"
)

// TestSurfaceWarnings proves advisory warnings from introspection and cast
// resolution are surfaced at boot, not swallowed — the coordinator mirror of
// the runner's logging (FIX-INSTR).
func TestSurfaceWarnings(t *testing.T) {
	var buf bytes.Buffer
	c := &Coordinator{log: slog.New(slog.NewTextHandler(&buf, nil))}

	c.surfaceWarnings("t", []core.Warning{{Message: "advisory one"}, {Message: "advisory two"}})

	out := buf.String()
	if !strings.Contains(out, "advisory one") || !strings.Contains(out, "advisory two") {
		t.Fatalf("warnings not surfaced: %q", out)
	}
	if !strings.Contains(out, "table=t") || !strings.Contains(out, "warning") {
		t.Fatalf("warning attributes missing: %q", out)
	}
}

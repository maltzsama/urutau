package builtin

import (
	"testing"

	"github.com/maltzsama/urutau/driver"
)

// TestAllDriversRegistered turns "forgot to blank-import a driver in
// builtin.go" into a CI error in the package that owns the imports.
// With init()-side registration a missing import compiles fine and only
// surfaces at runtime as "unknown source kind" on the first pipeline boot —
// this test catches it where the import lives (CR-039 §4.1).
func TestAllDriversRegistered(t *testing.T) {
	for _, kind := range []string{"mysql", "postgres", "kafka"} {
		if _, err := driver.CapsForKind(kind); err != nil {
			t.Errorf("source %q not registered: %v", kind, err)
		}
	}
	if !driver.SinkTypeExists("iceberg+rest") {
		t.Error(`sink "iceberg+rest" not registered`)
	}
}

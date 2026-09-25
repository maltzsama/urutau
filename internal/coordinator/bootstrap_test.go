package coordinator

import (
	"context"
	"strings"
	"testing"

	"github.com/maltzsama/urutau/spec"
)

// The coordinator does not implement tables[].bootstrap (issue #405): every
// block it cannot honor must fail the boot, and only what it already does
// (a snapshot, streaming from the current position) may pass.
func TestRequireSnapshotBootstrap(t *testing.T) {
	cases := []struct {
		name    string
		b       *spec.Bootstrap
		wantErr string
	}{
		{"no bootstrap", nil, ""},
		{"empty block", &spec.Bootstrap{}, ""},
		{"snapshot", &spec.Bootstrap{Mode: spec.BootstrapSnapshot}, ""},
		{"snapshot, startAt current", &spec.Bootstrap{Mode: spec.BootstrapSnapshot, StartAt: spec.StartAtCurrent}, ""},
		{"startAt current", &spec.Bootstrap{StartAt: spec.StartAtCurrent}, ""},
		{"adopt", &spec.Bootstrap{Mode: spec.Adopt}, `bootstrap.mode "adopt"`},
		{"adopt-verify", &spec.Bootstrap{Mode: spec.AdoptVerify}, `bootstrap.mode "adopt-verify"`},
		{"adopt, startAt current", &spec.Bootstrap{Mode: spec.Adopt, StartAt: spec.StartAtCurrent}, `bootstrap.mode "adopt"`},
		{"adopt, startAt explicit", &spec.Bootstrap{Mode: spec.Adopt, StartAt: spec.StartAtExplicit, Position: "uuid:1-10"}, `bootstrap.mode "adopt"`},
		{"snapshot, startAt explicit", &spec.Bootstrap{Mode: spec.BootstrapSnapshot, StartAt: spec.StartAtExplicit, Position: "uuid:1-10"}, `bootstrap.startAt "explicit"`},
	}
	for _, c := range cases {
		err := requireSnapshotBootstrap(spec.Table{Target: "orders", Bootstrap: c.b})
		if c.wantErr == "" {
			if err != nil {
				t.Errorf("%s: unexpected error: %v", c.name, err)
			}
			continue
		}
		if err == nil {
			t.Errorf("%s: want an error containing %s, got nil", c.name, c.wantErr)
			continue
		}
		if !strings.Contains(err.Error(), c.wantErr) || !strings.Contains(err.Error(), "orders") {
			t.Errorf("%s: error %q should name the table and contain %s", c.name, err, c.wantErr)
		}
	}
}

// The check runs before the source is opened, so an unreachable or unknown
// source cannot mask it.
func TestRunRejectsBootstrapBeforeOpeningSource(t *testing.T) {
	c := &Coordinator{cfg: Config{Spec: &spec.Spec{
		Pipeline: "p",
		Source:   spec.Source{Kind: "no-such-source"},
		Tables:   []spec.Table{{Source: "db.orders", Target: "orders", Bootstrap: &spec.Bootstrap{Mode: spec.Adopt}}},
	}}}
	err := c.run(context.Background())
	if err == nil || !strings.Contains(err.Error(), `bootstrap.mode "adopt"`) {
		t.Fatalf("run: want the bootstrap error before any source error, got %v", err)
	}
}

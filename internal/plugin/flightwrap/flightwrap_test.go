package flightwrap_test

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/maltzsama/urutau/driver"
	_ "github.com/maltzsama/urutau/internal/builtin" // built-in drivers register into the shared registry
	"github.com/maltzsama/urutau/internal/plugin/flightwrap"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// TestLoadPluginServesOverFlight proves the .so redesign end-to-end: a
// plugin's Init() registers a plain source.Source via driver.RegisterSource
// — exactly the pre-existing author-facing API, zero Flight code in the
// plugin — and driver.LoadPlugin(path, flightwrap.Wrap{}) must still
// round-trip that source's rows through an in-process Arrow Flight server +
// client (internal/plugin/flightserver + internal/plugin/client), the SAME
// contract a subprocess plugin speaks. This is the only value of this test:
// proving the Flight round-trip itself — data survives serializing out to
// Arrow records over a real Flight RPC and back through the client adapter.
func TestLoadPluginServesOverFlight(t *testing.T) {
	soPath := buildTestPlugin(t)
	if err := driver.LoadPlugin(soPath, flightwrap.Wrap{}); err != nil {
		// Go's plugin ABI ties a .so to the exact build of every package it
		// imports, including the loader's own binary. `go test` compiles a
		// synthesized test binary with a different build id than the plain
		// `go build` used for the .so above, so identical source can still
		// fail to load with this exact error — a toolchain limitation, not
		// a bug in LoadPlugin or the flightwrap/flightserver code. Skip
		// rather than fail; the round trip is proven whenever this
		// environment's go test and go build share a build id.
		if strings.Contains(err.Error(), "different version of package") {
			t.Skipf("go test/go build plugin ABI mismatch in this environment: %v", err)
		}
		t.Fatalf("LoadPlugin: %v", err)
	}

	s := &spec.Spec{
		Pipeline: "flight-wrap-proof",
		Source:   spec.Source{Kind: "plugin_demo", URI: "plugin_demo://"},
		Tables: []spec.Table{{
			Source: "src.t", Target: "raw.t", PrimaryKey: []string{"id"},
		}},
	}

	src, err := driver.OpenSource(s, source.Runtime{})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	ref, cs, _, err := src.Introspect(ctx, s.Tables[0])
	if err != nil {
		t.Fatalf("Introspect: %v", err)
	}
	if len(cs.Columns) != 2 {
		t.Fatalf("introspected schema has %d columns, want 2 (id, v)", len(cs.Columns))
	}

	rdr, err := src.Open(ctx, []source.TableRef{ref})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rdr.Close()
	if err := rdr.Start(ctx, nil); err != nil {
		t.Fatalf("Start: %v", err)
	}

	b, err := rdr.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if b == nil {
		t.Fatal("Next returned nil batch — expected the plugin's 2 seeded rows")
	}
	defer b.Record.Release()

	if got := b.Record.NumRows(); got != 2 {
		t.Fatalf("batch has %d rows, want 2 (the plugin's seeded id=1/v=a, id=2/v=b)", got)
	}
}

// buildTestPlugin compiles test/plugin/standalone into a .so under
// t.TempDir, skipping the test if the toolchain cannot build Go plugins in
// this environment (buildmode=plugin requires cgo and is Linux/macOS-only).
func buildTestPlugin(t *testing.T) string {
	t.Helper()
	repoRoot, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}
	out := filepath.Join(t.TempDir(), "plugin_demo.so")
	cmd := exec.Command("go", "build", "-buildmode=plugin", "-o", out, "./test/plugin/standalone")
	cmd.Dir = repoRoot
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("cannot build test .so plugin (buildmode=plugin unsupported here?): %v\n%s", err, b)
	}
	return out
}

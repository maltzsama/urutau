// Package architecture enforces the dependency walls at test time.
// It checks the DIRECT imports of the contract packages: a wall leaks the
// moment a package imports the other side. `go list -deps` (transitive) is
// too strict — the orchestration legitimately depends on the driver registry,
// which is populated by the concrete implementations only through blank
// imports in the binaries.
package architecture

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// listTimeout bounds every `go list` subprocess so a hung toolchain (network
// proxy, slow module load) fails the test instead of hanging CI.
const listTimeout = 30 * time.Second

// listPackages resolves a package pattern (e.g. ./internal/sink/...) into
// import paths, so the walls auto-cover new drivers instead of drifting from
// a hand-maintained list.
func listPackages(t *testing.T, pattern string) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), listTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "go", "list", pattern).CombinedOutput()
	if err != nil {
		t.Fatalf("go list %s: %v\n%s", pattern, err, out)
	}
	var pkgs []string
	for _, line := range strings.Fields(string(out)) {
		if line != "" {
			pkgs = append(pkgs, line)
		}
	}
	return pkgs
}

// directImports returns the direct import set of pkg (non-test files).
func directImports(t *testing.T, pkg string) map[string]bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), listTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "go", "list", "-f", "{{.Imports}}", pkg).CombinedOutput()
	if err != nil {
		t.Fatalf("go list -f imports %s: %v\n%s", pkg, err, out)
	}
	set := map[string]bool{}
	for _, line := range strings.Fields(string(out)) {
		if line != "" && line != "[" && line != "]" {
			set[strings.Trim(line, "[]")] = true
		}
	}
	return set
}

// TestSourcesNeverKnowSinks: the source packages must not directly import
// any sink or iceberg-go (acceptance §2). Discovered via go list so a new
// source driver is covered without editing this test.
func TestSourcesNeverKnowSinks(t *testing.T) {
	pkgs := append(listPackages(t, "github.com/maltzsama/urutau/internal/source/..."),
		"github.com/maltzsama/urutau/internal/snapshot")
	for _, pkg := range pkgs {
		d := directImports(t, pkg)
		for imp := range d {
			if strings.HasPrefix(imp, "github.com/maltzsama/urutau/sink") ||
				strings.HasPrefix(imp, "github.com/maltzsama/urutau/internal/sink") ||
				strings.HasPrefix(imp, "github.com/apache/iceberg-go") {
				t.Errorf("%s imports %s — sources map to core.Schema", pkg, imp)
			}
		}
	}
}

// TestSinksNeverKnowSources: every sink package must not directly import any
// source or the go-mysql driver (acceptance §3). Discovered via go list so a
// new sink driver is covered.
func TestSinksNeverKnowSources(t *testing.T) {
	for _, pkg := range listPackages(t, "github.com/maltzsama/urutau/internal/sink/...") {
		d := directImports(t, pkg)
		for imp := range d {
			if strings.HasPrefix(imp, "github.com/maltzsama/urutau/source") ||
				strings.HasPrefix(imp, "github.com/maltzsama/urutau/internal/source") ||
				strings.HasPrefix(imp, "github.com/go-mysql-org/go-mysql") {
				t.Errorf("%s imports %s — sinks consume core.Schema", pkg, imp)
			}
		}
	}
}

// TestOrchestrationConsumesContracts: runner, coordinator and worker consume
// only the source/sink/driver contracts — never a concrete source or sink,
// and never iceberg-go. internal/builtin is prohibited too: it blank-imports
// every concrete driver, so importing it would be a backdoor past this wall.
func TestOrchestrationConsumesContracts(t *testing.T) {
	for _, pkg := range []string{
		"github.com/maltzsama/urutau/internal/runner",
		"github.com/maltzsama/urutau/internal/coordinator",
		"github.com/maltzsama/urutau/internal/worker",
	} {
		d := directImports(t, pkg)
		for imp := range d {
			if strings.HasPrefix(imp, "github.com/maltzsama/urutau/internal/source") ||
				strings.HasPrefix(imp, "github.com/maltzsama/urutau/internal/sink") ||
				strings.HasPrefix(imp, "github.com/maltzsama/urutau/internal/builtin") ||
				strings.HasPrefix(imp, "github.com/apache/iceberg-go") {
				t.Errorf("%s imports %s — consume the source/sink/driver contracts", pkg, imp)
			}
		}
	}
}

// TestContractsArePluginSafe: the public contract packages must not import
// anything under internal/ — an external plugin imports these contracts and
// must not transitively pull the engine internals. internal/rowchange is
// deliberately NOT listed: it is the internal row-universe type at the CDC
// decoder boundary, never a public contract (Go forbids external modules
// from importing internal/ at all), so checking it here would be a false
// promise.
func TestContractsArePluginSafe(t *testing.T) {
	for _, pkg := range []string{
		"github.com/maltzsama/urutau/source",
		"github.com/maltzsama/urutau/sink",
		"github.com/maltzsama/urutau/driver",
		"github.com/maltzsama/urutau/core",
		"github.com/maltzsama/urutau/position",
		"github.com/maltzsama/urutau/spec",
		"github.com/maltzsama/urutau/dataplane",
	} {
		d := directImports(t, pkg)
		for imp := range d {
			if strings.HasPrefix(imp, "github.com/maltzsama/urutau/internal/") {
				t.Errorf("%s imports %s — contracts must stay free of engine internals", pkg, imp)
			}
		}
	}
}

// TestPluginPackageImportsOnlyContracts: the reference plugin (test/plugin)
// implements a source and sink using only the public contracts — no
// internal/ import. This is the CI-locked proof that a driver can be written
// outside the engine.
func TestPluginPackageImportsOnlyContracts(t *testing.T) {
	d := directImports(t, "github.com/maltzsama/urutau/test/plugin")
	for imp := range d {
		if strings.HasPrefix(imp, "github.com/maltzsama/urutau/internal/") {
			t.Errorf("test/plugin imports %s — a driver must use only the public contracts", imp)
		}
	}
}

// Package architecture enforces the dependency walls at test time.
// It checks the DIRECT imports of the contract packages: a wall leaks the
// moment a package imports the other side. `go list -deps` (transitive) is
// too strict — the orchestration legitimately depends on the driver registry,
// which is populated by the concrete implementations only through blank
// imports in the binaries.
package architecture

import (
	"os/exec"
	"strings"
	"testing"
)

// directImports returns the direct import set of pkg (non-test files).
func directImports(t *testing.T, pkg string) map[string]bool {
	t.Helper()
	out, err := exec.Command("go", "list", "-f", "{{.Imports}}", pkg).Output()
	if err != nil {
		t.Fatalf("go list -f imports %s: %v", pkg, err)
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
// any sink or iceberg-go (acceptance §2).
func TestSourcesNeverKnowSinks(t *testing.T) {
	for _, pkg := range []string{
		"github.com/maltzsama/urutau/internal/source/mysql",
		"github.com/maltzsama/urutau/internal/source/postgres",
		"github.com/maltzsama/urutau/internal/source/kafka",
		"github.com/maltzsama/urutau/internal/snapshot",
	} {
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

// TestSinksNeverKnowSources: the sink package must not directly import any
// source or the go-mysql driver (acceptance §3).
func TestSinksNeverKnowSources(t *testing.T) {
	d := directImports(t, "github.com/maltzsama/urutau/internal/sink/iceberg")
	for imp := range d {
		if strings.HasPrefix(imp, "github.com/maltzsama/urutau/source") ||
			strings.HasPrefix(imp, "github.com/maltzsama/urutau/internal/source") ||
			strings.HasPrefix(imp, "github.com/go-mysql-org/go-mysql") {
			t.Errorf("internal/sink/iceberg imports %s — sinks consume core.Schema", imp)
		}
	}
}

// TestOrchestrationConsumesContracts: runner, coordinator and worker consume
// only the source/sink/driver contracts — never a concrete source or sink,
// and never iceberg-go.
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
				strings.HasPrefix(imp, "github.com/apache/iceberg-go") {
				t.Errorf("%s imports %s — consume the source/sink/driver contracts", pkg, imp)
			}
		}
	}
}

// TestContractsArePluginSafe: the public contract packages must not import
// anything under internal/ — an external plugin imports these contracts and
// must not transitively pull the engine internals.
func TestContractsArePluginSafe(t *testing.T) {
	for _, pkg := range []string{
		"github.com/maltzsama/urutau/source",
		"github.com/maltzsama/urutau/sink",
		"github.com/maltzsama/urutau/driver",
		"github.com/maltzsama/urutau/core",
		"github.com/maltzsama/urutau/change",
		"github.com/maltzsama/urutau/position",
		"github.com/maltzsama/urutau/spec",
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

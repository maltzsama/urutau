// Package architecture enforces the dependency walls at test time.
// It checks the DIRECT imports of the contract packages: a wall leaks the
// moment a package imports the other side. `go list -deps` (transitive) is
// too strict — the runner legitimately depends on drivers, which know the
// implementations.
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
		"github.com/maltzsama/urutau/internal/snapshot",
	} {
		d := directImports(t, pkg)
		for imp := range d {
			if strings.HasPrefix(imp, "github.com/maltzsama/urutau/sink") || imp == "github.com/apache/iceberg-go" {
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
		if strings.HasPrefix(imp, "github.com/maltzsama/urutau/internal/source") ||
			strings.HasPrefix(imp, "github.com/go-mysql-org/go-mysql") {
			t.Errorf("internal/sink/iceberg imports %s — sinks consume core.Schema", imp)
		}
	}
}

// TestRunnerConsumesInterfaces: the runner must not directly import the
// concrete source or sink implementations (acceptance §7).
func TestRunnerConsumesInterfaces(t *testing.T) {
	d := directImports(t, "github.com/maltzsama/urutau/internal/runner")
	for imp := range d {
		if imp == "github.com/maltzsama/urutau/internal/source/mysql" ||
			imp == "github.com/maltzsama/urutau/internal/sink/iceberg" ||
			imp == "github.com/maltzsama/urutau/internal/adapter" {
			t.Errorf("internal/runner imports %s — consume drivers/interfaces instead", imp)
		}
	}
}

// TestAdapterIsContractsOnly: the adapter must not directly import any
// concrete source or sink implementation — it holds contracts and the type
// aliases only. The assembly (the switch that names the sources) lives in
// internal/drivers. source/types is exempt: it carries the shared contract
// types (Runtime, Capabilities, StreamSource), not an implementation.
func TestAdapterIsContractsOnly(t *testing.T) {
	d := directImports(t, "github.com/maltzsama/urutau/internal/adapter")
	for imp := range d {
		if strings.HasPrefix(imp, "github.com/maltzsama/urutau/internal/source/") && imp != "github.com/maltzsama/urutau/internal/source/types" {
			t.Errorf("internal/adapter imports %s — resolve implementations in drivers", imp)
		}
		if strings.HasPrefix(imp, "github.com/maltzsama/urutau/internal/sink/") {
			t.Errorf("internal/adapter imports %s — sinks consume core.Schema via drivers", imp)
		}
	}
}

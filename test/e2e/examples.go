package e2e

import (
	"bytes"
	"os"
	"testing"

	"github.com/maltzsama/urutau/spec"
)

// examplesDir is the repo-relative path to the tested example specs, from
// this package's working directory (test/e2e).
const examplesDir = "../../examples/"

// loadExampleSpec reads examples/<name> — the same spec file the docs
// embed — validates it, then applies any env-var overrides needed to run
// against this test run's actual stack (broker address, registry URL,
// topic, catalog URI, warehouse — whatever the caller passes). Keeping
// the example file itself free of test-only substitution keeps it a
// realistic, copy-pasteable pipeline: the file docs point at is exactly
// what a user would write, not a template.
func loadExampleSpec(t *testing.T, name string, overrides func(*spec.Spec)) *spec.Spec {
	t.Helper()
	b, err := os.ReadFile(examplesDir + name)
	if err != nil {
		t.Fatalf("read example %s: %v", name, err)
	}
	s, err := spec.LoadYAML(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("load example %s: %v", name, err)
	}
	if overrides != nil {
		overrides(s)
	}
	if err := s.Validate(); err != nil {
		t.Fatalf("validate example %s: %v", name, err)
	}
	return s
}

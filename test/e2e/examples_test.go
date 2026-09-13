package e2e

import (
	"os"
	"path/filepath"
	"testing"
)

// TestExamplesValidate proves every example spec under examples/ (the
// files the docs embed) parses and validates on its own — no docker, no
// URUTAU_E2E, no override. This is what keeps the docs from rotting: an
// example that stops validating fails here, at test time, not silently
// in someone's copy-pasted pipeline.
func TestExamplesValidate(t *testing.T) {
	entries, err := os.ReadDir("../../examples")
	if err != nil {
		t.Fatalf("read examples dir: %v", err)
	}
	found := 0
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".yaml" {
			continue
		}
		found++
		t.Run(e.Name(), func(t *testing.T) {
			loadExampleSpec(t, e.Name(), nil)
		})
	}
	if found == 0 {
		t.Fatal("no examples/*.yaml files found — the docs have nothing tested to embed")
	}
}

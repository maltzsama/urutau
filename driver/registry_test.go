package driver

import (
	"context"
	"strings"
	"testing"

	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// The driver package's test binary imports nothing that registers a driver,
// so its registry is empty by construction — exactly the state a binary
// that forgot its blank imports runs with. The precondition below keeps
// that assumption loud if the import graph ever changes.
func preconditionEmptyRegistry(t *testing.T) {
	t.Helper()
	if kinds, sinks := registeredKinds(), registeredSinks(); len(kinds) != 0 || len(sinks) != 0 {
		t.Fatalf("precondition: registry must be empty in this test binary; got sources=%v sinks=%v", kinds, sinks)
	}
}

// TestUnknownSourceKindSuggestsBlankImport documents the unknown-kind path
// (CR-039 §5.2): the error must read as "the binary was built without this
// driver" — naming what IS registered and pointing at the blank import —
// not as a user configuration mistake.
func TestUnknownSourceKindSuggestsBlankImport(t *testing.T) {
	preconditionEmptyRegistry(t)

	s := &spec.Spec{Source: spec.Source{Kind: "mysql"}}
	_, err := OpenSource(s, source.Runtime{})
	if err == nil {
		t.Fatal("want error for unregistered kind")
	}
	for _, want := range []string{
		`unknown source kind "mysql"`,
		"registered: []",
		"blank-imported in internal/builtin",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q: want substring %q", err, want)
		}
	}

	if _, err := CapsForKind("mysql"); err == nil {
		t.Fatal("CapsForKind: want error for unregistered kind")
	} else if !strings.Contains(err.Error(), "blank-imported in internal/builtin") {
		t.Errorf("CapsForKind error %q: want blank-import hint", err)
	}
}

// TestUnknownSinkTypeSuggestsBlankImport mirrors the source check for sinks.
func TestUnknownSinkTypeSuggestsBlankImport(t *testing.T) {
	preconditionEmptyRegistry(t)

	_, err := OpenSinkConfig(context.Background(), sink.Config{Type: "iceberg+rest"})
	if err == nil {
		t.Fatal("want error for unregistered sink type")
	}
	for _, want := range []string{
		`unknown sink type "iceberg+rest"`,
		"registered: []",
		"blank-imported in internal/builtin",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q: want substring %q", err, want)
		}
	}
}

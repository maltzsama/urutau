package driver

import (
	"context"
	"testing"

	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

func TestSinkConfigFromSpec(t *testing.T) {
	s := &spec.Spec{
		Source: spec.Source{Kind: "mysql"},
		Sink: spec.Sink{
			Type:      "iceberg+rest",
			URI:       "http://catalog:8181",
			Namespace: "lakehouse",
			Warehouse: "wh",
			ClientID:  "cid",
			Scope:     "public",
			CommitMode: "atomic",
			Defaults: spec.Defaults{
				TargetFileSize: "256MB",
			},
		},
	}
	got := SinkConfig(s)
	if got.Type != "iceberg+rest" {
		t.Errorf("Type = %q", got.Type)
	}
	if got.URI != "http://catalog:8181" {
		t.Errorf("URI = %q", got.URI)
	}
	if got.Namespace != "lakehouse" {
		t.Errorf("Namespace = %q", got.Namespace)
	}
	if got.SourceKind != "mysql" {
		t.Errorf("SourceKind = %q", got.SourceKind)
	}
	if got.Options[OptWarehouse] != "wh" {
		t.Errorf("warehouse = %q", got.Options[OptWarehouse])
	}
	if got.Options[OptCommitMode] != "atomic" {
		t.Errorf("commit_mode = %q", got.Options[OptCommitMode])
	}
	if got.Options[OptTargetFileSize] != "256MB" {
		t.Errorf("target_file_size = %q", got.Options[OptTargetFileSize])
	}
}

func TestSinkTypeExistsEmptyDefaultsToIceberg(t *testing.T) {
	resetRegistry()
	t.Cleanup(resetRegistry)

	// Register the default sink type.
	if err := RegisterSink(DefaultSinkType, func(context.Context, sink.Config) (sink.Sink, error) { return nil, nil }); err != nil {
		t.Fatalf("register: %v", err)
	}
	if !SinkTypeExists("") {
		t.Error("empty scheme should resolve to default")
	}
}

func TestSinkTypeExistsNotRegistered(t *testing.T) {
	resetRegistry()
	t.Cleanup(resetRegistry)
	if SinkTypeExists("nonexistent") {
		t.Error("unregistered type should return false")
	}
}

func TestValidateParallelismWithRegisteredSource(t *testing.T) {
	resetRegistry()
	t.Cleanup(resetRegistry)

	capFn := func(*spec.Spec, source.Runtime) (source.Source, error) { return nil, nil }
	if err := RegisterSource("testsrc", source.Capabilities{MaxConnections: 5}, capFn); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Zero means no opinion, always passes.
	if err := ValidateParallelism("testsrc", 0); err != nil {
		t.Errorf("zero parallelism: %v", err)
	}

	// Under ceiling.
	if err := ValidateParallelism("testsrc", 5); err != nil {
		t.Errorf("at ceiling: %v", err)
	}

	// Over ceiling.
	if err := ValidateParallelism("testsrc", 6); err == nil {
		t.Error("over ceiling: want error")
	}
}

func TestValidateParallelismZeroCap(t *testing.T) {
	resetRegistry()
	t.Cleanup(resetRegistry)

	capFn := func(*spec.Spec, source.Runtime) (source.Source, error) { return nil, nil }
	if err := RegisterSource("nocap", source.Capabilities{}, capFn); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Zero MaxConnections means no ceiling.
	if err := ValidateParallelism("nocap", 1000); err != nil {
		t.Errorf("no ceiling: %v", err)
	}
}

func TestRegisterAndLookupRoundTrip(t *testing.T) {
	resetRegistry()
	t.Cleanup(resetRegistry)

	capFn := func(*spec.Spec, source.Runtime) (source.Source, error) { return nil, nil }
	if err := RegisterSource("mysql", source.Capabilities{MaxConnections: 10}, capFn); err != nil {
		t.Fatalf("register source: %v", err)
	}
	sinkFn := func(context.Context, sink.Config) (sink.Sink, error) { return nil, nil }
	if err := RegisterSink("iceberg+rest", sinkFn); err != nil {
		t.Fatalf("register sink: %v", err)
	}

	caps, err := CapsForKind("mysql")
	if err != nil {
		t.Fatalf("CapsForKind: %v", err)
	}
	if caps.MaxConnections != 10 {
		t.Errorf("MaxConnections = %d, want 10", caps.MaxConnections)
	}

	if !SinkTypeExists("iceberg+rest") {
		t.Error("SinkTypeExists should return true for registered type")
	}

	kinds := registeredKinds()
	if len(kinds) != 1 || kinds[0] != "mysql" {
		t.Errorf("registeredKinds = %v, want [mysql]", kinds)
	}

	sinks := registeredSinks()
	if len(sinks) != 1 || sinks[0] != "iceberg+rest" {
		t.Errorf("registeredSinks = %v, want [iceberg+rest]", sinks)
	}
}

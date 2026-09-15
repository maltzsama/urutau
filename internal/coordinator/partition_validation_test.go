package coordinator

import (
	"strings"
	"testing"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/spec"
)

// partitioned builds a table with workers>1 (WK-001 C0).
func partitioned(target string, n int) spec.Table {
	return spec.Table{Target: target, Workers: &spec.WorkerSpec{Number: n}}
}

// fakeConcurrent is a minimal ConcurrentWriter for the capability probe —
// the check never touches a concrete sink.
type fakeConcurrent struct{ ok bool }

func (f fakeConcurrent) SupportsConcurrentWriters() bool { return f.ok }

func TestRequirePartitionKey(t *testing.T) {
	ref := core.TableRef{Source: "db.t", Target: "raw.t", PrimaryKey: []string{"id"}}

	if err := requirePartitionKey(partitioned("raw.t", 3), ref); err != nil {
		t.Fatalf("a key present with workers>1 must pass: %v", err)
	}

	noPK := core.TableRef{Source: "db.t", Target: "raw.t"}
	err := requirePartitionKey(partitioned("raw.t", 3), noPK)
	if err == nil || !strings.Contains(err.Error(), "primary key") {
		t.Fatalf("workers>1 without a key must fail citing it, got: %v", err)
	}

	if err := requirePartitionKey(spec.Table{Target: "raw.t"}, noPK); err != nil {
		t.Fatalf("workers<=1 without a key must pass: %v", err)
	}
}

func TestRequireConcurrentSink(t *testing.T) {
	tab := partitioned("raw.t", 3)

	// No capability at all: a plugin-style sink, or one that never declared it.
	if err := requireConcurrentSink(tab, nil); err == nil {
		t.Fatal("a nil sink must be rejected with workers>1")
	}
	if err := requireConcurrentSink(tab, struct{}{}); err == nil {
		t.Fatal("a type without ConcurrentWriter must be rejected")
	}
	// Declared, but false: the sink's concurrent path is not built.
	if err := requireConcurrentSink(tab, fakeConcurrent{ok: false}); err == nil {
		t.Fatal("SupportsConcurrentWriters()==false must be rejected")
	}
	// Declared and true: allowed.
	if err := requireConcurrentSink(tab, fakeConcurrent{ok: true}); err != nil {
		t.Fatalf("SupportsConcurrentWriters()==true must pass: %v", err)
	}
	// workers<=1 ignores the sink entirely.
	if err := requireConcurrentSink(spec.Table{Target: "raw.t"}, nil); err != nil {
		t.Fatalf("workers<=1 must pass regardless of the sink: %v", err)
	}
}

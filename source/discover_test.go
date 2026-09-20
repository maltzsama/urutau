package source

import (
	"context"
	"errors"
	"testing"

	"github.com/maltzsama/urutau/spec"
)

// fakeDiscoverer embeds the Source interface (nil) so it satisfies Source,
// then adds Discover — the optional capability ExpandTables type-asserts.
type fakeDiscoverer struct {
	Source
	refs    []TableRef
	err     error
	schemas []string
}

func (f *fakeDiscoverer) Discover(_ context.Context, schemas []string) ([]TableRef, error) {
	f.schemas = schemas
	return f.refs, f.err
}

func TestExpandTablesPassthrough(t *testing.T) {
	s := &spec.Spec{Tables: []spec.Table{{Source: "public.a", Target: "raw.a"}}}
	got, err := ExpandTables(context.Background(), struct{ Source }{}, s)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Source != "public.a" {
		t.Fatalf("passthrough = %+v, want the explicit list", got)
	}
}

func TestExpandTablesDiscovery(t *testing.T) {
	d := &fakeDiscoverer{refs: []TableRef{{Source: "public.users"}, {Source: "public.orders"}}}
	s := &spec.Spec{
		Source: spec.Source{Kind: "postgres", Postgres: &spec.PostgresSource{Discover: true, Schemas: []string{"public"}}},
		Sink:   spec.Sink{Namespace: "raw"},
	}
	got, err := ExpandTables(context.Background(), d, s)
	if err != nil {
		t.Fatal(err)
	}
	if len(d.schemas) != 1 || d.schemas[0] != "public" {
		t.Fatalf("schemas passed = %v, want [public]", d.schemas)
	}
	if len(got) != 2 {
		t.Fatalf("got %d tables, want 2", len(got))
	}
	// Sorted by source; target derived from the sink namespace.
	if got[0].Source != "public.orders" || got[0].Target != "raw.orders" || !got[0].CreateIfNotExists {
		t.Fatalf("table[0] = %+v, want public.orders → raw.orders createIfNotExists", got[0])
	}
	if got[1].Source != "public.users" || got[1].Target != "raw.users" {
		t.Fatalf("table[1] = %+v, want public.users → raw.users", got[1])
	}
}

func TestExpandTablesCollision(t *testing.T) {
	d := &fakeDiscoverer{refs: []TableRef{{Source: "public.orders"}, {Source: "analytics.orders"}}}
	s := &spec.Spec{
		Source: spec.Source{Kind: "postgres", Postgres: &spec.PostgresSource{Discover: true}},
		Sink:   spec.Sink{Namespace: "raw"},
	}
	if _, err := ExpandTables(context.Background(), d, s); err == nil {
		t.Fatal("two sources mapping to one target must error")
	}
}

func TestExpandTablesEmptyIsError(t *testing.T) {
	d := &fakeDiscoverer{}
	s := &spec.Spec{
		Source: spec.Source{Kind: "postgres", Postgres: &spec.PostgresSource{Discover: true}},
		Sink:   spec.Sink{Namespace: "raw"},
	}
	if _, err := ExpandTables(context.Background(), d, s); err == nil {
		t.Fatal("an empty discovery must error, not boot with no tables")
	}
}

func TestExpandTablesUnsupportedSource(t *testing.T) {
	s := &spec.Spec{
		Source: spec.Source{Kind: "kafka", Postgres: &spec.PostgresSource{Discover: true}},
		Sink:   spec.Sink{Namespace: "raw"},
	}
	if _, err := ExpandTables(context.Background(), struct{ Source }{}, s); err == nil {
		t.Fatal("a source without Discoverer must error when discovery is requested")
	}
}

func TestExpandTablesDiscoverError(t *testing.T) {
	d := &fakeDiscoverer{err: errors.New("catalog down")}
	s := &spec.Spec{
		Source: spec.Source{Kind: "postgres", Postgres: &spec.PostgresSource{Discover: true}},
		Sink:   spec.Sink{Namespace: "raw"},
	}
	if _, err := ExpandTables(context.Background(), d, s); err == nil {
		t.Fatal("a Discover error must propagate")
	}
}

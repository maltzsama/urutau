package plan

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// fakeSource is the minimal source.Source the introspection tests exercise:
// only Introspect (and, optionally, Discover) are used. Everything else errors
// if reached.
type fakeSource struct {
	schema   core.Schema
	warnings []core.Warning
	discover []source.TableRef
}

func (f *fakeSource) Open(context.Context, []source.TableRef) (source.Reader, error) {
	return nil, errors.New("not used")
}
func (f *fakeSource) InitialPosition(context.Context) (position.Position, error) {
	return nil, nil
}
func (f *fakeSource) ParsePosition(string) (position.Position, error) { return nil, nil }
func (f *fakeSource) Introspect(_ context.Context, t spec.Table) (core.TableRef, core.Schema, []core.Warning, error) {
	return core.TableRef{Source: t.Source, Target: t.Target, PrimaryKey: []string{"id"}}, f.schema, f.warnings, nil
}
func (f *fakeSource) Discover(context.Context, []string) ([]source.TableRef, error) {
	return f.discover, nil
}

// A per-table source for the multi-table / ordering tests.
type perTableSource struct {
	bySource map[string]core.Schema
}

func (p *perTableSource) Open(context.Context, []source.TableRef) (source.Reader, error) {
	return nil, errors.New("not used")
}
func (p *perTableSource) InitialPosition(context.Context) (position.Position, error) {
	return nil, nil
}
func (p *perTableSource) ParsePosition(string) (position.Position, error) { return nil, nil }
func (p *perTableSource) Introspect(_ context.Context, t spec.Table) (core.TableRef, core.Schema, []core.Warning, error) {
	return core.TableRef{Source: t.Source, Target: t.Target, PrimaryKey: []string{"id"}}, p.bySource[t.Source], nil, nil
}

func int64Column(name string) core.Column {
	return core.Column{Name: name, Type: core.ColumnType{Kind: core.KindInt64}}
}

// FT-1 (moved from the runner): Introspect extends BOTH the wire and the
// resolved shape with the reference destinations (explicit selects, final
// names), so EnsureTable creates the column and the drift check sees one stable
// shape from batch 1 — while the raw source view stays free of them.
func TestIntrospectExtendsBothShapesForEnrich(t *testing.T) {
	src := &perTableSource{bySource: map[string]core.Schema{
		"db.users": {Columns: []core.Column{int64Column("id")}},
	}}
	tables := []spec.Table{{
		Source: "db.users", Target: "raw.users",
		Enrich: []spec.Enrich{{
			Table:  "users",
			Select: []string{"name"},
			On:     map[string]string{"user_ref": "id"},
			As:     map[string]string{"users.name": "user_name"},
		}},
	}}
	p, err := Introspect(context.Background(), src, tables, Options{Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatalf("Introspect: %v", err)
	}
	for name, m := range map[string]map[string]core.Schema{"resolved": p.Resolved, "wire": p.Wire} {
		col, ok := m["db.users"].Column("user_name")
		if !ok || col.Type.Kind != core.KindString || !col.Type.Nullable {
			t.Fatalf("%s shape lacks the reference column as nullable string: %+v", name, col.Type)
		}
	}
}

// A custom Resolver (the collapsed runner's stage-backed path) is used for
// every table: it receives the RAW source view, and the names it returns are
// applied to BOTH shapes by plan — the runner's single-query behavior.
func TestIntrospectUsesCustomResolver(t *testing.T) {
	src := &perTableSource{bySource: map[string]core.Schema{
		"db.users": {Columns: []core.Column{int64Column("id")}},
	}}
	var gotSchema core.Schema
	resolve := func(_ context.Context, _ spec.Table, sourceSchema core.Schema) ([]string, error) {
		gotSchema = sourceSchema
		return []string{"users.tier"}, nil
	}
	tables := []spec.Table{{Source: "db.users", Target: "raw.users"}}
	p, err := Introspect(context.Background(), src, tables, Options{
		Logger:  slog.New(slog.DiscardHandler),
		Resolve: resolve,
	})
	if err != nil {
		t.Fatalf("Introspect: %v", err)
	}
	if _, ok := gotSchema.Column("users.tier"); ok {
		t.Fatal("resolver received a schema already carrying the reference destination")
	}
	for name, m := range map[string]core.Schema{"wire": p.Wire["db.users"], "resolved": p.Resolved["db.users"]} {
		if _, ok := m.Column("users.tier"); !ok {
			t.Fatalf("%s shape missing the custom resolver's destination", name)
		}
	}
}

// Introspect preserves ref order (positional against Tables), indexes BySource,
// and parses each table's cast policy.
func TestIntrospectOrderAndIndexes(t *testing.T) {
	src := &perTableSource{bySource: map[string]core.Schema{
		"db.a": {Columns: []core.Column{int64Column("id")}},
		"db.b": {Columns: []core.Column{int64Column("id")}},
	}}
	tables := []spec.Table{
		{Source: "db.a", Target: "raw.a"},
		{Source: "db.b", Target: "raw.b"},
	}
	p, err := Introspect(context.Background(), src, tables, Options{Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatalf("Introspect: %v", err)
	}
	if len(p.Refs) != 2 || p.Refs[0].Source != "db.a" || p.Refs[1].Source != "db.b" {
		t.Fatalf("Refs order = %v, want [db.a db.b]", p.Refs)
	}
	if got := p.BySource["db.b"].Target; got != "raw.b" {
		t.Fatalf("BySource[db.b].Target = %q, want raw.b", got)
	}
	if _, ok := p.Casts["db.a"]; !ok {
		t.Fatal("Casts missing db.a")
	}
}

// Source and cast-resolution warnings both surface through the logger, once.
func TestIntrospectSurfacesWarnings(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	src := &perTableSource{bySource: map[string]core.Schema{
		"db.users": {PrimaryKey: []string{"id"}, Columns: []core.Column{int64Column("id")}},
	}}
	// Casting the primary-key column warns at resolve time.
	tables := []spec.Table{{
		Source: "db.users", Target: "raw.users",
		Cast: map[string]string{"id": "string"},
	}}
	if _, err := Introspect(context.Background(), src, tables, Options{Logger: logger}); err != nil {
		t.Fatalf("Introspect: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "primary-key") {
		t.Fatalf("cast warning not surfaced: %q", out)
	}
	if !strings.Contains(out, "warning=") || !strings.Contains(out, "table=db.users") {
		t.Fatalf("warning attributes missing: %q", out)
	}
}

// A wildcard reference's expansion is part of introspection: plan runs the ONE
// shared algorithm (enrich.LoadWildcardColumns), and a reference it cannot
// reach fails boot loudly, naming the reference — never a silent empty schema.
func TestIntrospectWildcardFailsLoudlyNamingReference(t *testing.T) {
	src := &perTableSource{bySource: map[string]core.Schema{
		"db.users": {Columns: []core.Column{int64Column("id"), int64Column("user_ref")}},
	}}
	tables := []spec.Table{{
		Source: "db.users", Target: "raw.users",
		Enrich: []spec.Enrich{{
			Table:    "users",
			Source:   spec.EnrichSource{URI: "sqlite://not-a-supported-scheme", Query: "SELECT id, tier FROM users"},
			On:       map[string]string{"user_ref": "id"},
			Select:   []string{"*"},
			JoinType: "left",
		}},
	}}
	_, err := Introspect(context.Background(), src, tables, Options{Logger: slog.New(slog.DiscardHandler)})
	if err == nil {
		t.Fatal("Introspect: want error for an unreachable wildcard reference, got nil")
	}
	if !strings.Contains(err.Error(), "users") {
		t.Fatalf("Introspect error %q does not name the reference", err.Error())
	}
}

// Build expands a discovery pipeline's table list (source.Enumerate) and
// introspects it, deriving the target from the sink namespace.
func TestBuildExpandsDiscoveredTables(t *testing.T) {
	src := &fakeSource{
		schema: core.Schema{Columns: []core.Column{int64Column("id")}},
		discover: []source.TableRef{
			{Source: "public.orders"},
			{Source: "public.users"},
		},
	}
	s := &spec.Spec{
		Source: spec.Source{Kind: "postgres", Postgres: &spec.PostgresSource{Discover: true, Schemas: []string{"public"}}},
		Sink:   spec.Sink{Namespace: "raw"},
	}
	p, err := Build(context.Background(), src, s, Options{Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(p.Tables) != 2 || p.Tables[0].Source != "public.orders" || p.Tables[1].Source != "public.users" {
		t.Fatalf("discovered tables = %v, want sorted [public.orders public.users]", p.Tables)
	}
	if p.Refs[0].Target != "raw.orders" {
		t.Fatalf("derived target = %q, want raw.orders", p.Refs[0].Target)
	}
}

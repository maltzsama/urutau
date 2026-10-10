// Package plan performs the boot introspection both orchestration modes share:
// resolve every spec table through the source, apply the cast policy, resolve
// the sink schema, and expand the enrich reference columns — including the real
// names a wildcard select only discovers by running its query — ONCE, producing
// a single Plan the collapsed runner (internal/runner) and the distributed
// coordinator (internal/coordinator) each consume. It is step 2 of the shared
// collapsed/distributed core (issue #718, design #404).
//
// The package may not import internal/runner or internal/coordinator: it is the
// shared leaf both depend on. It may import source, core, spec, internal/enrich
// (and, if ever needed, internal/snapshot / driver).
package plan

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/enrich"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// Plan is the resolved boot introspection both orchestration modes consume.
// Every schema/policy map is keyed by SOURCE table (spec.Table.Source).
type Plan struct {
	// Tables is the expanded table list, in spec order (a discovery pipeline's
	// list is enumerated by the source). Refs[i] is Tables[i]'s resolved ref.
	Tables []spec.Table
	// Refs are the resolved table refs, in the same order as Tables.
	Refs []source.TableRef
	// Wire is the WIRE shape: the source types the worker encodes
	// (core.WireSchema) plus the enrich reference destination columns.
	Wire map[string]core.Schema
	// Resolved is the sink target shape: cast types + metadata columns plus the
	// enrich reference destination columns.
	Resolved map[string]core.Schema
	// Casts is the parsed cast policy per source table.
	Casts map[string]core.CastPolicy
	// BySource indexes the spec table by its source name.
	BySource map[string]spec.Table
}

// Resolver resolves a table's enrich destination columns — the explicit
// selects (renamed) plus the real names a wildcard select only discovers by
// running its query. sourceSchema is the RAW source view (before any enrich
// extension).
//
// The coordinator leaves this nil, so plan uses enrich.LoadWildcardColumns (the
// coordinator has no enrich stage of its own). The collapsed runner supplies a
// stage-backed resolver: it must build the long-lived join stage anyway, and
// resolving through that same stage keeps ONE reference query and warms the
// stage synchronously — exactly the behavior before the extraction. Both paths
// share plan's single schema-extension step.
type Resolver func(ctx context.Context, t spec.Table, sourceSchema core.Schema) ([]string, error)

// Options tunes one introspection. The zero value is the coordinator's: no
// logger (slog.Default) and the default wildcard algorithm.
type Options struct {
	Logger  *slog.Logger
	Resolve Resolver
}

// ExpandTables returns the effective table list for a pipeline — the shared
// first step of introspection. A discovery pipeline lists no tables and the
// source enumerates them now; otherwise it is the spec's explicit list.
func ExpandTables(ctx context.Context, src source.Source, s *spec.Spec) ([]spec.Table, error) {
	tables, err := source.ExpandTables(ctx, src, s)
	if err != nil {
		return nil, fmt.Errorf("plan: %w", err)
	}
	return tables, nil
}

// Introspect resolves the (already expanded) tables ONCE: read each table's
// source schema, parse its cast policy, resolve the sink schema, and extend
// both the wire and the resolved shapes with the enrich reference destination
// columns — including the real names a wildcard select only discovers by
// running its query. The expansion is this package's single algorithm; opts.Resolve
// supplies only the query mechanism (the default is enrich.LoadWildcardColumns).
// Advisory source and cast warnings are surfaced through opts.Logger exactly as
// each mode did before.
//
// It is behaviour-preserving for both modes: the coordinator's canonical
// (wire) and resolved (DDL) maps, the runner's wire and resolved maps, the ref
// order and the warning stream are identical to the code it replaces.
func Introspect(ctx context.Context, src source.Source, tables []spec.Table, opts Options) (*Plan, error) {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	resolve := opts.Resolve
	if resolve == nil {
		resolve = defaultResolver
	}
	p := &Plan{
		Tables:   tables,
		Refs:     make([]source.TableRef, 0, len(tables)),
		Wire:     make(map[string]core.Schema, len(tables)),
		Resolved: make(map[string]core.Schema, len(tables)),
		Casts:    make(map[string]core.CastPolicy, len(tables)),
		BySource: make(map[string]spec.Table, len(tables)),
	}
	for _, t := range tables {
		ref, srcSchema, srcWarns, err := src.Introspect(ctx, t)
		if err != nil {
			return nil, err
		}
		for _, w := range srcWarns {
			logger.Warn("schema", "table", ref.Source, "warning", w.Message)
		}
		cast, err := core.ParseCastPolicy(t.Cast)
		if err != nil {
			return nil, fmt.Errorf("plan: table %s cast: %w", t.Target, err)
		}
		res, resWarns, err := core.ResolveSchema(srcSchema, cast, t.Metadata)
		if err != nil {
			return nil, fmt.Errorf("plan: table %s: %w", t.Target, err)
		}
		for _, w := range resWarns {
			logger.Warn("schema", "table", ref.Source, "warning", w.Message)
		}
		// One schema-extension algorithm for both modes: an explicit-select
		// reference contributes its renamed destinations with no I/O; a wildcard
		// reference runs its query synchronously here so its real columns are
		// known before sink DDL. The mode supplies only the query mechanism.
		dests, err := resolve(ctx, t, srcSchema)
		if err != nil {
			return nil, err
		}
		p.Refs = append(p.Refs, ref)
		p.Wire[t.Source] = enrich.AddColumns(core.WireSchema(srcSchema, res), dests)
		p.Resolved[t.Source] = enrich.AddColumns(res, dests)
		p.Casts[t.Source] = cast
		p.BySource[t.Source] = t
	}
	return p, nil
}

// defaultResolver is the no-stage wildcard algorithm: enrich.LoadWildcardColumns
// opens its own throwaway loader per wildcard reference (the coordinator path).
// An explicit-only config does no I/O.
func defaultResolver(ctx context.Context, t spec.Table, _ core.Schema) ([]string, error) {
	dests, err := enrich.LoadWildcardColumns(ctx, t.Enrich)
	if err != nil {
		return nil, fmt.Errorf("plan: %s: enrich: %w", t.Source, err)
	}
	return dests, nil
}

// Build expands the table list and introspects it in one call — the
// coordinator's entry point, which needs neither step separately.
func Build(ctx context.Context, src source.Source, s *spec.Spec, opts Options) (*Plan, error) {
	tables, err := ExpandTables(ctx, src, s)
	if err != nil {
		return nil, err
	}
	return Introspect(ctx, src, tables, opts)
}

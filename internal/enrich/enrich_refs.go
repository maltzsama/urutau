package enrich

import (
	"context"
	"fmt"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/spec"
)

// RefColumnsFor returns the FINAL destination names (renames applied) the
// given declarations inject, for explicit selects — the wire-schema extension
// a caller must apply BEFORE the pipeline starts, so every batch travels with
// the full column set and the drift check sees one stable shape. Empty for a
// wildcard select: this function does no I/O and the real names are only
// known once the reference query runs — see LoadWildcardColumns (no Stage)
// or Stage.LoadWildcards + Stage.RefColumns (with a Stage) for the
// wildcard-aware equivalent.
//
// Exported because the pipeline's three schema owners — the collapsed runner,
// the worker's assignment path and the coordinator's assignment builder —
// must all apply the SAME extension, and the coordinator builds no stage.
func RefColumnsFor(cfgs []spec.Enrich) []string {
	var out []string
	for _, cfg := range cfgs {
		if len(cfg.Select) == 1 && cfg.Select[0] == "*" {
			continue
		}
		for _, s := range cfg.Select {
			dest := cfg.Table + "." + s
			if as, ok := cfg.As[dest]; ok {
				dest = as
			}
			out = append(out, dest)
		}
	}
	return out
}

// LoadWildcardColumns runs the first load, synchronously, for every
// WILDCARD reference in cfgs and returns the complete RefColumns set for
// cfgs: RefColumnsFor's explicit-select names plus the real names just
// discovered for each wildcard reference. For a caller with no Stage of
// its own — the coordinator, which forwards enrich declarations to the
// worker instead of running the join itself, so it has no long-lived
// place to keep a Stage — but still needs the real wildcard columns before
// it extends a table's wire schema for sink DDL.
//
// Each wildcard reference gets its own short-lived loader (opened and
// closed here); this is deliberately independent of whatever Stage the
// worker later builds for the same declarations — the two processes share
// no connection, so the worker's real, long-lived load still runs a second
// time. Accepted, disclosed cost of the coordinator/worker split; not
// avoidable without a protocol change.
func LoadWildcardColumns(ctx context.Context, cfgs []spec.Enrich) ([]string, error) {
	out := RefColumnsFor(cfgs)
	for _, cfg := range cfgs {
		if !isWildcard(cfg) {
			continue
		}
		maxRows := cfg.MaxRows
		if maxRows == 0 {
			maxRows = DefaultMaxRows
		}
		l, err := newSQLLoader(cfg.Source.URI, cfg.Source.Query, onRefOf(cfg), maxRows)
		if err != nil {
			return nil, fmt.Errorf("enrich: reference %q: %w", cfg.Table, err)
		}
		rec, err := l.Load(ctx)
		if err != nil {
			_ = l.Close()
			return nil, fmt.Errorf("enrich: reference %q: %w", cfg.Table, err)
		}
		snap, err := buildImage(&refJoin{cfg: cfg, onRef: onRefOf(cfg)}, rec)
		rec.Release()
		if err != nil {
			_ = l.Close()
			return nil, fmt.Errorf("enrich: reference %q: %w", cfg.Table, err)
		}
		if err := l.Close(); err != nil {
			return nil, fmt.Errorf("enrich: reference %q: close: %w", cfg.Table, err)
		}
		for _, d := range snap.dests {
			out = append(out, d.as)
		}
	}
	return out, nil
}

// onRefOf returns the reference-side column name of cfg's single join
// pair — New already validates cfg.On has exactly one entry; this mirrors
// that resolution for a throwaway refJoin used only for column discovery.
func onRefOf(cfg spec.Enrich) string {
	for _, ref := range cfg.On {
		return ref
	}
	return ""
}

// AddColumns returns cs extended with exactly the given destination names
// (deduplicated; nullable strings — the registered type decision until
// CR-069 resolves real types). The sibling of AddRefColumns for a caller
// that already resolved the real destination names itself — e.g. via
// LoadWildcardColumns, or Stage.RefColumns() after LoadWildcards — instead
// of recomputing them from cfgs (which RefColumnsFor cannot do for a
// wildcard select).
func AddColumns(cs core.Schema, dests []string) core.Schema {
	for _, dest := range dests {
		if _, exists := cs.Column(dest); !exists {
			cs.Columns = append(cs.Columns, core.Column{
				Name: dest,
				Type: core.ColumnType{Kind: core.KindString, Nullable: true},
			})
		}
	}
	return cs
}

// AddRefColumns returns cs extended with the declarations' EXPLICIT-select
// reference destination columns — see RefColumnsFor for why a wildcard
// select contributes nothing here. Callers apply it to every schema shape
// that feeds a sink table or a drift check BEFORE the pipeline starts:
// without it, the first enriched batch carries columns the table lacks and
// every sink silently drops them (they project by the table's own columns,
// never by the wire's). A caller with a wildcard reference must resolve its
// real columns first (LoadWildcardColumns, or Stage.RefColumns() after
// LoadWildcards) and use AddColumns instead.
func AddRefColumns(cs core.Schema, cfgs []spec.Enrich) core.Schema {
	return AddColumns(cs, RefColumnsFor(cfgs))
}

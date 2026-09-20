package source

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/maltzsama/urutau/spec"
)

// ExpandTables returns the effective table list for a pipeline: the source's
// discovered tables when spec.Source.Postgres.Discover is set, else the
// spec's explicit list. A discovery pipeline lists no tables — the source
// enumerates them at boot — and each target is derived as
// "<sink namespace>.<relname>".
//
// A derived-target collision (public.orders and analytics.orders both
// becoming ns.orders) is an error: two sources cannot share one target, and
// a silent pick would be data loss.
func ExpandTables(ctx context.Context, src Source, s *spec.Spec) ([]spec.Table, error) {
	pg := s.Source.Postgres
	if pg == nil || !pg.Discover {
		return s.Tables, nil
	}
	d, ok := src.(Discoverer)
	if !ok {
		return nil, fmt.Errorf("source %q does not support discovery", s.Source.Kind)
	}
	refs, err := d.Discover(ctx, pg.Schemas)
	if err != nil {
		return nil, err
	}
	if len(refs) == 0 {
		return nil, fmt.Errorf("source: discovery found no tables (schemas %v)", pg.Schemas)
	}
	tables := make([]spec.Table, 0, len(refs))
	seen := make(map[string]string, len(refs)) // target → source
	for _, r := range refs {
		_, table, ok := strings.Cut(r.Source, ".")
		if !ok {
			return nil, fmt.Errorf("source: discovered table %q is not schema.table", r.Source)
		}
		target := s.Sink.Namespace + "." + table
		if prev, dup := seen[target]; dup {
			return nil, fmt.Errorf("source: discovery maps %s and %s to the same target %s — use an explicit tables list with distinct targets", prev, r.Source, target)
		}
		seen[target] = r.Source
		tables = append(tables, spec.Table{
			Source:            r.Source,
			Target:            target,
			CreateIfNotExists: true,
		})
	}
	sort.Slice(tables, func(i, j int) bool { return tables[i].Source < tables[j].Source })
	return tables, nil
}

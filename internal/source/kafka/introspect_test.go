package kafka

import (
	"context"
	"slices"
	"testing"

	"github.com/maltzsama/urutau/internal/spec"
)

// The spec declares columns as a map with no order; introspection must
// resolve a deterministic column order, or two boots of the same spec
// produce differently ordered target tables and schemas.
func TestIntrospectDeterministicColumnOrder(t *testing.T) {
	s := Source{}
	tbl := spec.Table{
		Source:     "shop.users",
		Target:     "raw.users",
		PrimaryKey: []string{"id"},
		Columns: map[string]string{
			"id":     "int64",
			"name":   "string",
			"uid":    "uuid",
			"active": "bool",
		},
	}

	var first []string
	for i := 0; i < 20; i++ {
		_, cs, _, _, err := s.Introspect(context.Background(), nil, tbl)
		if err != nil {
			t.Fatalf("introspect %d: %v", i, err)
		}
		names := make([]string, 0, len(cs.Columns))
		for _, c := range cs.Columns {
			names = append(names, c.Name)
		}
		if first == nil {
			first = names
			continue
		}
		if !slices.Equal(first, names) {
			t.Fatalf("introspection %d: columns %v, want stable %v", i, names, first)
		}
	}
}

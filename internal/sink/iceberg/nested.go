// Composite-value append for the Iceberg writer. Schema translation (CR-034)
// makes struct/list/map columns expressible; this is the VALUE path — when a
// batch carries a nested column, appendColumn dispatches here and the shape
// descends recursively. Values arrive from the source as map[string]any for a
// struct or map and []any for a list; leaves fall back to the scalar column
// path, so the whole tree reuses one conversion rule.
package iceberg

import (
	"fmt"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// sortedKeys returns a map's string keys in sorted order, so map writes are
// reproducible regardless of Go's randomized map iteration.
func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// appendOneComposite appends a single row of a composite column. It is
// row-major because list and map builders record element offsets as values
// are appended — the parent span and its children must be built together.
func appendOneComposite(builder array.Builder, field arrow.Field, v any) error {
	switch b := builder.(type) {
	case *array.StructBuilder:
		if v == nil {
			b.AppendNull()
			return nil
		}
		row, ok := v.(map[string]any)
		if !ok {
			return fmt.Errorf("iceberg: column %q: want a struct row (map), got %T", field.Name, v)
		}
		b.Append(true)
		st, ok := field.Type.(*arrow.StructType)
		if !ok {
			return fmt.Errorf("iceberg: column %q: struct builder but field type %v", field.Name, field.Type)
		}
		for i, cf := range st.Fields() {
			if err := appendColumn(b.FieldBuilder(i), cf, []any{row[cf.Name]}); err != nil {
				return err
			}
		}
		return nil
	case *array.ListBuilder:
		if v == nil {
			b.AppendNull()
			return nil
		}
		items, ok := v.([]any)
		if !ok {
			return fmt.Errorf("iceberg: column %q: want a list row, got %T", field.Name, v)
		}
		lt, ok := field.Type.(*arrow.ListType)
		if !ok {
			return fmt.Errorf("iceberg: column %q: list builder but field type %v", field.Name, field.Type)
		}
		ef := lt.ElemField()
		b.Append(true)
		for _, it := range items {
			if err := appendColumn(b.ValueBuilder(), ef, []any{it}); err != nil {
				return err
			}
		}
		return nil
	case *array.MapBuilder:
		if v == nil {
			b.AppendNull()
			return nil
		}
		entries, ok := v.(map[string]any)
		if !ok {
			return fmt.Errorf("iceberg: column %q: want a map row, got %T", field.Name, v)
		}
		mt, ok := field.Type.(*arrow.MapType)
		if !ok {
			return fmt.Errorf("iceberg: column %q: map builder but field type %v", field.Name, field.Type)
		}
		// Deterministic key order so writes are reproducible.
		keys := sortedKeys(entries)
		b.Append(true)
		for _, k := range keys {
			if err := appendColumn(b.KeyBuilder(), mt.KeyField(), []any{k}); err != nil {
				return err
			}
			if err := appendColumn(b.ItemBuilder(), mt.ItemField(), []any{entries[k]}); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("iceberg: column %q: unsupported composite builder %T", field.Name, builder)
	}
}

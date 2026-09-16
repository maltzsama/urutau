package rowchange

import (
	"fmt"
	"strings"
)

// KeyString renders a primary key tuple into a unique map key. Every value
// carries its Go type as a prefix: without it, fmt of int64(1) and float64(1)
// both yield "1" and two different rows would index to the same key — the
// sink indexes equality-delete tuples by key position, so a silent merge is
// corruption, not deduplication. Strings get their own prefix, and elements
// are joined with a unit separator so adjacent values cannot merge.
func KeyString(key []any) string {
	parts := make([]string, len(key))
	for i, v := range key {
		if s, ok := v.(string); ok {
			parts[i] = "s:" + s
			continue
		}
		if v == nil {
			parts[i] = "nil"
			continue
		}
		parts[i] = fmt.Sprintf("%T:%v", v, v)
	}
	return strings.Join(parts, "\x1f")
}

package change

import (
	"fmt"
	"strings"
)

// Collapse reduces a batch to its final state per primary key: the LAST
// operation for a key wins, and keys keep their first-appearance order.
//
// The invariant this enforces: a key whose last operation is a delete never
// yields a data row. Writing a data row and an equality delete for the same
// key in one commit is a correctness trap — in iceberg-go v0.6.0 the delete
// carries the higher sequence number and kills the fresh row; in Java
// Iceberg the delete cannot kill a same-snapshot row and the row survives.
// Either way, emitting both is wrong somewhere, so collapse never does.
func Collapse(changes []Change) Collapsed {
	last := make(map[string]Change, len(changes))
	order := make([]string, 0, len(changes))
	for _, c := range changes {
		k := keyString(c.Key)
		if _, seen := last[k]; !seen {
			order = append(order, k)
		}
		last[k] = c
	}

	var out Collapsed
	for _, k := range order {
		c := last[k]
		switch c.Op {
		case OpInsert, OpUpdate:
			out.Upserts = append(out.Upserts, c)
		case OpDelete:
			out.Deletes = append(out.Deletes, c)
		}
	}
	return out
}

// KeyString renders a primary key tuple into a unique map key. Strings are
// prefixed so a string "1" never collides with the integer 1; elements are
// joined with a unit separator so adjacent values cannot merge.
func KeyString(key []any) string { return keyString(key) }

// keyString renders a primary key tuple into a unique map key. Strings are
// prefixed so a string "1" never collides with the integer 1; elements are
// joined with a unit separator so adjacent values cannot merge.
func keyString(key []any) string {
	parts := make([]string, len(key))
	for i, v := range key {
		if s, ok := v.(string); ok {
			parts[i] = "s:" + s
		} else {
			parts[i] = fmt.Sprintf("%v", v)
		}
	}
	return strings.Join(parts, "\x1f")
}

package dataplane

import "fmt"

// RequireUpsertKey returns an error when an upsert table has no primary key.
// The equality delete then carries no columns, so an upsert cannot identify
// the row it replaces — a silent data-correctness bug. This catches the case
// an explicit spec cannot: a discovered table (no declared primaryKey) whose
// source table has no primary key.
func RequireUpsertKey(target string, pk []string, mode WriteMode) error {
	if mode == UpsertMode && len(pk) == 0 {
		return fmt.Errorf("%s: upsert requires a primary key, but the source table has none — add one, or replicate it with writeMode: append", target)
	}
	return nil
}

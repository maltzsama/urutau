package coordinator

import (
	"testing"

	route "github.com/maltzsama/urutau/internal/routing"
)

// ownerKey returns an int64 primary key that the production rendezvous owner
// function assigns to owner index want among names. Tests use it instead of
// hardcoding a key/range split, so they keep asserting per-owner behavior
// without depending on the hash's arithmetic.
func ownerKey(t *testing.T, names []string, want int) int64 {
	t.Helper()
	for i := int64(0); i < 1<<20; i++ {
		o, err := route.OwnerOfKey([]any{i}, names)
		if err != nil {
			t.Fatalf("OwnerOfKey(%d): %v", i, err)
		}
		if o == want {
			return i
		}
	}
	t.Fatalf("no key routes to owner %d of %v", want, names)
	return 0
}

// ownerKeys returns n distinct int64 keys that all route to owner index want.
func ownerKeys(t *testing.T, names []string, want, n int) []int64 {
	t.Helper()
	out := make([]int64, 0, n)
	for i := int64(0); len(out) < n; i++ {
		o, err := route.OwnerOfKey([]any{i}, names)
		if err != nil {
			t.Fatalf("OwnerOfKey(%d): %v", i, err)
		}
		if o == want {
			out = append(out, i)
		}
	}
	return out
}

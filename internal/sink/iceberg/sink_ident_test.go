package iceberg

import (
	"testing"

	"github.com/apache/iceberg-go/table"
)

// #189: a dotted target is the full path — namespace levels then the table —
// so a multi-level namespace is not mis-split into a table named "b.c".
func TestIdentMultiLevelNamespace(t *testing.T) {
	s := &Sink{ns: "raw"}
	cases := []struct {
		target string
		want   table.Identifier
	}{
		{"a.b.orders", table.Identifier{"a", "b", "orders"}},
		{"raw.orders", table.Identifier{"raw", "orders"}},
		{"orders", table.Identifier{"raw", "orders"}},
	}
	for _, c := range cases {
		if got := s.ident(c.target); !equalIdent(got, c.want) {
			t.Fatalf("ident(%q) = %v, want %v", c.target, got, c.want)
		}
	}
	// A multi-level default namespace applies to a bare name.
	s2 := &Sink{ns: "a.b"}
	if got := s2.ident("orders"); !equalIdent(got, table.Identifier{"a", "b", "orders"}) {
		t.Fatalf("ident(bare, ns=a.b) = %v, want [a b orders]", got)
	}
}

func equalIdent(a, b table.Identifier) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

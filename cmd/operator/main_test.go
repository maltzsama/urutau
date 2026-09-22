package main

import "testing"

func TestNamespaceConfig(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{"empty watches all", "", nil},
		{"whitespace only", "   ", nil},
		{"single", "team-a", []string{"team-a"}},
		{"trimmed and split", " team-a , team-b ", []string{"team-a", "team-b"}},
		{"skips empty segments", "team-a,,team-b,", []string{"team-a", "team-b"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := namespaceConfig(tt.in)
			if len(got) != len(tt.want) {
				t.Fatalf("namespaceConfig(%q) = %v, want %v", tt.in, got, tt.want)
			}
			for _, ns := range tt.want {
				if _, ok := got[ns]; !ok {
					t.Fatalf("namespaceConfig(%q) missing %q (got %v)", tt.in, ns, got)
				}
			}
		})
	}
}

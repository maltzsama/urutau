package memlimit

import "testing"

func TestParse(t *testing.T) {
	for in, want := range map[string]int64{
		"3221225472\n":          3221225472, // 3 GiB
		"max\n":                 0,
		"":                      0,
		"9223372036854771712\n": 0, // cgroup v1 "no limit"
		"garbage":               0,
	} {
		got, ok := parse(in)
		if got != want || ok != (want != 0) {
			t.Errorf("parse(%q) = %d, %v; want %d", in, got, ok, want)
		}
	}
}

func TestApplyLeavesAnExplicitGOMEMLIMIT(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "1GiB")
	if got := Apply(nil); got != 0 {
		t.Fatalf("Apply set %d over an explicit GOMEMLIMIT", got)
	}
}

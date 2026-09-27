package memlimit

import (
	"math"
	"os"
	"runtime/debug"
	"testing"
)

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

// A host exposing both cgroup versions may read "max" in one and the real
// limit in the other: an unlimited file is skipped, not the end of the search.
func TestApplySkipsAnUnlimitedFile(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "")
	dir := t.TempDir()
	unlimited, limited := dir+"/v2", dir+"/v1"
	if err := os.WriteFile(unlimited, []byte("max\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(limited, []byte("1073741824\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	saved := cgroupFiles
	cgroupFiles = []string{unlimited, limited}
	t.Cleanup(func() {
		cgroupFiles = saved
		debug.SetMemoryLimit(math.MaxInt64)
	})
	gib := float64(1 << 30)
	if got, want := Apply(nil), int64(gib*Fraction); got != want {
		t.Fatalf("Apply = %d, want %d (90%% of the second file's 1 GiB)", got, want)
	}
}

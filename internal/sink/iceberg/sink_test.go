package iceberg

import "testing"

// targetFileSizeFrom is the bridge from the spec's byte-size spelling
// ("128Mi") to the int64 bytes WithTargetFileSize wants. A regression here is
// silent — strconv.ParseInt("128Mi") errors and the override is dropped, so
// sink.defaults.targetFileSize would validate and then do nothing — so this
// pins the grammar.
func TestTargetFileSizeFrom(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"", 0}, // unset: no override
		{"128Mi", 128 * 1024 * 1024},
		{"512Mi", 512 * 1024 * 1024},
		{"1Gi", 1024 * 1024 * 1024},
		{"64Ki", 64 * 1024},
		{"4096", 4096}, // bare integer = plain bytes
		{"0", 0},
		{"not-a-size", 0}, // defense: Validate rejects this first
	}
	for _, c := range cases {
		if got := targetFileSizeFrom(c.in); got != c.want {
			t.Errorf("targetFileSizeFrom(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

// Package memlimit gives the Go runtime the container's memory limit.
//
// Without GOMEMLIMIT the garbage collector only paces on GOGC: the heap may
// grow to twice the live data before a collection, whatever the cgroup
// allows. Under a sustained load a worker's heap climbed past its Pod limit
// and was OOM-killed while most of it was garbage (#437). Apply sets the
// runtime's soft limit to a fraction of the cgroup's, so the collector works
// harder as the limit nears instead of the kernel killing the process.
package memlimit

import (
	"log/slog"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
)

// Fraction of the cgroup limit given to the runtime: the rest covers memory
// the runtime does not count (goroutine stacks in flight, cgo, the kernel's
// page cache charged to the cgroup).
const Fraction = 0.9

// cgroupFiles are the memory limit files of cgroup v2 and v1.
var cgroupFiles = []string{
	"/sys/fs/cgroup/memory.max",
	"/sys/fs/cgroup/memory/memory.limit_in_bytes",
}

// Apply sets the runtime's memory limit from the cgroup's, unless GOMEMLIMIT
// is set (the runtime already honors it) or no limit is found. It returns the
// limit set, 0 when it set none.
func Apply(log *slog.Logger) int64 {
	if os.Getenv("GOMEMLIMIT") != "" {
		return 0
	}
	for _, f := range cgroupFiles {
		b, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		limit, ok := parse(string(b))
		if !ok {
			continue // unlimited or unreadable here: the other version may set one
		}
		set := int64(float64(limit) * Fraction)
		debug.SetMemoryLimit(set)
		if log != nil {
			log.Info("memory limit", "cgroup_bytes", limit, "runtime_bytes", set)
		}
		return set
	}
	return 0
}

// parse reads a cgroup memory limit: a byte count, or "max" (v2) or a value
// near MaxInt64 (v1) for none.
func parse(s string) (int64, bool) {
	s = strings.TrimSpace(s)
	if s == "" || s == "max" {
		return 0, false
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 || n >= 1<<60 {
		return 0, false
	}
	return n, true
}

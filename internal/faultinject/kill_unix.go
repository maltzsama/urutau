//go:build unix

package faultinject

import (
	"os"
	"syscall"
)

// kill ends the process the way a crash does: no deferred function and no
// signal handler runs.
//
// SIGKILL alone is not enough in a container. The engine runs as PID 1 there,
// and the kernel ignores a signal PID 1 sends to itself from inside its own
// PID namespace — the first e2e run of the fault points left the worker alive
// and blocked forever instead of crashed. os.Exit is the fallback: it also
// skips every defer, and it is what ends the process when SIGKILL was ignored.
func kill() {
	_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
	os.Exit(137) // 128+SIGKILL, the exit code a real SIGKILL would report
}

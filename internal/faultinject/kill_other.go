//go:build !unix

package faultinject

import "os"

// kill is the closest non-unix equivalent of SIGKILL: exit immediately,
// running no deferred functions.
func kill() {
	os.Exit(137)
}

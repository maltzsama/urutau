// Package version exposes build metadata injected via ldflags (see Makefile).
package version

var (
	Version = "dev"
	Commit  = "none"
	Date    = "unknown"
)

// String satisfies fmt.Stringer.
func String() string {
	return Version + " (" + Commit + ", " + Date + ")"
}

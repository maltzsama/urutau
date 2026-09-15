// Package version exposes build metadata injected via ldflags (see Makefile).
//
// Version is also the value release-please bumps on every release: the
// inline annotation below maps this literal to the `extra-files` entry in
// release-please-config.json, so a release PR updates it together with
// CHANGELOG.md. Keep the annotation on the same line as the literal.
package version

var (
	// Version is the release version. The Makefile overrides it at build
	// time via ldflags with the git tag, so an official binary always
	// reports the tag it was built from.
	Version = "0.1.0" // x-release-please-version
	Commit  = "none"
	Date    = "unknown"
)

// String satisfies fmt.Stringer.
func String() string {
	return Version + " (" + Commit + ", " + Date + ")"
}

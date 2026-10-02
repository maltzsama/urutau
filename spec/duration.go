package spec

import "time"

// positiveDuration reports whether s parses as a duration that is strictly
// positive. A zero or negative duration passes time.ParseDuration but is
// silently replaced by the default at point of use (ParseDurationOrDefault),
// so a spec that sets one looks configured and does nothing — reject it at
// validation instead (issue #503).
func positiveDuration(s string) bool {
	d, err := time.ParseDuration(s)
	return err == nil && d > 0
}

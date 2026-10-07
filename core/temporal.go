package core

import (
	"fmt"
	"strings"
	"time"
)

// Temporal text parsing: RFC3339, PostgreSQL's own text form, and the
// canonical KindTime rendering. Split out of cast.go for the size ratchet.

// dateLayout, naiveTimestampLayout and timeOfDayLayout are the source-native
// temporal renderings. The naive layout's trailing .999999999 makes the
// fraction optional and strips trailing zeros, so it alone covers a naive
// timestamp with or without a fraction — there is no separate layout for
// "no fraction".
//
// naiveTimestampTextLayout is the canonical OUTPUT for a naive timestamp:
// fixed nine-digit fraction, no zone (the zone does not exist in the data).
const (
	dateLayout               = "2006-01-02"
	naiveTimestampLayout     = "2006-01-02 15:04:05.999999999"
	naiveTimestampTextLayout = "2006-01-02 15:04:05.000000000"
	timeOfDayLayout          = "15:04:05.999999999"
)

// ParseTimestampText parses a temporal text into a time.Time: an RFC3339
// instant (zone preserved), a bare date (midnight), or a naive timestamp
// with an optional fraction.
func ParseTimestampText(s string) (time.Time, error) {
	if tm, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return tm, nil
	}
	if tm, err := time.Parse(dateLayout, s); err == nil {
		return tm, nil
	}
	// PostgreSQL's own text form: "YYYY-MM-DD HH:MM:SS[.ffffff][±HH[:MM[:SS]]]"
	// ('T' is not used, and the zone may be a bare hour). The snapshot
	// delivers a time.Time through the driver, but the CDC text form must
	// parse too or the first stream event fails forever (issue #560).
	for _, layout := range []string{
		naiveTimestampLayout,
		"2006-01-02 15:04:05.999999999-07",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999-07:00:00",
	} {
		if tm, err := time.Parse(layout, s); err == nil {
			return tm, nil
		}
	}
	return time.Time{}, fmt.Errorf("core: %q is not a temporal value", s)
}

// ParseTimeOfDayText parses "HH:MM:SS[.fraction]" into micros since midnight.
// A trailing zone (timetz, e.g. "12:00:00+02" or "12:00:00.5-03:30") is
// dropped: the canonical KindTime carries no zone (issue #560).
func ParseTimeOfDayText(s string) (int64, error) {
	s = strings.TrimSuffix(s, "Z")
	if i := strings.IndexAny(s, "+-"); i >= 0 {
		s = s[:i]
	}
	tm, err := time.Parse(timeOfDayLayout, s)
	if err != nil {
		return 0, fmt.Errorf("core: %q is not a time-of-day value", s)
	}
	return int64(tm.Hour())*3_600_000_000 + int64(tm.Minute())*60_000_000 +
		int64(tm.Second())*1_000_000 + int64(tm.Nanosecond())/1_000, nil
}

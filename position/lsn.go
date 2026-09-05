// Package position defines the replication position contract. This file
// implements the PostgreSQL log sequence number (LSN) position.
package position

import (
	"fmt"
	"strconv"
	"strings"
)

// LSN is a PostgreSQL log sequence number: a byte offset into the write-ahead
// log, canonically rendered as high/low hex ("0/1A2B3C"). As a resume
// position it means "everything committed at or before this offset".
type LSN uint64

// ParseLSN parses the canonical "high/low" hex form (e.g. "0/1A2B3C").
func ParseLSN(s string) (*LSN, error) {
	hi, lo, ok := strings.Cut(strings.TrimSpace(s), "/")
	if !ok {
		return nil, fmt.Errorf("position: parse lsn %q: want high/low hex form", s)
	}
	h, err := strconv.ParseUint(hi, 16, 32)
	if err != nil {
		return nil, fmt.Errorf("position: parse lsn %q: bad high part: %w", s, err)
	}
	l, err := strconv.ParseUint(lo, 16, 32)
	if err != nil {
		return nil, fmt.Errorf("position: parse lsn %q: bad low part: %w", s, err)
	}
	lsn := LSN(h<<32 | l)
	return &lsn, nil
}

// MustLSN is ParseLSN for configuration-time literals; it panics on error.
func MustLSN(s string) *LSN {
	l, err := ParseLSN(s)
	if err != nil {
		panic(err)
	}
	return l
}

// String renders the canonical high/low hex form.
func (l *LSN) String() string {
	return fmt.Sprintf("%X/%X", uint64(*l)>>32, uint64(*l)&0xFFFFFFFF)
}

// Compare orders LSNs numerically: a later offset is greater.
func (l *LSN) Compare(other Position) int {
	o, ok := other.(*LSN)
	if !ok {
		panic(fmt.Sprintf("position: cannot compare LSN to %T", other))
	}
	switch {
	case *l < *o:
		return -1
	case *l > *o:
		return 1
	default:
		return 0
	}
}

// Contains reports whether other marks no point after l: for a linear WAL,
// containment is "at or before".
func (l *LSN) Contains(other Position) bool {
	o, ok := other.(*LSN)
	if !ok {
		panic(fmt.Sprintf("position: cannot contain-check LSN against %T", other))
	}
	return *l >= *o
}

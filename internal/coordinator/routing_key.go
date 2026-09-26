package coordinator

import (
	"bytes"
	"cmp"
	"fmt"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/source"
)

// partitionOwner returns the index of the partition range containing key
// — the range r such that r.Low <= key < r.High (nil bounds are open).
// Ranges must be contiguous and ordered (as Partitions/the single-range
// default always produce); returns -1 only if no range matches, which
// never happens for a correctly resolved table. A key that cannot be
// ordered against the bounds is an error, never a guess.
func partitionOwner(ranges []source.Chunk, key []any) (int, error) {
	if len(ranges) == 1 {
		return 0, nil // the common, unpartitioned case — skip the comparison
	}
	for i, r := range ranges {
		if r.Low != nil {
			c, err := comparePK(key, r.Low)
			if err != nil {
				return -1, err
			}
			if c < 0 {
				continue
			}
		}
		if r.High != nil {
			c, err := comparePK(key, r.High)
			if err != nil {
				return -1, err
			}
			if c >= 0 {
				continue
			}
		}
		return i, nil
	}
	return -1, nil
}

// comparePK compares two same-shaped primary-key tuples column by
// column, the same row-constructor semantics the chunkers' own bounds
// comparisons use (lexicographic over the tuple). Supports the ordered
// scalar types a partition key can be: signed and unsigned integers,
// floats, and string/[]byte (partitioning today only supports a
// single-column key — see source.PartitionSource — so in practice these
// tuples always have exactly one element, but the comparison is written
// for the general tuple shape to match Chunk's own []any convention).
func comparePK(a, b []any) (int, error) {
	for i := 0; i < len(a) && i < len(b); i++ {
		c, err := compareScalar(a[i], b[i])
		if err != nil {
			return 0, err
		}
		if c != 0 {
			return c, nil
		}
	}
	return len(a) - len(b), nil
}

// compareScalar orders two key values the way the source's SQL orders
// them. Integers compare exactly, signed against unsigned included: float64
// cannot represent adjacent int64 values above 2^53, so a float round-trip
// would collapse distinct keys and boundaries and route a key to the wrong
// worker. A pair with no common ordering (a number against a string, say)
// is an error: a lexical fallback would order "100" before "50" and route
// silently to the wrong partition (issue #406).
func compareScalar(a, b any) (int, error) {
	if ai, aok := asInteger(a); aok {
		if bi, bok := asInteger(b); bok {
			return ai.compare(bi), nil
		}
	}
	if af, aok := core.AsFloat64(a); aok {
		if bf, bok := core.AsFloat64(b); bok {
			return cmp.Compare(af, bf), nil
		}
	}
	if as, aok := asBytes(a); aok {
		if bs, bok := asBytes(b); bok {
			return bytes.Compare(as, bs), nil
		}
	}
	return 0, fmt.Errorf("coordinator: cannot order partition key value %v (%T) against %v (%T)", a, a, b, b)
}

// integer is an exact integer of either signedness: a negative value is
// always signed, so neg plus the magnitude orders every int64 and uint64.
type integer struct {
	neg bool
	mag uint64 // |value|; for neg, the two's-complement magnitude
}

func (x integer) compare(y integer) int {
	switch {
	case x.neg && !y.neg:
		return -1
	case !x.neg && y.neg:
		return 1
	case x.neg: // both negative: the larger magnitude is the smaller value
		return cmp.Compare(y.mag, x.mag)
	default:
		return cmp.Compare(x.mag, y.mag)
	}
}

func signed(v int64) integer {
	if v < 0 {
		return integer{neg: true, mag: uint64(-(v + 1)) + 1} // -MinInt64 overflows int64
	}
	return integer{mag: uint64(v)}
}

// asInteger extracts an exact integer when v is an integer type. A float is
// not coerced here: a float64 that is integral may still be an
// approximation of a larger int64.
func asInteger(v any) (integer, bool) {
	switch t := v.(type) {
	case int64:
		return signed(t), true
	case int32:
		return signed(int64(t)), true
	case int16:
		return signed(int64(t)), true
	case int8:
		return signed(int64(t)), true
	case int:
		return signed(int64(t)), true
	case uint64:
		return integer{mag: t}, true
	case uint32:
		return integer{mag: uint64(t)}, true
	case uint16:
		return integer{mag: uint64(t)}, true
	case uint8:
		return integer{mag: uint64(t)}, true
	case uint:
		return integer{mag: uint64(t)}, true
	default:
		return integer{}, false
	}
}

// asBytes returns the bytes of a string or []byte key. Go orders strings
// bytewise, so both compare the same way.
func asBytes(v any) ([]byte, bool) {
	switch t := v.(type) {
	case string:
		return []byte(t), true
	case []byte:
		return t, true
	default:
		return nil, false
	}
}

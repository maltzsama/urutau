package routing

import (
	"fmt"

	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// Layout is the immutable snapshot of every table's partition layout: the
// owners in partition order and the key ranges they own, where owners[i]
// owns ranges[i]. It is never mutated after publication — a re-slice builds
// a new value and swaps the pointer, so a reader that loaded it keeps
// routing a whole batch by one consistent layout.
//
// O is the caller's handle on an owner. The layout only orders owners; what
// an owner is stays with the caller.
type Layout[O any] struct {
	owners map[string][]O
	ranges map[string][]source.Chunk
}

// NewLayout returns a layout over the given maps, which it takes over: the
// caller must not change them afterwards.
func NewLayout[O any](owners map[string][]O, ranges map[string][]source.Chunk) *Layout[O] {
	if owners == nil {
		owners = map[string][]O{}
	}
	if ranges == nil {
		ranges = map[string][]source.Chunk{}
	}
	return &Layout[O]{owners: owners, ranges: ranges}
}

// OwnersOf returns the owners of target and whether the table is routed.
func (l *Layout[O]) OwnersOf(target string) ([]O, bool) {
	if l == nil {
		return nil, false
	}
	o, ok := l.owners[target]
	return o, ok
}

// RangesOf returns the key ranges of target, owners[i] owning ranges[i].
func (l *Layout[O]) RangesOf(target string) []source.Chunk {
	if l == nil {
		return nil
	}
	return l.ranges[target]
}

// Tables returns the routed tables, in no particular order.
func (l *Layout[O]) Tables() []string {
	if l == nil {
		return nil
	}
	out := make([]string, 0, len(l.owners))
	for t := range l.owners {
		out = append(out, t)
	}
	return out
}

// clone copies the two maps one level deep. The slices are shared, never
// appended to in place: a re-slice replaces a table's slice wholesale, so a
// reader holding the previous snapshot still sees the previous slice.
func (l *Layout[O]) clone() *Layout[O] {
	out := &Layout[O]{owners: map[string][]O{}, ranges: map[string][]source.Chunk{}}
	if l == nil {
		return out
	}
	for k, v := range l.owners {
		out.owners[k] = v
	}
	for k, v := range l.ranges {
		out.ranges[k] = v
	}
	return out
}

// WithTable returns a copy of the layout in which target has these owners
// and ranges. The receiver is left as it was.
func (l *Layout[O]) WithTable(target string, owners []O, ranges []source.Chunk) *Layout[O] {
	out := l.clone()
	out.owners[target] = owners
	out.ranges[target] = ranges
	return out
}

// WithOwners returns a copy of the layout in which target has these owners,
// its ranges unchanged.
func (l *Layout[O]) WithOwners(target string, owners []O) *Layout[O] {
	out := l.clone()
	out.owners[target] = owners
	return out
}

// WithRanges returns a copy of the layout with every table's ranges replaced
// by ranges, which it takes over.
func (l *Layout[O]) WithRanges(ranges map[string][]source.Chunk) *Layout[O] {
	out := l.clone()
	if ranges == nil {
		ranges = map[string][]source.Chunk{}
	}
	out.ranges = ranges
	return out
}

// PartitionName derives partition p's worker group name, the same shape
// spec.Table.WorkerGroupNames produces at boot.
func PartitionName(pipeline, target string, p int) string {
	return fmt.Sprintf("%s-%d", spec.WorkerGroupPrefix(pipeline, target), p)
}

// Reslice returns the owner slice for n partitions, keeping the existing
// owners in place (minimal remap: only the boundary moves). A scale-out
// calls add for each partition it gains; add reports whether it created the
// owner or reused one. Reslice also returns the owners a scale-in drops and
// the ones add created, so a failure before the flip can roll exactly those
// back. cur is never written to.
func Reslice[O any](cur []O, n int, add func(p int) (owner O, created bool, err error)) (next, removed, created []O, err error) {
	if n <= len(cur) {
		return cur[:n:n], cur[n:], nil, nil
	}
	next = make([]O, len(cur), n)
	copy(next, cur)
	for p := len(cur); p < n; p++ {
		o, isNew, err := add(p)
		if err != nil {
			return nil, nil, nil, err
		}
		next = append(next, o)
		if isNew {
			created = append(created, o)
		}
	}
	return next, nil, created, nil
}

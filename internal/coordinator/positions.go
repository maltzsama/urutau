package coordinator

import (
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
)

// maxPositions returns the greatest of a source batch's per-partition high
// positions, parsing each exactly once. The coordinator records it as a staged
// cycle's committed position: sub-batches are ordered by partition, not by
// source position, so the batch maximum is the only position that safely
// covers the whole cycle (issue #492). An empty or unparsable candidate is
// skipped, and an Incomparable pair keeps the incumbent; no usable position
// yields "".
func maxPositions(src source.Source, highs []string) string {
	var best position.Position
	for _, s := range highs {
		if s == "" {
			continue
		}
		p, err := src.ParsePosition(s)
		if err != nil {
			continue
		}
		if best == nil {
			best = p
			continue
		}
		if c := p.Compare(best); c != position.Incomparable && c > 0 {
			best = p
		}
	}
	if best == nil {
		return ""
	}
	return best.String()
}

package snapshot

import (
	"github.com/maltzsama/urutau/internal/partition"
	"github.com/maltzsama/urutau/source"
)

// ClipChunksToRange keeps only the chunks that intersect partitionRange,
// clamping each kept chunk's own Low/High to the range's bounds so a chunk
// straddling the partition boundary never sends rows outside it. An empty
// partitionRange (the unpartitioned {} zero value) matches everything
// unchanged.
func ClipChunksToRange(chunks []source.Chunk, partitionRange source.Chunk) ([]source.Chunk, error) {
	if partitionRange.Low == nil && partitionRange.High == nil {
		return chunks, nil
	}
	var out []source.Chunk
	for _, ch := range chunks {
		if partitionRange.High != nil && ch.Low != nil {
			c, err := partition.ComparePK(ch.Low, partitionRange.High)
			if err != nil {
				return nil, err
			}
			if c >= 0 {
				continue // chunk starts at/after the range ends
			}
		}
		if partitionRange.Low != nil && ch.High != nil {
			c, err := partition.ComparePK(ch.High, partitionRange.Low)
			if err != nil {
				return nil, err
			}
			if c <= 0 {
				continue // chunk ends at/before the range starts — both are half-open [Low,High)
			}
		}
		clipped := ch
		if partitionRange.Low != nil {
			c := -1
			if ch.Low != nil {
				var err error
				if c, err = partition.ComparePK(ch.Low, partitionRange.Low); err != nil {
					return nil, err
				}
			}
			if c < 0 {
				clipped.Low = partitionRange.Low
			}
		}
		if partitionRange.High != nil {
			c := 1
			if ch.High != nil {
				var err error
				if c, err = partition.ComparePK(ch.High, partitionRange.High); err != nil {
					return nil, err
				}
			}
			if c > 0 {
				clipped.High = partitionRange.High
			}
		}
		out = append(out, clipped)
	}
	return out, nil
}

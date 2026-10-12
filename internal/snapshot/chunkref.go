package snapshot

import (
	"fmt"
	"slices"

	"github.com/maltzsama/urutau/source"
)

// ChunkRef is a snapshot chunk's durable id: its partition in the high bits,
// its index among that partition's clipped chunks in the low 20.
func ChunkRef(partition, index int) uint32 { return uint32(partition)<<20 | uint32(index) }

// PartitionLayout renders partition ranges as recorded with the progress.
func PartitionLayout(ranges []source.Chunk) string {
	return fmt.Sprintf("%d:%v", len(ranges), ranges)
}

// AllChunkRefs is every chunk of every partition. A clip failure or a chunk
// index that overflows the 20 bits a chunk id addresses is an error, not a
// silently dropped chunk (issue #559).
func AllChunkRefs(all []source.Chunk, ranges []source.Chunk) (map[uint32]bool, error) {
	todo := map[uint32]bool{}
	for p, r := range ranges {
		clipped, err := ClipChunksToRange(all, r)
		if err != nil {
			return nil, fmt.Errorf("snapshot: clip partition %d: %w", p, err)
		}
		if len(clipped) > 1<<20 {
			return nil, fmt.Errorf("snapshot: partition %d has %d chunks, over the %d a chunk id can address", p, len(clipped), 1<<20)
		}
		for i := range clipped {
			todo[ChunkRef(p, i)] = true
		}
	}
	return todo, nil
}

// SortedRefs lists chunk ids in order.
func SortedRefs(todo map[uint32]bool) []uint32 {
	out := make([]uint32, 0, len(todo))
	for id := range todo {
		out = append(out, id)
	}
	slices.Sort(out)
	return out
}

// PendingAfter is the chunks still to do once partition's chunk index is
// committed: later chunks of the same partition, and every chunk of the
// partitions after it, among todo.
func PendingAfter(todo map[uint32]bool, partition, index int) []uint32 {
	var out []uint32
	for _, id := range SortedRefs(todo) {
		p, i := int(id>>20), int(id&(1<<20-1))
		if p > partition || (p == partition && i > index) {
			out = append(out, id)
		}
	}
	return out
}

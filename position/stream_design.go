// Variable-keyspace positions — DESIGN, NOT IMPLEMENTED.
//
// This file records the design for the Kinesis position type before the
// contract changes. It is deliberately not wired anywhere yet; it exists so
// the decision is conscious and the shapes are agreed before a source that
// needs them lands.
//
// The problem. Offsets{Parts map[int32]int64} assumes a stable key set:
// Kafka partitions appear once and only grow. Kinesis shards SPLIT and
// MERGE: shards close and children appear with parent lineage, so the key
// space changes over time and reading order is mandatory (drain a parent
// fully before its child, or event order — and therefore upsert — breaks).
//
// Two consequences the current contract does not express:
//
//  1. A position recorded yesterday references shards that no longer exist.
//     Contains must treat a closed, fully-drained shard as satisfied, not
//     pending.
//  2. Sequence numbers are decimal strings up to 56 digits — they do not
//     fit int64, so the coordinate is a string.
//
// The shape:
//
//	type ShardPos struct {
//		Sequence string // string: a Kinesis sequence number does not fit int64
//		Closed   bool   // shard closed and fully drained
//		Parent   string // lineage, for ordering the read (parent before child)
//	}
//
//	type StreamPosition struct {
//		Stream string
//		Shards map[string]ShardPos
//	}
//
//	// Contains: every shard of other is covered; a Closed, drained shard
//	// counts as covered even if it no longer exists in the current space.
//	func (p *StreamPosition) Contains(other Position) bool
//
// Caps would declare the capabilities a monotonic-offset source (Kafka)
// lacks and a reshardable one (Kinesis) has:
//
//	type Caps struct {
//		ReshardableKeyspace bool // shards may appear/disappear (Kinesis)
//		MonotonicSequence   bool // the coordinate never reappears
//	}
//
// StreamPosition must implement position.Position (String/Compare/Contains)
// so the runner and coordinator consume it through the existing contract,
// with no type assertion.
package position

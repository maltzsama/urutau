package position

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Offsets is a Kafka consumer position: per-partition offsets for one or
// more topics. It implements Position with a PARTIAL order — two positions
// are ordered only when one contains the other; positions whose partitions
// diverge are Incomparable, and Meet gives their greatest lower bound.
//
// The topic is part of the key, not a single field: one reader subscribes to
// every topic its spec tables name, and partition numbers repeat across
// topics. Keying by partition alone collapsed orders/p0 and payments/p0 onto
// one entry, so whichever topic wrote last destroyed the other's offset and
// the resume either replayed or skipped.
//
// The wire form is JSON; the canonical text form is "topic:p0=10,p1=20",
// with multiple topics joined by ";" in topic order.
type Offsets struct {
	// Topics maps a topic name to its per-partition offsets.
	Topics map[string]map[int32]int64
}

// NewOffsets builds a position for a single topic.
func NewOffsets(topic string, parts map[int32]int64) *Offsets {
	return &Offsets{Topics: map[string]map[int32]int64{topic: parts}}
}

// Set records the offset of one partition, creating the topic entry when it
// is the first partition seen for that topic.
func (o *Offsets) Set(topic string, partition int32, offset int64) {
	if o.Topics == nil {
		o.Topics = make(map[string]map[int32]int64)
	}
	parts, ok := o.Topics[topic]
	if !ok {
		parts = make(map[int32]int64)
		o.Topics[topic] = parts
	}
	parts[partition] = offset
}

// Clone returns a deep copy. Callers hand positions to other goroutines
// (the reader keeps mutating its own while a batch carries a snapshot), so
// sharing the inner maps would race.
func (o *Offsets) Clone() *Offsets {
	if o == nil {
		return nil
	}
	c := &Offsets{Topics: make(map[string]map[int32]int64, len(o.Topics))}
	for topic, parts := range o.Topics {
		cp := make(map[int32]int64, len(parts))
		for p, off := range parts {
			cp[p] = off
		}
		c.Topics[topic] = cp
	}
	return c
}

// topicNames returns the topics in sorted order, so every rendering and
// comparison walks them the same way.
func (o *Offsets) topicNames() []string {
	names := make([]string, 0, len(o.Topics))
	for t := range o.Topics {
		names = append(names, t)
	}
	sort.Strings(names)
	return names
}

// ParseOffsets parses the canonical text form "topic:p0=10,p1=20", with
// multiple topics separated by ";".
func ParseOffsets(s string) (*Offsets, error) {
	o := &Offsets{Topics: make(map[string]map[int32]int64)}
	if s == "" {
		return o, nil
	}
	for _, topicStr := range strings.Split(s, ";") {
		topic, partsStr, ok := strings.Cut(topicStr, ":")
		if !ok {
			return nil, fmt.Errorf("position: parse offsets %q: want topic:p0=offset,... form", s)
		}
		parts := make(map[int32]int64)
		if partsStr != "" {
			for _, pair := range strings.Split(partsStr, ",") {
				k, v, ok := strings.Cut(pair, "=")
				if !ok {
					return nil, fmt.Errorf("position: parse offsets %q: bad pair %q", s, pair)
				}
				p, err := strconv.ParseInt(strings.TrimPrefix(k, "p"), 10, 32)
				if err != nil {
					return nil, fmt.Errorf("position: parse offsets %q: bad partition %q: %w", s, k, err)
				}
				offset, err := strconv.ParseInt(v, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("position: parse offsets %q: bad offset %q: %w", s, v, err)
				}
				parts[int32(p)] = offset
			}
		}
		if _, exists := o.Topics[topic]; exists {
			return nil, fmt.Errorf("position: parse offsets %q: duplicate topic %q", s, topic)
		}
		o.Topics[topic] = parts
	}
	return o, nil
}

// String renders the canonical text form.
func (o *Offsets) String() string {
	if o == nil {
		return ""
	}
	var b strings.Builder
	for i, topic := range o.topicNames() {
		if i > 0 {
			b.WriteByte(';')
		}
		b.WriteString(topic)
		b.WriteByte(':')
		parts := o.Topics[topic]
		keys := make([]int32, 0, len(parts))
		for p := range parts {
			keys = append(keys, p)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		for j, p := range keys {
			if j > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, "p%d=%d", p, parts[p])
		}
	}
	return b.String()
}

// Compare implements the PARTIAL order of Offsets: containment. A position
// is greater when it contains the other (every partition of every topic at
// or beyond the other's offset), and the two are equal when each contains
// the other. Anything else has no defined order and returns Incomparable —
// notably positions that diverge ({p0:100,p1:5} vs {p0:5,p1:100}), whether
// the divergence is within one topic or across topics.
//
// It does NOT fall back to the maximum partition offset. That heuristic
// reported 0 for divergent positions, which made covered() (coordinator/
// flow.go) treat an uncovered batch as covered and skip data that was never
// replayed. Incomparable is the value every caller already handles as
// "cannot decide — never skip".
//
// Because the order is partial, Min/MinSafe cannot pick a safe minimum by
// selection alone; Meet computes the greatest lower bound instead.
func (o *Offsets) Compare(other Position) int {
	oth, ok := other.(*Offsets)
	if !ok {
		panic(fmt.Sprintf("position: cannot compare Offsets to %T", other))
	}
	switch fwd, rev := o.Contains(oth), oth.Contains(o); {
	case fwd && rev:
		return 0
	case fwd:
		return 1
	case rev:
		return -1
	default:
		return Incomparable
	}
}

// Meet returns the greatest lower bound of o and other: the per-partition
// minimum offset, for every topic both sides carry. It is the only safe
// resume point when two positions diverge — selecting either one would
// resume PAST the partitions where the other is behind, and that data would
// never be read.
//
// A partition (or topic) missing from one side is absent from the meet: an
// unknown offset cannot be bounded, so the resume must not assume progress
// on it.
func (o *Offsets) Meet(other Position) (Position, bool) {
	oth, ok := other.(*Offsets)
	if !ok {
		return nil, false
	}
	met := &Offsets{Topics: make(map[string]map[int32]int64)}
	for topic, mine := range o.Topics {
		theirs, ok := oth.Topics[topic]
		if !ok {
			continue
		}
		parts := make(map[int32]int64, len(mine))
		for p, myOff := range mine {
			theirOff, ok := theirs[p]
			if !ok {
				continue
			}
			parts[p] = min(myOff, theirOff)
		}
		met.Topics[topic] = parts
	}
	return met, true
}

// Contains returns true when o's offsets are all at least as large as
// other's, for every partition of every topic present in other. Topics and
// partitions present in o but absent in other are ignored (future work).
func (o *Offsets) Contains(other Position) bool {
	oth, ok := other.(*Offsets)
	if !ok {
		return false
	}
	for topic, theirs := range oth.Topics {
		mine, ok := o.Topics[topic]
		if !ok && len(theirs) > 0 {
			return false
		}
		for p, off := range theirs {
			if my, ok := mine[p]; !ok || my < off {
				return false
			}
		}
	}
	return true
}

// offsetsJSON is the wire shape: topics, each with sorted partitions.
type offsetsJSON struct {
	Topics []topicJSON `json:"topics"`
}

type topicJSON struct {
	Topic string     `json:"topic"`
	Parts []partJSON `json:"parts"`
}

type partJSON struct {
	Partition int32 `json:"partition"`
	Offset    int64 `json:"offset"`
}

// MarshalJSON serializes the offsets as a JSON object.
func (o *Offsets) MarshalJSON() ([]byte, error) {
	out := offsetsJSON{Topics: make([]topicJSON, 0, len(o.Topics))}
	for _, topic := range o.topicNames() {
		parts := o.Topics[topic]
		tj := topicJSON{Topic: topic, Parts: make([]partJSON, 0, len(parts))}
		for p, off := range parts {
			tj.Parts = append(tj.Parts, partJSON{Partition: p, Offset: off})
		}
		sort.Slice(tj.Parts, func(i, j int) bool { return tj.Parts[i].Partition < tj.Parts[j].Partition })
		out.Topics = append(out.Topics, tj)
	}
	return json.Marshal(out)
}

// UnmarshalJSON deserializes the offsets from a JSON object. It detects
// the legacy {"topic":"...","parts":[...]} format and migrates it into the
// canonical {"topics":[{topic,parts}]} shape.
func (o *Offsets) UnmarshalJSON(data []byte) error {
	var raw offsetsJSON
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	// Detect legacy format: {"topic":"orders","parts":[{"partition":0,"offset":10}]}
	if len(raw.Topics) == 0 {
		var legacy struct {
			Topic string `json:"topic"`
			Parts []struct {
				Partition int32 `json:"partition"`
				Offset    int64 `json:"offset"`
			} `json:"parts"`
		}
		if err := json.Unmarshal(data, &legacy); err == nil && legacy.Topic != "" {
			o.Topics = make(map[string]map[int32]int64, 1)
			parts := make(map[int32]int64, len(legacy.Parts))
			for _, p := range legacy.Parts {
				parts[p.Partition] = p.Offset
			}
			o.Topics[legacy.Topic] = parts
			return nil
		}
	}
	o.Topics = make(map[string]map[int32]int64, len(raw.Topics))
	for _, tj := range raw.Topics {
		parts := make(map[int32]int64, len(tj.Parts))
		for _, p := range tj.Parts {
			parts[p.Partition] = p.Offset
		}
		o.Topics[tj.Topic] = parts
	}
	return nil
}

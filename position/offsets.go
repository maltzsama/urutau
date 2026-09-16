package position

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Offsets is a Kafka consumer position: one topic with per-partition
// offsets. It implements Position with a PARTIAL order — two positions of
// the same topic are ordered only when one contains the other; positions
// whose partitions diverge are Incomparable, and Meet gives their greatest
// lower bound. The wire form is JSON; the canonical text form is
// "topic:p0=10,p1=20".
type Offsets struct {
	Topic string
	Parts map[int32]int64
}

// ParseOffsets parses the canonical text form "topic:p0=10,p1=20".
func ParseOffsets(s string) (*Offsets, error) {
	topic, partsStr, ok := strings.Cut(s, ":")
	if !ok {
		return nil, fmt.Errorf("position: parse offsets %q: want topic:p0=offset,... form", s)
	}
	parts := make(map[int32]int64)
	if partsStr == "" {
		return &Offsets{Topic: topic, Parts: parts}, nil
	}
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
	return &Offsets{Topic: topic, Parts: parts}, nil
}

// String renders the canonical text form.
func (o *Offsets) String() string {
	if o == nil {
		return ""
	}
	keys := make([]int32, 0, len(o.Parts))
	for p := range o.Parts {
		keys = append(keys, p)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	var b strings.Builder
	b.WriteString(o.Topic)
	b.WriteByte(':')
	for i, p := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "p%d=%d", p, o.Parts[p])
	}
	return b.String()
}

// Compare implements the PARTIAL order of Offsets: containment. A position
// is greater when it contains the other (every partition at or beyond the
// other's offset), and the two are equal when each contains the other.
// Anything else has no defined order and returns Incomparable — notably two
// positions whose partitions diverge ({p0:100,p1:5} vs {p0:5,p1:100}), and
// any two positions of different topics.
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
	if o.Topic != oth.Topic {
		return Incomparable
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
// minimum offset. It is the only safe resume point when two partitioned
// positions diverge — selecting either one would resume PAST the partitions
// where the other is behind, and that data would never be read.
//
// A partition missing from one side is absent from the meet: an unknown
// offset cannot be bounded, so the resume must not assume progress on it.
// Offsets of different topics have no meet.
func (o *Offsets) Meet(other Position) (Position, bool) {
	oth, ok := other.(*Offsets)
	if !ok || o.Topic != oth.Topic {
		return nil, false
	}
	parts := make(map[int32]int64, len(o.Parts))
	for p, mine := range o.Parts {
		theirs, ok := oth.Parts[p]
		if !ok {
			continue
		}
		parts[p] = min(mine, theirs)
	}
	return &Offsets{Topic: o.Topic, Parts: parts}, true
}

// Contains returns true when o's offsets are all at least as large as
// other's for every partition present in other. Partitions present in o
// but absent in other are ignored (future partitions).
func (o *Offsets) Contains(other Position) bool {
	oth, ok := other.(*Offsets)
	if !ok {
		return false
	}
	if o.Topic != oth.Topic {
		return false
	}
	for p, off := range oth.Parts {
		if my, ok := o.Parts[p]; !ok || my < off {
			return false
		}
	}
	return true
}

// MarshalJSON serializes the offsets as a JSON object.
func (o *Offsets) MarshalJSON() ([]byte, error) {
	type Part struct {
		Partition int32 `json:"partition"`
		Offset    int64 `json:"offset"`
	}
	parts := make([]Part, 0, len(o.Parts))
	for p, off := range o.Parts {
		parts = append(parts, Part{Partition: p, Offset: off})
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].Partition < parts[j].Partition })
	return json.Marshal(struct {
		Topic string `json:"topic"`
		Parts []Part `json:"parts"`
	}{Topic: o.Topic, Parts: parts})
}

// UnmarshalJSON deserializes the offsets from a JSON object.
func (o *Offsets) UnmarshalJSON(data []byte) error {
	type Part struct {
		Partition int32 `json:"partition"`
		Offset    int64 `json:"offset"`
	}
	var raw struct {
		Topic string `json:"topic"`
		Parts []Part `json:"parts"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	o.Topic = raw.Topic
	o.Parts = make(map[int32]int64, len(raw.Parts))
	for _, p := range raw.Parts {
		o.Parts[p.Partition] = p.Offset
	}
	return nil
}

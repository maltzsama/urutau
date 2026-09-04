// Raw lands the message envelope without interpreting the payload. The
// record value is carried as an opaque string: no JSON parsing, no schema
// inference. This is the bronze-landing decoder — the point is to NOT
// understand the content, so payloads that are not even valid JSON land
// verbatim. The transport envelope (topic, partition, offset, timestamp,
// key, headers) is attached by the reader, not here.
package decoder

import (
	"github.com/maltzsama/urutau/internal/change"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Raw is the passthrough decoder. Every message is an insert — raw landing
// has no update or delete semantics, the log is the data. A tombstone
// (nil value) becomes a row with a NULL payload, which is the faithful
// record of the fact.
type Raw struct{}

func (d *Raw) Decode(r *kgo.Record) ([]change.Change, error) {
	c := change.Change{
		Op:    change.OpInsert,
		After: map[string]any{"payload": rawValue(r.Value)},
	}
	return []change.Change{c}, nil
}

// rawValue carries a nil value as a NULL payload (tombstone) rather than an
// empty string: the absence of a payload is a fact in its own right.
func rawValue(v []byte) any {
	if v == nil {
		return nil
	}
	return string(v)
}

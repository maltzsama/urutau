// Field extraction turns a decoded payload document into the flat set of
// columns a table declared. It is shared by the raw and avro decoders: both
// end up holding a map[string]any and both need to project it onto the
// declared shape. What differs is the source of that map (a JSON parse
// versus an Avro unmarshal) and whether a raw blob accompanies it — neither
// of which this file knows about.
package decoder

import (
	"fmt"
	"strings"
)

// Field is one declared column and where to read it from.
type Field struct {
	// Name is the destination column.
	Name string
	// Path is the payload path, dot-separated for nested documents. Empty
	// means Name is the path.
	Path string
	// Required fails the record when the field is absent, rather than
	// landing NULL.
	Required bool
}

// path returns the lookup path, defaulting to the column name.
func (f Field) path() string {
	if f.Path != "" {
		return f.Path
	}
	return f.Name
}

// MissFn is called when a non-required declared field is absent from a
// payload, so the caller can count it. An absent field is either contract
// optionality or a producer that changed shape, and the counter is what
// makes the second case visible. The decoders close over the record's topic
// before handing this to Extract, so the callback signature here stays a
// plain column name — the pure unit is "document + declared fields", the
// topic is the caller's context, not this function's.
type MissFn func(column string)

// TopicExtraction is one topic's field-extraction declaration: which fields
// to keep, and (for raw only) whether to also keep the full payload
// alongside them.
type TopicExtraction struct {
	Fields      []Field
	KeepPayload bool
}

// ErrFieldMissing marks a required field the payload does not carry.
type ErrFieldMissing struct {
	Column string
	Path   string
}

func (e *ErrFieldMissing) Error() string {
	return fmt.Sprintf("extract: required field %q (path %q) is absent from the payload", e.Column, e.Path)
}

// Extract projects doc onto fields. A declared field that the document does
// not carry lands as a nil entry (NULL) and reports a miss, unless it is
// Required, which fails the record instead.
//
// The result always carries every declared column, present or not: a batch
// encodes against a fixed schema, so a column that appears only in some rows
// would make the shape depend on the data.
func Extract(doc map[string]any, fields []Field, miss MissFn) (map[string]any, error) {
	out := make(map[string]any, len(fields))
	for _, f := range fields {
		v, ok := lookup(doc, f.path())
		if !ok {
			if f.Required {
				return nil, &ErrFieldMissing{Column: f.Name, Path: f.path()}
			}
			if miss != nil {
				miss(f.Name)
			}
			out[f.Name] = nil
			continue
		}
		out[f.Name] = v
	}
	return out, nil
}

// lookup walks a dot-separated path into nested documents. A path that runs
// into a non-document before it ends is absent, not an error: the producer
// changed the shape, which is the same condition as the field being missing
// and is reported the same way.
func lookup(doc map[string]any, path string) (any, bool) {
	cur := doc
	parts := strings.Split(path, ".")
	for i, part := range parts {
		v, ok := cur[part]
		if !ok {
			return nil, false
		}
		if i == len(parts)-1 {
			return v, true
		}
		next, ok := v.(map[string]any)
		if !ok {
			return nil, false
		}
		cur = next
	}
	return nil, false
}

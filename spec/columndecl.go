package spec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/maltzsama/urutau/core"
)

// ColumnDecl is one entry of Table.Columns: either a scalar type string
// ("int64", "decimal(20,4)", ...), unmarshaled the same way as Table.Cast,
// or a composite declaration — struct/list/map — nested to any depth. The
// two shapes share the wire the same way Kind's scalar and composite
// dimensions do (core/table.go): a scalar is a plain JSON string, a
// composite is a JSON object naming exactly one of "struct", "list", "map".
type ColumnDecl struct {
	// Scalar holds the type string when this entry is a plain JSON string.
	// Empty when Struct/List/Map is set.
	Scalar string
	// Struct holds the field declarations when this entry is
	// {"struct": {name: decl, ...}}. Nil otherwise.
	Struct map[string]ColumnDecl
	// List holds the element declaration when this entry is
	// {"list": decl}. Nil otherwise.
	List *ColumnDecl
	// Map holds the key/value declarations when this entry is
	// {"map": {"key": decl, "value": decl}}. Nil otherwise.
	Map *MapDecl

	// From is the payload path this column is extracted from, for message
	// sources whose payload is a document rather than a row: "totals.grand"
	// reads a nested field. Empty means the column name is the path, which
	// is the common case. Only meaningful for Kafka raw/avro.
	From string
	// Required fails the record when the declared field is absent from the
	// payload, instead of landing NULL. A field can be missing because the
	// contract says it is optional or because the producer broke, and only
	// the operator knows which — so it is declared per column rather than
	// being one global policy.
	Required bool
}

// MapDecl is the {"key": decl, "value": decl} body of a map declaration.
type MapDecl struct {
	Key   ColumnDecl `json:"key"`
	Value ColumnDecl `json:"value"`
}

// compositeBody is the wire shape of a non-scalar ColumnDecl. Exactly one
// of struct/list/map names the shape, or "type" gives a scalar that needs
// the extraction attributes an inline string cannot carry. From and Required
// may accompany any of them.
type compositeBody struct {
	Struct   map[string]ColumnDecl `json:"struct,omitempty"`
	List     *ColumnDecl           `json:"list,omitempty"`
	Map      *MapDecl              `json:"map,omitempty"`
	Type     string                `json:"type,omitempty"`
	From     string                `json:"from,omitempty"`
	Required bool                  `json:"required,omitempty"`
}

// UnmarshalJSON accepts a plain string (scalar) or an object with exactly
// one of "struct", "list", "map" (composite). Any other shape — an empty
// object, an object with more than one key, an object with an unknown key,
// or a JSON type that is neither string nor object — is rejected: the
// grammar is closed, matching every other textual field in this package
// (audit #10).
func (d *ColumnDecl) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		*d = ColumnDecl{Scalar: s}
		return nil
	}

	var body compositeBody
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		return fmt.Errorf("spec: column: %q is neither a scalar type string nor a struct/list/map declaration: %w", string(b), err)
	}
	set := 0
	if body.Struct != nil {
		set++
	}
	if body.List != nil {
		set++
	}
	if body.Map != nil {
		set++
	}
	if body.Type != "" {
		set++
	}
	if set != 1 {
		return fmt.Errorf("spec: column: exactly one of type, struct, list, map is required, got %d", set)
	}
	*d = ColumnDecl{
		Scalar:   body.Type,
		Struct:   body.Struct,
		List:     body.List,
		Map:      body.Map,
		From:     body.From,
		Required: body.Required,
	}
	return nil
}

// MarshalJSON renders a plain scalar back to its bare string, and anything
// else back to its object form — the inverse of UnmarshalJSON. A scalar that
// carries extraction attributes cannot be a bare string, so it renders as
// {"type": ..., "from": ...}.
func (d ColumnDecl) MarshalJSON() ([]byte, error) {
	body := compositeBody{
		Struct:   d.Struct,
		List:     d.List,
		Map:      d.Map,
		From:     d.From,
		Required: d.Required,
	}
	switch {
	case d.Struct != nil || d.List != nil || d.Map != nil:
		return json.Marshal(body)
	case d.From != "" || d.Required:
		body.Type = d.Scalar
		return json.Marshal(body)
	default:
		return json.Marshal(d.Scalar)
	}
}

// Resolve turns the declaration into a canonical core.ColumnType,
// recursively for struct/list/map. A scalar resolves through
// core.ParseColumnType — the same grammar Table.Cast already uses — so
// "int64", "decimal(20,4)" etc. behave identically whether they appear at
// the top level or nested inside a struct/list/map.
func (d ColumnDecl) Resolve() (core.ColumnType, error) {
	switch {
	case d.Struct != nil:
		names := make([]string, 0, len(d.Struct))
		for name := range d.Struct {
			names = append(names, name)
		}
		sort.Strings(names)
		fields := make([]core.Column, 0, len(d.Struct))
		for _, name := range names {
			ft, err := d.Struct[name].Resolve()
			if err != nil {
				return core.ColumnType{}, fmt.Errorf("struct field %q: %w", name, err)
			}
			fields = append(fields, core.Column{Name: name, Type: ft})
		}
		return core.ColumnType{Kind: core.KindStruct, Fields: fields}, nil
	case d.List != nil:
		et, err := d.List.Resolve()
		if err != nil {
			return core.ColumnType{}, fmt.Errorf("list element: %w", err)
		}
		return core.ColumnType{Kind: core.KindList, Elem: &et}, nil
	case d.Map != nil:
		kt, err := d.Map.Key.Resolve()
		if err != nil {
			return core.ColumnType{}, fmt.Errorf("map key: %w", err)
		}
		vt, err := d.Map.Value.Resolve()
		if err != nil {
			return core.ColumnType{}, fmt.Errorf("map value: %w", err)
		}
		return core.ColumnType{Kind: core.KindMap, KeyType: &kt, ValueType: &vt}, nil
	default:
		ct, err := core.ParseColumnType(d.Scalar)
		if err != nil {
			return core.ColumnType{}, err
		}
		return ct, nil
	}
}

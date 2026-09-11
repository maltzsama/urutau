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
}

// MapDecl is the {"key": decl, "value": decl} body of a map declaration.
type MapDecl struct {
	Key   ColumnDecl `json:"key"`
	Value ColumnDecl `json:"value"`
}

// compositeBody is the wire shape of a composite ColumnDecl: exactly one
// of the three keys is present.
type compositeBody struct {
	Struct map[string]ColumnDecl `json:"struct,omitempty"`
	List   *ColumnDecl           `json:"list,omitempty"`
	Map    *MapDecl              `json:"map,omitempty"`
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
	if set != 1 {
		return fmt.Errorf("spec: column: exactly one of struct, list, map is required, got %d", set)
	}
	*d = ColumnDecl{Struct: body.Struct, List: body.List, Map: body.Map}
	return nil
}

// MarshalJSON renders a scalar back to its bare string, and a composite
// back to its single-key object — the inverse of UnmarshalJSON.
func (d ColumnDecl) MarshalJSON() ([]byte, error) {
	switch {
	case d.Struct != nil:
		return json.Marshal(compositeBody{Struct: d.Struct})
	case d.List != nil:
		return json.Marshal(compositeBody{List: d.List})
	case d.Map != nil:
		return json.Marshal(compositeBody{Map: d.Map})
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

package worker

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
)

// SchemaDrift is emitted when the batcher detects a column in the change
// stream that was not present at introspection time.
type SchemaDrift struct {
	Table  string
	Column string
	Kind   string // "added", "removed"
}

// schemaDrift returns the first data column the batch carries a VALUE in
// that the known schema lacks. An all-null extra column is a padding
// artifact of schema merging (e.g. a reference column declared but not yet
// populated), not a source column; only a column the batch actually
// populates counts as drift.
//
// Declared struct columns are descended into: a field added INSIDE a struct
// is drift too, reported by its dotted path ("address.complement"). Without
// that, a nested addition was silently dropped while an equivalent
// top-level column stopped the table — the opposite of the fail-closed
// contract, and invisible to the operator.
func schemaDrift(b *dataplane.Batch, schema core.Schema) (SchemaDrift, bool, error) {
	rec := b.Record
	for i := range rec.Schema().NumFields() {
		name := rec.Schema().Field(i).Name
		if isMetadataName(name) {
			continue
		}
		col := rec.Column(i)
		declared, ok := schema.Column(name)
		if !ok {
			if col.NullN() == col.Len() {
				continue // entirely null — a padding artifact, no source value
			}
			return SchemaDrift{Column: name, Kind: "added"}, true, nil
		}
		if declared.Type.Kind != core.KindStruct {
			continue
		}
		st, ok := col.(*array.Struct)
		if !ok {
			continue // declared a struct but not carried as one: a cast concern
		}
		if d, hit := nestedDrift(name, st, declared.Type.Fields); hit {
			return d, true, nil
		}
	}
	return SchemaDrift{}, false, nil
}

// nestedDrift descends one struct level, comparing the record's child fields
// against the declared ones and recursing into declared child structs. path
// is the dotted prefix of the struct being descended.
//
// The null rule matches the top level: an all-null extra field is padding,
// not a source value.
//
// The leading null-struct check is defensive. A builder nulls a struct's
// children along with the parent, so the per-field check below already
// covers that case; but Arrow permits a null parent over populated child
// buffers, and those values belong to no row.
func nestedDrift(path string, st *array.Struct, declared []core.Column) (SchemaDrift, bool) {
	if st.IsNull(0) {
		return SchemaDrift{}, false
	}
	stType, ok := st.DataType().(*arrow.StructType)
	if !ok {
		return SchemaDrift{}, false
	}
	for i, f := range stType.Fields() {
		full := path + "." + f.Name
		child := st.Field(i)
		var decl *core.Column
		for j := range declared {
			if declared[j].Name == f.Name {
				decl = &declared[j]
				break
			}
		}
		if decl == nil {
			if child.NullN() == child.Len() {
				continue // entirely null — a padding artifact, no source value
			}
			return SchemaDrift{Column: full, Kind: "added"}, true
		}
		if decl.Type.Kind != core.KindStruct {
			continue
		}
		nested, ok := child.(*array.Struct)
		if !ok {
			continue
		}
		if d, hit := nestedDrift(full, nested, decl.Type.Fields); hit {
			return d, true
		}
	}
	return SchemaDrift{}, false
}

// isMetadataName reports the reserved wire metadata columns.
func isMetadataName(name string) bool {
	switch name {
	case "__op", "__pos", "__commit_ts", "__ingest_ts", "__snapshot", "__phase":
		return true
	}
	return false
}

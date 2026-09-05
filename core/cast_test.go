package core

import (
	"testing"
)

func TestParseCastTarget(t *testing.T) {
	tests := []struct {
		input string
		want  CastTarget
		err   bool
	}{
		{"string", CastTarget{Type: ColumnType{Kind: KindString}}, false},
		{"int64", CastTarget{Type: ColumnType{Kind: KindInt64}}, false},
		{"float64", CastTarget{Type: ColumnType{Kind: KindFloat64}}, false},
		{"decimal(20,4)", CastTarget{Type: ColumnType{Kind: KindDecimal, Precision: 20, Scale: 4}}, false},
		{"timestamptz(assume_utc)", CastTarget{Type: ColumnType{Kind: KindTimestampTZ}, AssumeUTC: true}, false},
		{"uuid", CastTarget{Type: ColumnType{Kind: KindUUID}}, false},
		{"json", CastTarget{Type: ColumnType{Kind: KindJSON}}, false},
		{"unknown_type", CastTarget{}, true},
		{"decimal()", CastTarget{}, true},
	}
	for _, tt := range tests {
		got, err := ParseCastTarget(tt.input)
		if (err != nil) != tt.err {
			t.Errorf("ParseCastTarget(%q) error = %v, wantErr %v", tt.input, err, tt.err)
			continue
		}
		if !tt.err && got.Type.Kind != tt.want.Type.Kind {
			t.Errorf("ParseCastTarget(%q) kind = %v, want %v", tt.input, got.Type.Kind, tt.want.Type.Kind)
		}
	}
}

func TestCheckCastWidening(t *testing.T) {
	widening := []struct {
		src ColumnType
		dst ColumnType
	}{
		{ColumnType{Kind: KindInt32}, ColumnType{Kind: KindInt64}},
		{ColumnType{Kind: KindFloat32}, ColumnType{Kind: KindFloat64}},
		{ColumnType{Kind: KindString}, ColumnType{Kind: KindString}},
		{ColumnType{Kind: KindBool}, ColumnType{Kind: KindString}},
		{ColumnType{Kind: KindTimestampTZ}, ColumnType{Kind: KindString}},
	}
	for _, tt := range widening {
		err := CheckCast(tt.src, CastTarget{Type: tt.dst})
		if err != nil {
			t.Errorf("CheckCast(%+v → %+v) unexpected error: %v", tt.src, tt.dst, err)
		}
	}
}

func TestCheckCastNarrowingBlocked(t *testing.T) {
	narrowing := []struct {
		src ColumnType
		dst ColumnType
	}{
		{ColumnType{Kind: KindInt64}, ColumnType{Kind: KindInt32}},
		{ColumnType{Kind: KindFloat64}, ColumnType{Kind: KindFloat32}},
		{ColumnType{Kind: KindString}, ColumnType{Kind: KindInt64}},
	}
	for _, tt := range narrowing {
		err := CheckCast(tt.src, CastTarget{Type: tt.dst})
		if err == nil {
			t.Errorf("CheckCast(%+v → %+v) should error on narrowing", tt.src, tt.dst)
		}
	}
}

func TestCheckCastUnknownBypass(t *testing.T) {
	err := CheckCast(ColumnType{Kind: KindUnknown}, CastTarget{Type: ColumnType{Kind: KindString}})
	if err != nil {
		t.Errorf("CheckCast with KindUnknown source should not error (bypass): %v", err)
	}
}

func TestCheckCastTemporalReinterpret(t *testing.T) {
	err := CheckCast(
		ColumnType{Kind: KindTimestamp},
		CastTarget{Type: ColumnType{Kind: KindTimestampTZ}, AssumeUTC: true},
	)
	if err != nil {
		t.Errorf("timestamptz(assume_utc) should not error: %v", err)
	}
}

func TestConvertWidening(t *testing.T) {
	tests := []struct {
		src    ColumnType
		target CastTarget
		input  any
		want   any
	}{
		{ColumnType{Kind: KindInt32}, CastTarget{Type: ColumnType{Kind: KindInt64}}, int32(42), int64(42)},
		{ColumnType{Kind: KindFloat32}, CastTarget{Type: ColumnType{Kind: KindFloat64}}, float32(3.14), float64(float32(3.14))},
		{ColumnType{Kind: KindBool}, CastTarget{Type: ColumnType{Kind: KindString}}, true, "true"},
	}
	for _, tt := range tests {
		got, err := tt.target.Convert(tt.input)
		if err != nil {
			t.Errorf("Convert(%v → %+v) unexpected error: %v", tt.input, tt.target, err)
			continue
		}
		if got != tt.want {
			t.Errorf("Convert(%v → %+v) = %v, want %v", tt.input, tt.target, got, tt.want)
		}
	}
}

func TestConvertNilPassthrough(t *testing.T) {
	got, err := CastTarget{Type: ColumnType{Kind: KindString}}.Convert(nil)
	if err != nil {
		t.Errorf("Convert(nil) unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("Convert(nil) = %v, want nil", got)
	}
}

func TestConvertTypeMismatch(t *testing.T) {
	_, err := CastTarget{Type: ColumnType{Kind: KindInt64}}.Convert("not a number")
	if err == nil {
		t.Error("Convert with mismatched type should error")
	}
}

func TestCastPolicyResolve(t *testing.T) {
	src := Schema{
		Columns: []Column{
			{Name: "id", Type: ColumnType{Kind: KindInt64}},
			{Name: "amount", Type: ColumnType{Kind: KindInt32}},
			{Name: "label", Type: ColumnType{Kind: KindString}},
		},
	}
	policy := CastPolicy{
		Columns: map[string]CastTarget{
			"amount": {Type: ColumnType{Kind: KindInt64}},
		},
	}
	resolved, warns, err := policy.Resolve(src)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(warns) != 0 {
		t.Errorf("Resolve: unexpected warnings: %v", warns)
	}
	if len(resolved.Columns) != 3 {
		t.Fatalf("Resolve: got %d columns, want 3", len(resolved.Columns))
	}
	if resolved.Columns[1].Type.Kind != KindInt64 {
		t.Errorf("Resolve: amount kind = %v, want KindInt64", resolved.Columns[1].Type.Kind)
	}
}

func TestCastPolicyResolveUnknownWithCast(t *testing.T) {
	src := Schema{
		Columns: []Column{
			{Name: "geom", Type: ColumnType{Kind: KindUnknown}},
		},
	}
	policy := CastPolicy{
		Columns: map[string]CastTarget{
			"geom": {Type: ColumnType{Kind: KindString}},
		},
	}
	resolved, _, err := policy.Resolve(src)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if resolved.Columns[0].Type.Kind != KindString {
		t.Errorf("Resolve: geom kind = %v, want KindString", resolved.Columns[0].Type.Kind)
	}
}

func TestCastPolicyResolveUnknownWithoutCast(t *testing.T) {
	src := Schema{
		Columns: []Column{
			{Name: "geom", Type: ColumnType{Kind: KindUnknown}},
		},
	}
	policy := CastPolicy{}
	_, _, err := policy.Resolve(src)
	if err == nil {
		t.Error("Resolve should error on KindUnknown without explicit cast")
	}
}

func TestResolveSchemaMetadataCollision(t *testing.T) {
	src := Schema{
		Columns: []Column{
			{Name: "id", Type: ColumnType{Kind: KindInt64}},
			{Name: "op", Type: ColumnType{Kind: KindString}},
		},
	}
	meta := []MetadataColumn{
		{From: MetaOp, As: "op"},
	}
	_, _, err := ResolveSchema(src, CastPolicy{}, meta)
	if err == nil {
		t.Error("ResolveSchema should error when metadata as collides with source column")
	}
}

func TestResolveSchemaDuplicateMetadata(t *testing.T) {
	src := Schema{
		Columns: []Column{
			{Name: "id", Type: ColumnType{Kind: KindInt64}},
		},
	}
	meta := []MetadataColumn{
		{From: MetaOp, As: "op"},
		{From: MetaPhase, As: "op"},
	}
	_, _, err := ResolveSchema(src, CastPolicy{}, meta)
	if err == nil {
		t.Error("ResolveSchema should error on duplicate metadata as")
	}
}

// A cast changes the type, never the nullability: a nullable source column
// that is cast must stay nullable in the resolved schema, or a legitimate
// NULL would violate the typed Arrow schema downstream.
func TestResolveSchemaCastPreservesNullable(t *testing.T) {
	src := Schema{Columns: []Column{
		{Name: "id", Type: ColumnType{Kind: KindInt64, Nullable: false}},
		{Name: "note", Type: ColumnType{Kind: KindInt64, Nullable: true}},
	}}
	p, err := ParseCastPolicy(map[string]string{"note": "string"})
	if err != nil {
		t.Fatalf("cast policy: %v", err)
	}
	resolved, _, err := p.Resolve(src)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	for _, c := range resolved.Columns {
		switch c.Name {
		case "id":
			if c.Type.Kind != KindInt64 || c.Type.Nullable {
				t.Fatalf("id = %+v, want non-nullable int64", c.Type)
			}
		case "note":
			if c.Type.Kind != KindString || !c.Type.Nullable {
				t.Fatalf("note = %+v, want nullable string (cast changed kind, kept nullability)", c.Type)
			}
		}
	}
}

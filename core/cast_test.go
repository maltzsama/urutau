package core

import (
	"bytes"
	"math"
	"strings"
	"testing"
	"time"
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
		got, err := tt.target.Convert(tt.src.Kind, tt.input)
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
	got, err := CastTarget{Type: ColumnType{Kind: KindString}}.Convert(KindString, nil)
	if err != nil {
		t.Errorf("Convert(nil) unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("Convert(nil) = %v, want nil", got)
	}
}

func TestConvertTypeMismatch(t *testing.T) {
	_, err := CastTarget{Type: ColumnType{Kind: KindInt64}}.Convert(KindString, "not a number")
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

// ── Audit regressions: decimal integrity, key validation, uint64 ──────

// float32 → decimal: the matrix allowed it but the converter lacked a
// float32 case — every value failed at runtime after plan validation.
func TestConvertFloat32ToDecimal(t *testing.T) {
	got, err := CastTarget{Type: ColumnType{Kind: KindDecimal, Precision: 10, Scale: 2}}.Convert(KindFloat32, float32(3.14))
	if err != nil {
		t.Fatalf("Convert(float32 → decimal) unexpected error: %v", err)
	}
	if got != "3.14" {
		t.Errorf("Convert(float32 → decimal) = %v, want 3.14", got)
	}
}

// NaN and ±Inf have no decimal text; the converter must reject them.
func TestConvertNonFiniteToDecimal(t *testing.T) {
	target := CastTarget{Type: ColumnType{Kind: KindDecimal, Precision: 10, Scale: 2}}
	if _, err := target.Convert(KindFloat64, math.NaN()); err == nil {
		t.Error("Convert(NaN → decimal) should error")
	}
	if _, err := target.Convert(KindFloat64, math.Inf(1)); err == nil {
		t.Error("Convert(+Inf → decimal) should error")
	}
}

// int64 → decimal(4,2) overflows: 4-2 = 2 integer digits cannot hold a
// 3-digit value. The converter must reject it, not truncate.
func TestConvertDecimalPrecisionOverflow(t *testing.T) {
	// decimal(4,2) holds 2 integer digits. 12345 renders as "123.45" —
	// 3 integer digits overflow.
	target := CastTarget{Type: ColumnType{Kind: KindDecimal, Precision: 4, Scale: 2}}
	if _, err := target.Convert(KindInt64, int64(12345)); err == nil {
		t.Error("Convert(int64 12345 → decimal(4,2)) should error (3 integer digits > 2)")
	}
	// 123 renders as "1.23" (scale absorbs two digits) — fits.
	if _, err := target.Convert(KindInt64, int64(123)); err != nil {
		t.Errorf("Convert(int64 123 → decimal(4,2)) should pass: %v", err)
	}
	// 123456 → decimal(6,1) renders "12345.6" — 5 integer digits fits; the
	// 7th would not. Use a value that overflows: 1234567 → "123456.7".
	t61 := CastTarget{Type: ColumnType{Kind: KindDecimal, Precision: 6, Scale: 1}}
	if _, err := t61.Convert(KindInt64, int64(1234567)); err == nil {
		t.Error("Convert(int64 1234567 → decimal(6,1)) should error (6 integer digits > 5)")
	}
}

// string → decimal passthrough (KindUnknown bypass) must validate the text.
func TestConvertStringToDecimalValidates(t *testing.T) {
	target := CastTarget{Type: ColumnType{Kind: KindDecimal, Precision: 10, Scale: 2}}
	if _, err := target.Convert(KindString, "12.345"); err == nil {
		t.Error("Convert(\"12.345\" → decimal(10,2)) should error (3 fraction digits > scale 2)")
	}
	if _, err := target.Convert(KindString, "not-a-number"); err == nil {
		t.Error("Convert(\"not-a-number\" → decimal) should error")
	}
	if _, err := target.Convert(KindString, "12.34"); err != nil {
		t.Errorf("Convert(\"12.34\" → decimal(10,2)) should pass: %v", err)
	}
}

// decimal(p,s) with scale > precision is invalid and must fail at parse.
func TestParseCastTargetScaleExceedsPrecision(t *testing.T) {
	if _, err := ParseCastTarget("decimal(4,10)"); err == nil {
		t.Error("ParseCastTarget(\"decimal(4,10)\") should error (scale > precision)")
	}
}

// uint64 → string was missing from the "to string always" matrix, and
// ParseCastTarget rejected "uint64" — breaking the String() round-trip.
func TestUInt64CastSupport(t *testing.T) {
	if err := CheckCast(ColumnType{Kind: KindUInt64}, CastTarget{Type: ColumnType{Kind: KindString}}); err != nil {
		t.Errorf("CheckCast(uint64 → string) should be allowed (to string always): %v", err)
	}
	got, err := CastTarget{Type: ColumnType{Kind: KindString}}.Convert(KindUInt64, uint64(42))
	if err != nil || got != "42" {
		t.Errorf("Convert(uint64 → string) = %v, %v; want 42", got, err)
	}
	tgt, err := ParseCastTarget("uint64")
	if err != nil || tgt.Type.Kind != KindUInt64 {
		t.Errorf("ParseCastTarget(\"uint64\") = %+v, %v; want KindUInt64", tgt, err)
	}
}

// A cast key that names no source column is a typo that would silently
// no-op — the pipeline would carry the wrong type forever.
func TestResolveRejectsUnknownCastKey(t *testing.T) {
	src := Schema{Columns: []Column{{Name: "amount", Type: ColumnType{Kind: KindInt32}}}}
	policy := CastPolicy{Columns: map[string]CastTarget{
		"amout": {Type: ColumnType{Kind: KindInt64}}, // typo
	}}
	if _, _, err := policy.Resolve(src); err == nil {
		t.Error("Resolve with a cast key not in the schema should error")
	}
}

// A cast on a primary-key column changes the key's type — surface a warning.
func TestResolveWarnsOnPKCast(t *testing.T) {
	src := Schema{
		Columns:    []Column{{Name: "id", Type: ColumnType{Kind: KindInt64}}},
		PrimaryKey: []string{"id"},
	}
	policy := CastPolicy{Columns: map[string]CastTarget{
		"id": {Type: ColumnType{Kind: KindString}},
	}}
	_, warns, err := policy.Resolve(src)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	found := false
	for _, w := range warns {
		if strings.Contains(w.Message, "primary-key column") {
			found = true
		}
	}
	if !found {
		t.Errorf("Resolve should warn about a cast on a primary-key column, got %v", warns)
	}
}

// ── Columnar-representation completeness (the worker/sink round-trip) ──

// The sink re-applies Convert to values the worker already cast, so Convert
// must accept the canonical columnar representations too.
func TestConvertAcceptsColumnarRepresentations(t *testing.T) {
	ts := time.Date(2024, 1, 2, 15, 4, 5, 0, time.UTC)

	// timestamp <- time.Time (a batch that skipped the worker cast).
	got, err := CastTarget{Type: ColumnType{Kind: KindTimestamp}}.Convert(KindTimestampTZ, ts)
	if err != nil || got != "2024-01-02 15:04:05.000000000" {
		t.Fatalf("timestamp <- time.Time = %v, %v", got, err)
	}
	// timestamptz <- time.Time.
	if got, err := (CastTarget{Type: ColumnType{Kind: KindTimestampTZ}}).Convert(KindTimestampTZ, ts); err != nil || got != "2024-01-02T15:04:05Z" {
		t.Fatalf("timestamptz <- time.Time = %v, %v", got, err)
	}
	// uuid <- 16 raw bytes (idempotent re-cast).
	raw := bytes.Repeat([]byte{0xab}, 16)
	if got, err := (CastTarget{Type: ColumnType{Kind: KindUUID}}).Convert(KindUUID, raw); err != nil || !bytes.Equal(got.([]byte), raw) {
		t.Fatalf("uuid <- []byte = %v, %v", got, err)
	}
	if _, err := (CastTarget{Type: ColumnType{Kind: KindUUID}}).Convert(KindUUID, []byte{1, 2, 3}); err == nil {
		t.Error("uuid <- 3 bytes must error")
	}
	// json <- []byte and <- composite.
	if got, err := (CastTarget{Type: ColumnType{Kind: KindJSON}}).Convert(KindJSON, []byte(`{"a":1}`)); err != nil || got != `{"a":1}` {
		t.Fatalf("json <- []byte = %v, %v", got, err)
	}
	if got, err := (CastTarget{Type: ColumnType{Kind: KindJSON}}).Convert(KindStruct, map[string]any{"a": float64(1)}); err != nil || got != `{"a":1}` {
		t.Fatalf("json <- map = %v, %v", got, err)
	}
}

// StringifyScalar is the shared Go-type → string mapping.
func TestStringifyScalar(t *testing.T) {
	if s, err := StringifyScalar(time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)); err != nil || s != "2024-01-02T00:00:00Z" {
		t.Fatalf("StringifyScalar(time) = %q, %v", s, err)
	}
	if _, err := StringifyScalar([]byte{1}); err == nil {
		t.Error("StringifyScalar([]byte) must error — binary needs an encoding")
	}
}

// The single naive layout covers both with and without a fraction.
func TestParseTimestampText(t *testing.T) {
	for _, in := range []string{"2024-01-02", "2024-01-02 15:04:05", "2024-01-02 15:04:05.123", "2024-01-02T15:04:05Z"} {
		if _, err := ParseTimestampText(in); err != nil {
			t.Errorf("ParseTimestampText(%q): %v", in, err)
		}
	}
	if _, err := ParseTimestampText("not a time"); err == nil {
		t.Error("ParseTimestampText must reject free-form text")
	}
}

func TestParseTimeOfDayText(t *testing.T) {
	got, err := ParseTimeOfDayText("15:04:05.000001")
	if err != nil {
		t.Fatalf("ParseTimeOfDayText: %v", err)
	}
	want := int64(15*3_600_000_000 + 4*60_000_000 + 5*1_000_000 + 1)
	if got != want {
		t.Fatalf("micros = %d, want %d", got, want)
	}
}

// asInt64 must reject a uint64 that has no exact int64 form (FIX-DOC v2 B).
func TestAsInt64RejectsUint64Overflow(t *testing.T) {
	if _, err := asInt64(uint64(math.MaxInt64) + 1); err == nil {
		t.Fatal("uint64 above MaxInt64 must error")
	}
	if got, err := asInt64(uint64(42)); err != nil || got != 42 {
		t.Fatalf("asInt64(uint64(42)) = %d, %v; want 42", got, err)
	}
}

// C3: the temporal → string contract is canonical and stable, byte for byte.
// Naive timestamp carries no zone; timestamptz carries RFC3339Nano.
func TestTemporalToStringCanonical(t *testing.T) {
	ts := time.Date(2024, 1, 2, 15, 4, 5, 123000000, time.UTC)
	dateDays := int32(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix() / 86400)
	cases := []struct {
		name string
		from Kind
		v    any
		want string
	}{
		{"date", KindDate, dateDays, "2024-01-01"},
		{"time", KindTime, int64(15*3_600_000_000 + 4*60_000_000 + 5*1_000_000 + 1), "15:04:05.000001"},
		{"timestamp", KindTimestamp, ts, "2024-01-02 15:04:05.123000000"},
		{"timestamptz", KindTimestampTZ, ts, "2024-01-02T15:04:05.123Z"},
	}
	to := CastTarget{Type: ColumnType{Kind: KindString}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := to.Convert(tc.from, tc.v)
			if err != nil {
				t.Fatalf("Convert: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// C2: an integer under a timestamp source is a wire bug, not a guess.
func TestTemporalToStringRejectsInteger(t *testing.T) {
	to := CastTarget{Type: ColumnType{Kind: KindString}}
	for _, from := range []Kind{KindTimestamp, KindTimestampTZ} {
		_, err := to.Convert(from, int64(42))
		if err == nil || !strings.Contains(err.Error(), "ambiguous") {
			t.Fatalf("%s with an int must error as ambiguous, got %v", from, err)
		}
	}
}

// D1/D2: micros-since-midnight formatting, with the two out-of-range errors.
func TestFormatMicrosOfDay(t *testing.T) {
	cases := []struct {
		micros int64
		want   string
	}{
		{0, "00:00:00"},
		{int64(12 * time.Hour / time.Microsecond), "12:00:00"},
		{int64(15*3_600_000_000 + 4*60_000_000 + 5*1_000_000 + 123456), "15:04:05.123456"},
	}
	for _, tc := range cases {
		got, err := formatMicrosOfDay(tc.micros)
		if err != nil || got != tc.want {
			t.Fatalf("formatMicrosOfDay(%d) = %q, %v; want %q", tc.micros, got, err, tc.want)
		}
	}
	if _, err := formatMicrosOfDay(-1); err == nil {
		t.Error("negative micros must error")
	}
	if _, err := formatMicrosOfDay(int64(24 * time.Hour / time.Microsecond)); err == nil {
		t.Error("24h micros must error")
	}
}

// D3: castToTimestamp accepts a uint64 date (the overflow guard is B's).
func TestCastToTimestampAcceptsUint64(t *testing.T) {
	days := uint64(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix() / 86400)
	got, err := castToTimestamp(days)
	if err != nil {
		t.Fatalf("uint64 date → timestamp: %v", err)
	}
	if got != "2024-01-01 00:00:00.000000000" {
		t.Fatalf("got %v", got)
	}
}

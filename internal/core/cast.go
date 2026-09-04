// Package core defines the canonical type system. This file implements the
// per-column cast: a closed override of one column's canonical type, applied
// between the source mapping and the sink. It is annotation, never
// expression — there is no function grammar anywhere. The matrix is closed:
// widening always, to string always, narrowing and parsing never (except the
// explicitly declared temporal reinterpretations).
package core

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Warning is a validation outcome that does not reject the spec but must be
// surfaced to the operator (eventlog, status) — never only in logs.
type Warning struct {
	Message string
}

// ── Cast target parsing ───────────────────────────────────────────────

// CastTarget is the resolved destination of a per-column cast: a canonical
// type plus the explicit rules the matrix requires for the ambiguous cases.
type CastTarget struct {
	Type ColumnType
	// AssumeUTC marks timestamp → timestamptz(assume_utc): the naive
	// literal is asserted to be UTC, never shifted. Required for that cast.
	AssumeUTC bool
	// Encoding names the explicit encoding for binary → string: "hex" or
	// "base64". Plain binary → string is ambiguous and prohibited.
	Encoding string
}

// String renders the target in the textual spec form.
func (t CastTarget) String() string {
	s := t.Type.String()
	switch {
	case t.Type.Kind == KindTimestampTZ && t.AssumeUTC:
		return "timestamptz(assume_utc)"
	case t.Type.Kind == KindString && t.Encoding != "":
		return "string(" + t.Encoding + ")"
	default:
		return s
	}
}

// ParseCastTarget parses the textual cast value: a canonical kind name,
// decimal(p,s), timestamptz(assume_utc), or string(hex|base64).
func ParseCastTarget(s string) (CastTarget, error) {
	trimmed := strings.TrimSpace(strings.ToLower(s))
	if trimmed == "" {
		return CastTarget{}, fmt.Errorf("core: empty cast target")
	}

	// decimal(p,s)
	if base, args, ok := strings.Cut(trimmed, "("); ok && strings.HasSuffix(args, ")") && base == "decimal" {
		args = strings.TrimSuffix(args, ")")
		p, s, err := parseDecimalArgs(args)
		if err != nil {
			return CastTarget{}, err
		}
		return CastTarget{Type: ColumnType{Kind: KindDecimal, Precision: p, Scale: s}}, nil
	}

	// string(hex) / string(base64) / timestamptz(assume_utc)
	if base, args, ok := strings.Cut(trimmed, "("); ok && strings.HasSuffix(args, ")") {
		args = strings.TrimSuffix(args, ")")
		switch {
		case base == "string" && (args == "hex" || args == "base64"):
			return CastTarget{Type: ColumnType{Kind: KindString}, Encoding: args}, nil
		case base == "timestamptz" && args == "assume_utc":
			return CastTarget{Type: ColumnType{Kind: KindTimestampTZ}, AssumeUTC: true}, nil
		default:
			return CastTarget{}, fmt.Errorf("core: unknown cast target %q", s)
		}
	}

	switch trimmed {
	case "bool":
		return CastTarget{Type: ColumnType{Kind: KindBool}}, nil
	case "int32":
		return CastTarget{Type: ColumnType{Kind: KindInt32}}, nil
	case "int64":
		return CastTarget{Type: ColumnType{Kind: KindInt64}}, nil
	case "float32":
		return CastTarget{Type: ColumnType{Kind: KindFloat32}}, nil
	case "float64":
		return CastTarget{Type: ColumnType{Kind: KindFloat64}}, nil
	case "string":
		return CastTarget{Type: ColumnType{Kind: KindString}}, nil
	case "binary":
		return CastTarget{Type: ColumnType{Kind: KindBinary}}, nil
	case "date":
		return CastTarget{Type: ColumnType{Kind: KindDate}}, nil
	case "time":
		return CastTarget{Type: ColumnType{Kind: KindTime}}, nil
	case "timestamp":
		return CastTarget{Type: ColumnType{Kind: KindTimestamp}}, nil
	case "timestamptz":
		return CastTarget{Type: ColumnType{Kind: KindTimestampTZ}}, nil
	case "uuid":
		return CastTarget{Type: ColumnType{Kind: KindUUID}}, nil
	case "json":
		return CastTarget{Type: ColumnType{Kind: KindJSON}}, nil
	}
	return CastTarget{}, fmt.Errorf("core: unknown cast target %q", s)
}

func parseDecimalArgs(args string) (precision, scale int, err error) {
	p, s, ok := strings.Cut(args, ",")
	if !ok {
		return 0, 0, fmt.Errorf("core: decimal cast wants decimal(p,s), got %q", args)
	}
	precision, err = strconv.Atoi(strings.TrimSpace(p))
	if err != nil || precision <= 0 {
		return 0, 0, fmt.Errorf("core: decimal cast precision %q invalid", p)
	}
	scale, err = strconv.Atoi(strings.TrimSpace(s))
	if err != nil || scale < 0 {
		return 0, 0, fmt.Errorf("core: decimal cast scale %q invalid", s)
	}
	return precision, scale, nil
}

// ── Cast matrix ───────────────────────────────────────────────────────

// CheckCast validates that a cast from a source canonical column type to a
// target is in the closed matrix. It is the apply-time rule; nothing here
// runs at runtime beyond value conversion.
func CheckCast(from ColumnType, to CastTarget) error {
	// KindUnknown is the cast bypass: the source type has no canonical form
	// and the declared target becomes its type directly.
	if from.Kind == KindUnknown {
		return nil
	}
	switch to.Type.Kind {
	case KindString:
		switch from.Kind {
		case KindBool, KindInt32, KindInt64, KindFloat32, KindFloat64,
			KindDecimal, KindString, KindDate, KindTime, KindTimestamp,
			KindTimestampTZ, KindUUID, KindJSON:
			return nil
		case KindBinary, KindFixedBinary:
			if to.Encoding == "" {
				return fmt.Errorf("core: binary → string is ambiguous; declare string(hex) or string(base64)")
			}
			return nil
		default:
			return fmt.Errorf("core: %s → string is not allowed", from.Kind)
		}
	case KindInt64:
		if from.Kind == KindInt32 || from.Kind == KindInt64 {
			return nil
		}
		return fmt.Errorf("core: %s → int64 is not allowed (narrowing or incompatible)", from.Kind)
	case KindInt32:
		return fmt.Errorf("core: %s → int32 is not allowed (narrowing)", from.Kind)
	case KindFloat64:
		if from.Kind == KindFloat32 || from.Kind == KindFloat64 {
			return nil
		}
		return fmt.Errorf("core: %s → float64 is not allowed", from.Kind)
	case KindFloat32:
		return fmt.Errorf("core: %s → float32 is not allowed (narrowing)", from.Kind)
	case KindDecimal:
		if from.Kind == KindDecimal {
			// decimal → decimal only with p' >= p and s' = s; changing the
			// scale is rounding, i.e. silent loss.
			if to.Type.Scale != from.Scale {
				return fmt.Errorf("core: decimal → decimal scale must stay %d (changing scale rounds)", from.Scale)
			}
			if from.Precision > 0 && to.Type.Precision < from.Precision {
				return fmt.Errorf("core: decimal → decimal precision %d < source %d (narrowing)", to.Type.Precision, from.Precision)
			}
			return nil
		}
		if from.Kind == KindInt32 || from.Kind == KindInt64 || from.Kind == KindFloat32 || from.Kind == KindFloat64 {
			return nil
		}
		return fmt.Errorf("core: %s → decimal is not allowed", from.Kind)
	case KindUUID:
		if from.Kind == KindString || from.Kind == KindUUID {
			return nil
		}
		return fmt.Errorf("core: %s → uuid is not allowed", from.Kind)
	case KindJSON:
		if from.Kind == KindString || from.Kind == KindJSON {
			return nil
		}
		return fmt.Errorf("core: %s → json is not allowed", from.Kind)
	case KindDate:
		return fmt.Errorf("core: %s → date is not allowed (parsing)", from.Kind)
	case KindTime:
		return fmt.Errorf("core: %s → time is not allowed (parsing)", from.Kind)
	case KindTimestamp:
		if from.Kind == KindDate {
			return nil
		}
		if from.Kind == KindTimestampTZ {
			return nil // drops the zone; emit a warning at resolve time
		}
		return fmt.Errorf("core: %s → timestamp is not allowed", from.Kind)
	case KindTimestampTZ:
		if from.Kind == KindTimestamp {
			if !to.AssumeUTC {
				return fmt.Errorf("core: timestamp → timestamptz requires timestamptz(assume_utc): the naive literal is asserted, not converted")
			}
			return nil
		}
		return fmt.Errorf("core: %s → timestamptz is not allowed", from.Kind)
	case KindBinary:
		// Fixed binary widens to variable binary without loss.
		if from.Kind == KindBinary || from.Kind == KindFixedBinary {
			return nil
		}
		return fmt.Errorf("core: %s → binary is not allowed", from.Kind)
	default:
		return fmt.Errorf("core: %s → %s is not allowed", from.Kind, to.Type.Kind)
	}
}

// CastWarning returns the advisory warning for an allowed cast that loses
// information (timestamptz → timestamp), or "" for a clean cast.
func CastWarning(from Kind, to CastTarget) string {
	if from == KindTimestampTZ && to.Type.Kind == KindTimestamp {
		return "timestamptz → timestamp discards the timezone"
	}
	return ""
}

// ── Value conversion ──────────────────────────────────────────────────

// Convert applies an allowed cast to one value. It returns an error for a
// value the cast cannot represent (invalid UUID text, invalid JSON).
func (t CastTarget) Convert(v any) (any, error) {
	switch t.Type.Kind {
	case KindString:
		return castToString(v, t.Encoding)
	case KindBinary:
		return castToBinary(v)
	case KindInt64:
		return castToInt64(v)
	case KindFloat64:
		return castToFloat64(v)
	case KindDecimal:
		return castToDecimal(v, t.Type.Scale)
	case KindUUID:
		return castToUUID(v)
	case KindJSON:
		return castToJSON(v)
	case KindTimestamp:
		return castToTimestamp(v)
	case KindTimestampTZ:
		return castToTimestampTZ(v, t.AssumeUTC)
	default:
		return nil, fmt.Errorf("core: cast to %s not implemented", t.Type)
	}
}

func castToString(v any, enc string) (any, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case string:
		return t, nil
	case bool:
		return strconv.FormatBool(t), nil
	case int:
		return strconv.FormatInt(int64(t), 10), nil
	case int32:
		return strconv.FormatInt(int64(t), 10), nil
	case int64:
		return strconv.FormatInt(t, 10), nil
	case float32:
		return strconv.FormatFloat(float64(t), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64), nil
	case []byte:
		switch enc {
		case "hex":
			return hex.EncodeToString(t), nil
		case "base64":
			return base64.StdEncoding.EncodeToString(t), nil
		default:
			return nil, fmt.Errorf("core: binary → string requires string(hex) or string(base64)")
		}
	default:
		return nil, fmt.Errorf("core: cannot cast %T to string", v)
	}
}

func castToBinary(v any) (any, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case []byte:
		return t, nil
	case string:
		return []byte(t), nil
	default:
		return nil, fmt.Errorf("core: cannot cast %T to binary", v)
	}
}

func castToInt64(v any) (any, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case int:
		return int64(t), nil
	case int32:
		return int64(t), nil
	case int64:
		return t, nil
	default:
		return nil, fmt.Errorf("core: cannot cast %T to int64", v)
	}
}

func castToFloat64(v any) (any, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case float32:
		return float64(t), nil
	case float64:
		return t, nil
	default:
		return nil, fmt.Errorf("core: cannot cast %T to float64", v)
	}
}

// castToDecimal renders an integral or float value as decimal text with the
// target scale.
func castToDecimal(v any, scale int) (any, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case int:
		return decimalText(big.NewInt(int64(t)), scale), nil
	case int32:
		return decimalText(big.NewInt(int64(t)), scale), nil
	case int64:
		return decimalText(big.NewInt(t), scale), nil
	case float64:
		return decimalFloatText(t, scale), nil
	case string:
		return t, nil
	default:
		return nil, fmt.Errorf("core: cannot cast %T to decimal", v)
	}
}

func decimalText(n *big.Int, scale int) string {
	neg := n.Sign() < 0
	if neg {
		n = new(big.Int).Neg(n)
	}
	s := n.String()
	if scale == 0 {
		if neg {
			return "-" + s
		}
		return s
	}
	for len(s) <= scale {
		s = "0" + s
	}
	intPart, frac := s[:len(s)-scale], s[len(s)-scale:]
	if neg {
		return "-" + intPart + "." + frac
	}
	return intPart + "." + frac
}

func decimalFloatText(f float64, scale int) string {
	if scale == 0 {
		return strconv.FormatFloat(f, 'f', 0, 64)
	}
	return strconv.FormatFloat(f, 'f', scale, 64)
}

var uuidRe = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func castToUUID(v any) (any, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case string:
		if !uuidRe.MatchString(t) {
			return nil, fmt.Errorf("core: %q is not a valid uuid", t)
		}
		return strings.ToLower(t), nil
	default:
		return nil, fmt.Errorf("core: cannot cast %T to uuid", v)
	}
}

func castToJSON(v any) (any, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case string:
		if !json.Valid([]byte(t)) {
			return nil, fmt.Errorf("core: %q is not valid json", t)
		}
		return t, nil
	default:
		return nil, fmt.Errorf("core: cannot cast %T to json", v)
	}
}

// naiveTimestampLayouts are the source-native renderings a naive timestamp
// may arrive in (MySQL DATETIME without fraction, Postgres with micros).
var naiveTimestampLayouts = []string{
	"2006-01-02 15:04:05.999999999",
	"2006-01-02 15:04:05.999999",
	"2006-01-02 15:04:05",
}

// castToTimestamp reinterprets a naive temporal text: date becomes midnight,
// timestamptz drops its zone. Never parses a free-form string.
func castToTimestamp(v any) (any, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case string:
		if tm, err := time.Parse("2006-01-02", t); err == nil {
			return tm.Format("2006-01-02 15:04:05.000000000"), nil
		}
		for _, layout := range naiveTimestampLayouts {
			if tm, err := time.Parse(layout, t); err == nil {
				return tm.Format("2006-01-02 15:04:05.000000000"), nil
			}
		}
		if tm, err := time.Parse(time.RFC3339Nano, t); err == nil {
			return tm.Format("2006-01-02 15:04:05.000000000"), nil
		}
		return nil, fmt.Errorf("core: %q is not a naive timestamp or date", t)
	default:
		return nil, fmt.Errorf("core: cannot cast %T to timestamp", v)
	}
}

func castToTimestampTZ(v any, assumeUTC bool) (any, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case string:
		if tm, err := time.Parse("2006-01-02", t); err == nil && assumeUTC {
			return tm.UTC().Format(time.RFC3339Nano), nil
		}
		for _, layout := range naiveTimestampLayouts {
			if tm, err := time.Parse(layout, t); err == nil {
				if !assumeUTC {
					return nil, fmt.Errorf("core: naive timestamp → timestamptz requires timestamptz(assume_utc)")
				}
				return tm.UTC().Format(time.RFC3339Nano), nil
			}
		}
		if tm, err := time.Parse(time.RFC3339Nano, t); err == nil {
			return tm.Format(time.RFC3339Nano), nil
		}
		return nil, fmt.Errorf("core: %q is not a naive timestamp", t)
	default:
		return nil, fmt.Errorf("core: cannot cast %T to timestamptz", v)
	}
}

// ── Cast policy ───────────────────────────────────────────────────────

// CastPolicy is the per-table map of source column name → target canonical
// type. Keys are source-side names, matching the introspection result.
type CastPolicy struct {
	Columns map[string]CastTarget
}

// ParseCastPolicy builds a policy from the spec's textual map.
func ParseCastPolicy(m map[string]string) (CastPolicy, error) {
	cols := make(map[string]CastTarget, len(m))
	for name, text := range m {
		t, err := ParseCastTarget(text)
		if err != nil {
			return CastPolicy{}, fmt.Errorf("core: cast column %q: %w", name, err)
		}
		cols[name] = t
	}
	return CastPolicy{Columns: cols}, nil
}

// Target returns the declared cast for a source column, if any.
func (p CastPolicy) Target(name string) (CastTarget, bool) {
	if p.Columns == nil {
		return CastTarget{}, false
	}
	t, ok := p.Columns[name]
	return t, ok
}

// Resolve applies the policy to one source schema, producing the final
// canonical schema: cast columns in place, KindUnknown columns replaced by
// their cast or rejected, and the source primary key preserved.
func (p CastPolicy) Resolve(src Schema) (Schema, []Warning, error) {
	out := Schema{PrimaryKey: append([]string(nil), src.PrimaryKey...)}
	var warns []Warning
	for _, col := range src.Columns {
		target, hasCast := p.Target(col.Name)
		if col.Type.Kind == KindUnknown && !hasCast {
			// The escape valve names what it is carrying when the source
			// knew: "column location is unmappable (mysql point); declare a
			// cast" beats a blind error that forces a guess.
			prov := col.Type.Opaque
			if prov != nil {
				return Schema{}, nil, fmt.Errorf("core: column %q is unmappable (%s); declare a cast, e.g. cast: {%s: string}", col.Name, prov, col.Name)
			}
			return Schema{}, nil, fmt.Errorf("core: column %q has an unmappable source type; declare a cast", col.Name)
		}
		if !hasCast {
			out.Columns = append(out.Columns, col)
			continue
		}
		if err := CheckCast(col.Type, target); err != nil {
			return Schema{}, nil, fmt.Errorf("core: column %q (%s): %w", col.Name, col.Type.Kind, err)
		}
		if w := CastWarning(col.Type.Kind, target); w != "" {
			warns = append(warns, Warning{Message: fmt.Sprintf("core: column %q: %s", col.Name, w)})
		}
		// The cast changes the type, never the nullability: a nullable
		// source column must stay nullable in the sink schema, or a
		// legitimate NULL would violate the typed Arrow schema downstream.
		tt := target.Type
		tt.Nullable = col.Type.Nullable
		out.Columns = append(out.Columns, Column{Name: col.Name, Type: tt})
	}
	return out, warns, nil
}

// ResolveSchema applies the cast policy and appends the declared metadata
// columns (all nullable, in declaration order) to a source schema, producing
// the final canonical schema the sink is built from. A metadata destination
// must never collide with a source column: a silent collision would
// overwrite user data.
func ResolveSchema(src Schema, cast CastPolicy, meta []MetadataColumn) (Schema, []Warning, error) {
	resolved, warns, err := cast.Resolve(src)
	if err != nil {
		return Schema{}, nil, err
	}
	srcNames := make(map[string]bool, len(resolved.Columns))
	for _, c := range resolved.Columns {
		srcNames[c.Name] = true
	}
	seen := map[string]bool{}
	for _, m := range meta {
		if srcNames[m.As] {
			return Schema{}, nil, fmt.Errorf("core: metadata column %q collides with a source column", m.As)
		}
		if seen[m.As] {
			return Schema{}, nil, fmt.Errorf("core: metadata column %q declared twice", m.As)
		}
		seen[m.As] = true
		resolved.Columns = append(resolved.Columns, Column{
			Name: m.As,
			Type: ColumnType{Kind: m.From.ColumnType().Kind, Nullable: true},
		})
	}
	return resolved, warns, nil
}

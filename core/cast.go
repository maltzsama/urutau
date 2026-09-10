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
	"math"
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
	case "uint64":
		return CastTarget{Type: ColumnType{Kind: KindUInt64}}, nil
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
	if scale > precision {
		return 0, 0, fmt.Errorf("core: decimal cast scale %d exceeds precision %d", scale, precision)
	}
	return precision, scale, nil
}

// ── Cast matrix ───────────────────────────────────────────────────────

// CheckCast validates that a cast from a source canonical column type to a
// target is in the closed matrix. It is the apply-time rule; nothing here
// runs at runtime beyond value conversion.
func CheckCast(from ColumnType, to CastTarget) error {
	// KindUnknown is the cast bypass: the source type has no canonical form
	// and the declared target becomes its type directly. An encoded string
	// (string(hex)/string(base64)) is rejected: an unmappable column carries
	// no encoding on the wire, so the sink could never apply it — a plain
	// string (the JSON dump) is the supported escape valve.
	if from.Kind == KindUnknown {
		if to.Type.Kind == KindString && to.Encoding != "" {
			prov := ""
			if from.Opaque != nil {
				prov = " (" + from.Opaque.String() + ")"
			}
			return fmt.Errorf("core: unmappable column%s → string(%s) is not supported; declare a plain string cast", prov, to.Encoding)
		}
		return nil
	}
	switch to.Type.Kind {
	case KindString:
		switch from.Kind {
		case KindBool, KindInt32, KindInt64, KindUInt64, KindFloat32, KindFloat64,
			KindDecimal, KindString, KindDate, KindTime, KindTimestamp,
			KindTimestampTZ, KindUUID, KindJSON:
			return nil
		case KindBinary, KindFixedBinary:
			if to.Encoding == "" {
				return fmt.Errorf("core: binary → string is ambiguous; declare string(hex) or string(base64)")
			}
			return nil
		case KindStruct, KindList, KindMap:
			// A composite → string cast is the escape hatch that dumps the
			// nested value as JSON when the developer does not want the
			// native nested type. The hex/base64 encodings are meaningless
			// here; only plain string applies.
			if to.Encoding != "" {
				return fmt.Errorf("core: %s → string(%s) is not allowed; composite dumps as JSON via plain string", from.Kind, to.Encoding)
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

// Convert applies an allowed cast to one value. from is the source column's
// canonical Kind: the value kernel needs it to disambiguate representations
// that share a Go type ([]byte is binary or uuid; int32 is an integer or a
// date; int64 is an integer or a time-of-day). from is consultive — it only
// shapes the rendering; the policy switch is t.Type. It returns an error for
// a value the cast cannot represent (invalid UUID text, invalid JSON).
//
// Convert is NOT a leftover from the pre-columnar design: it is the
// per-value cast kernel the sinks apply on their write paths (clickhouse,
// couchbase and iceberg coerce each column value through it before
// serialization), where the unit of work is one value, not a column. The
// dataplane's columnar Cast (transport.KindToArrow + compute) is the
// batch-level executor; the two coexist by design (W-1 / D-4: the matrix is
// the single policy, the arrow kernel and this value kernel are the two
// executors). Deleting Convert would strand the sinks.
func (t CastTarget) Convert(from Kind, v any) (any, error) {
	switch t.Type.Kind {
	case KindString:
		return castToString(from, v, t.Encoding)
	case KindBinary:
		return castToBinary(v)
	case KindInt64:
		return castToInt64(v)
	case KindFloat64:
		return castToFloat64(v)
	case KindDecimal:
		return castToDecimal(v, t.Type.Precision, t.Type.Scale)
	case KindUUID:
		return castToUUID(v)
	case KindJSON:
		return castToJSON(v)
	case KindTimestamp:
		return castToTimestamp(from, v)
	case KindTimestampTZ:
		return castToTimestampTZ(v, t.AssumeUTC)
	default:
		return nil, fmt.Errorf("core: cast to %s not implemented", t.Type)
	}
}

// StringifyScalar renders a non-binary scalar to its canonical string form.
// It is shared by the value cast kernel and the columnar encoder so both
// agree on the Go-type → string mapping for the "to string always" matrix.
// Binary ([]byte) is deliberately absent: it needs an explicit encoding and
// is handled by castToString and the encoder separately.
func StringifyScalar(v any) (string, error) {
	switch t := v.(type) {
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
	case uint64:
		return strconv.FormatUint(t, 10), nil
	case float32:
		return strconv.FormatFloat(float64(t), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64), nil
	case time.Time:
		return t.Format(time.RFC3339Nano), nil
	case map[string]any, []any:
		// Composite value dumped as JSON — the struct/list/map → string cast.
		b, err := json.Marshal(t)
		if err != nil {
			return "", fmt.Errorf("core: cannot JSON-encode %T to string", v)
		}
		return string(b), nil
	default:
		return "", fmt.Errorf("core: cannot cast %T to string", v)
	}
}

// castToString renders a source value as string. from selects the rendering
// for the Kinds whose Go type is ambiguous on its own: a date (int32 days),
// a time (int64 micros), a uuid ([]byte) and binary ([]byte, which needs the
// declared encoding). Everything else falls through to StringifyScalar.
func castToString(from Kind, v any, enc string) (any, error) {
	if v == nil {
		return nil, nil
	}
	switch from {
	case KindBinary, KindFixedBinary:
		b, err := asBytes(v)
		if err != nil {
			return nil, fmt.Errorf("core: binary → string: %w", err)
		}
		switch enc {
		case "hex":
			return hex.EncodeToString(b), nil
		case "base64":
			return base64.StdEncoding.EncodeToString(b), nil
		default:
			return nil, fmt.Errorf("core: binary → string requires string(hex) or string(base64)")
		}
	case KindUUID:
		switch t := v.(type) {
		case string:
			return strings.ToLower(t), nil
		case []byte:
			return formatUUID(t)
		default:
			return nil, fmt.Errorf("core: cannot cast %T to uuid text", v)
		}
	case KindDate:
		// The wire form of a date is int32 days since epoch.
		days, err := asInt64(v)
		if err != nil {
			return nil, fmt.Errorf("core: date → string: %w", err)
		}
		return time.Unix(days*86400, 0).UTC().Format(dateLayout), nil
	case KindTime:
		// The wire form of a time is int64 micros since midnight.
		micros, err := asInt64(v)
		if err != nil {
			return nil, fmt.Errorf("core: time → string: %w", err)
		}
		return formatMicrosOfDay(micros)
	case KindTimestamp:
		// Naive: the TZ does not exist in the data, so the canonical text
		// carries no zone. An integer here is ambiguous — the wire form of a
		// timestamp is time.Time, not a count of days or micros (that
		// distinction belongs to Date/Time, whose from says so) — so it is a
		// wire bug, not something to guess.
		if tm, ok := v.(time.Time); ok {
			return tm.Format(naiveTimestampTextLayout), nil
		}
		if s, ok := v.(string); ok {
			return s, nil
		}
		return nil, fmt.Errorf("core: cannot render %T as %s text — wire representation ambiguous; fix the wire Kind", v, from)
	case KindTimestampTZ:
		// The zone IS the semantics, so the canonical text is RFC3339Nano.
		if tm, ok := v.(time.Time); ok {
			return tm.Format(time.RFC3339Nano), nil
		}
		if s, ok := v.(string); ok {
			return s, nil
		}
		return nil, fmt.Errorf("core: cannot render %T as %s text — wire representation ambiguous; fix the wire Kind", v, from)
	default:
		return StringifyScalar(v)
	}
}

// asBytes interprets a value as raw bytes (a source may hand binary over as
// []byte or as an already-normalized string).
func asBytes(v any) ([]byte, error) {
	switch t := v.(type) {
	case []byte:
		return t, nil
	case string:
		return []byte(t), nil
	default:
		return nil, fmt.Errorf("cannot interpret %T as bytes", v)
	}
}

// asInt64 interprets a value as an integer (the columnar representation of a
// date is int32 days, of a time int64 micros). A uint64 above MaxInt64 has no
// exact int64 form and is rejected, never silently wrapped.
func asInt64(v any) (int64, error) {
	switch t := v.(type) {
	case int:
		return int64(t), nil
	case int32:
		return int64(t), nil
	case int64:
		return t, nil
	case uint64:
		if t > math.MaxInt64 {
			return 0, fmt.Errorf("core: uint64 %d overflows int64", t)
		}
		return int64(t), nil
	default:
		return 0, fmt.Errorf("cannot interpret %T as an integer", v)
	}
}

// formatUUID renders 16 raw bytes as the canonical 8-4-4-4-12 uuid text.
func formatUUID(b []byte) (string, error) {
	if len(b) != 16 {
		return "", fmt.Errorf("core: uuid bytes must be 16 long, got %d", len(b))
	}
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

// formatMicrosOfDay renders micros since midnight as HH:MM:SS[.ffffff]. A
// negative value or one at/after 24h has no time-of-day meaning and is an
// error — never clamped or truncated.
func formatMicrosOfDay(micros int64) (string, error) {
	if micros < 0 {
		return "", fmt.Errorf("core: time-of-day %d micros is negative", micros)
	}
	if micros >= int64(24*time.Hour/time.Microsecond) {
		return "", fmt.Errorf("core: time-of-day %d micros is at or past 24h", micros)
	}
	ns := micros * 1000
	h := ns / int64(time.Hour)
	ns -= h * int64(time.Hour)
	m := ns / int64(time.Minute)
	ns -= m * int64(time.Minute)
	s := ns / int64(time.Second)
	ns -= s * int64(time.Second)
	if ns == 0 {
		return fmt.Sprintf("%02d:%02d:%02d", h, m, s), nil
	}
	return fmt.Sprintf("%02d:%02d:%02d.%06d", h, m, s, ns/1000), nil
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
// target precision and scale. Non-finite floats are rejected (NaN/±Inf have
// no decimal text). Every rendered value is checked against the declared
// precision — an overflow is an error, not silent truncation. A string
// passthrough (only reachable via the KindUnknown bypass) is validated the
// same way.
func castToDecimal(v any, precision, scale int) (any, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case int:
		return decimalChecked(big.NewInt(int64(t)), precision, scale)
	case int32:
		return decimalChecked(big.NewInt(int64(t)), precision, scale)
	case int64:
		return decimalChecked(big.NewInt(t), precision, scale)
	case float32:
		if err := checkFinite(float64(t)); err != nil {
			return nil, err
		}
		return floatTextChecked(float64(t), precision, scale)
	case float64:
		if err := checkFinite(t); err != nil {
			return nil, err
		}
		return floatTextChecked(t, precision, scale)
	case string:
		if err := checkDecimalText(t, precision, scale); err != nil {
			return nil, err
		}
		return t, nil
	default:
		return nil, fmt.Errorf("core: cannot cast %T to decimal", v)
	}
}

// checkFinite rejects NaN and ±Inf, which have no decimal text.
func checkFinite(f float64) error {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return fmt.Errorf("core: cannot cast non-finite float %v to decimal", f)
	}
	return nil
}

// decimalChecked renders an integral value and validates it against the
// declared precision/scale.
func decimalChecked(n *big.Int, precision, scale int) (string, error) {
	s := decimalText(n, scale)
	if err := checkDecimalText(s, precision, scale); err != nil {
		return "", err
	}
	return s, nil
}

// floatTextChecked renders a float value and validates it against the
// declared precision/scale.
func floatTextChecked(f float64, precision, scale int) (string, error) {
	s := decimalFloatText(f, scale)
	if err := checkDecimalText(s, precision, scale); err != nil {
		return "", err
	}
	return s, nil
}

// decimalTextRe matches a decimal literal: optional sign, integer digits,
// optional fraction.
var decimalTextRe = regexp.MustCompile(`^[+-]?\d+(?:\.\d+)?$`)

// checkDecimalText validates a decimal text against precision and scale: at
// most (precision - scale) integer digits and at most scale fraction digits.
// Non-numeric text and overflow are errors.
func checkDecimalText(s string, precision, scale int) error {
	if !decimalTextRe.MatchString(s) {
		return fmt.Errorf("core: %q is not a valid decimal text", s)
	}
	body := s
	if body[0] == '+' || body[0] == '-' {
		body = body[1:]
	}
	intPart := body
	fracPart := ""
	if i := strings.IndexByte(body, '.'); i >= 0 {
		intPart, fracPart = body[:i], body[i+1:]
	}
	if len(fracPart) > scale {
		return fmt.Errorf("core: %q has %d fraction digits, decimal scale is %d", s, len(fracPart), scale)
	}
	if len(intPart) > precision-scale {
		return fmt.Errorf("core: %q exceeds decimal precision %d (scale %d)", s, precision, scale)
	}
	return nil
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

// decimalFloatText renders a float at the target scale. FormatFloat with an
// explicit scale rounds to that many fraction digits — the target's declared
// scale is authoritative, so this is the intended quantization, not silent
// truncation. The caller validates the rounded text against the precision
// afterwards, so an overflow is still an error.
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
	case []byte:
		// The columnar representation of a UUID is 16 raw bytes; a value
		// already in that form is valid as-is (idempotent re-cast).
		if len(t) != 16 {
			return nil, fmt.Errorf("core: uuid bytes must be 16 long, got %d", len(t))
		}
		return t, nil
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
	case []byte:
		if !json.Valid(t) {
			return nil, fmt.Errorf("core: %q is not valid json", t)
		}
		return string(t), nil
	case map[string]any, []any:
		// A source that already decoded the JSON (message-log decoders)
		// hands over a Go composite; re-encode it canonically.
		b, err := json.Marshal(t)
		if err != nil {
			return nil, fmt.Errorf("core: cannot JSON-encode %T", v)
		}
		return string(b), nil
	default:
		return nil, fmt.Errorf("core: cannot cast %T to json", v)
	}
}

// dateLayout, naiveTimestampLayout and timeOfDayLayout are the source-native
// temporal renderings. The naive layout's trailing .999999999 makes the
// fraction optional and strips trailing zeros, so it alone covers a naive
// timestamp with or without a fraction — there is no separate layout for
// "no fraction".
//
// naiveTimestampTextLayout is the canonical OUTPUT for a naive timestamp:
// fixed nine-digit fraction, no zone (the zone does not exist in the data).
const (
	dateLayout               = "2006-01-02"
	naiveTimestampLayout     = "2006-01-02 15:04:05.999999999"
	naiveTimestampTextLayout = "2006-01-02 15:04:05.000000000"
	timeOfDayLayout          = "15:04:05.999999999"
)

// ParseTimestampText parses a temporal text into a time.Time: an RFC3339
// instant (zone preserved), a bare date (midnight), or a naive timestamp
// with an optional fraction.
func ParseTimestampText(s string) (time.Time, error) {
	if tm, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return tm, nil
	}
	if tm, err := time.Parse(dateLayout, s); err == nil {
		return tm, nil
	}
	if tm, err := time.Parse(naiveTimestampLayout, s); err == nil {
		return tm, nil
	}
	return time.Time{}, fmt.Errorf("core: %q is not a temporal value", s)
}

// ParseTimeOfDayText parses "HH:MM:SS[.fraction]" into micros since midnight.
func ParseTimeOfDayText(s string) (int64, error) {
	tm, err := time.Parse(timeOfDayLayout, s)
	if err != nil {
		return 0, fmt.Errorf("core: %q is not a time-of-day value", s)
	}
	return int64(tm.Hour())*3_600_000_000 + int64(tm.Minute())*60_000_000 +
		int64(tm.Second())*1_000_000 + int64(tm.Nanosecond())/1_000, nil
}

// castToTimestamp reinterprets a naive temporal value: date becomes midnight,
// timestamptz drops its zone. Never parses a free-form string.
func castToTimestamp(from Kind, v any) (any, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case time.Time:
		// Already a timestamp (the columnar representation); re-render it
		// naive so the sink sees one consistent textual form.
		return t.Format(naiveTimestampTextLayout), nil
	case int, int32, int64, uint64:
		// Only a date (KindDate) carries days-since-epoch as an integer.
		// Any other source — including an unknown Kind, which cannot tell
		// days from micros — handing an integer here is a wire bug, not a
		// guess (from exists to say which).
		if from != KindDate {
			return nil, fmt.Errorf("core: cannot render %T as %s text — wire representation ambiguous; fix the wire Kind", v, from)
		}
		days, err := asInt64(t)
		if err != nil {
			return nil, fmt.Errorf("core: %s → timestamp: %w", from, err)
		}
		return time.Unix(days*86400, 0).UTC().Format(naiveTimestampTextLayout), nil
	case string:
		tm, err := ParseTimestampText(t)
		if err != nil {
			return nil, fmt.Errorf("core: %q is not a naive timestamp or date", t)
		}
		return tm.Format(naiveTimestampTextLayout), nil
	default:
		return nil, fmt.Errorf("core: cannot cast %T to timestamp", v)
	}
}

func castToTimestampTZ(v any, assumeUTC bool) (any, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case time.Time:
		// Already an instant (the columnar representation); keep it.
		return t.Format(time.RFC3339Nano), nil
	case string:
		// A value that already carries a zone (RFC3339) is an instant and
		// needs no assume_utc.
		if tm, err := time.Parse(time.RFC3339Nano, t); err == nil {
			return tm.Format(time.RFC3339Nano), nil
		}
		// Otherwise it is naive (date or naive timestamp) and requires
		// assume_utc: the naive literal is asserted, not shifted.
		tm, err := ParseTimestampText(t)
		if err != nil {
			return nil, fmt.Errorf("core: %q is not a naive timestamp", t)
		}
		if !assumeUTC {
			return nil, fmt.Errorf("core: naive timestamp → timestamptz requires timestamptz(assume_utc)")
		}
		return tm.UTC().Format(time.RFC3339Nano), nil
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
	if err := src.Validate(); err != nil {
		return Schema{}, nil, err
	}
	out := Schema{PrimaryKey: append([]string(nil), src.PrimaryKey...)}
	var warns []Warning
	srcHas := make(map[string]bool, len(src.Columns))
	for _, col := range src.Columns {
		srcHas[col.Name] = true
	}
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
		if inPrimaryKey(col.Name, src.PrimaryKey) {
			warns = append(warns, Warning{Message: fmt.Sprintf("core: cast on primary-key column %q changes the key type", col.Name)})
		}
		// The cast changes the type, never the nullability: a nullable
		// source column must stay nullable in the sink schema, or a
		// legitimate NULL would violate the typed Arrow schema downstream.
		tt := target.Type
		tt.Nullable = col.Type.Nullable
		out.Columns = append(out.Columns, Column{Name: col.Name, Type: tt})
	}
	// Reject cast keys that name no source column: a typo would silently
	// no-op and the pipeline would carry the wrong type forever.
	for name := range p.Columns {
		if !srcHas[name] {
			return Schema{}, nil, fmt.Errorf("core: cast column %q is not in the source schema", name)
		}
	}
	return out, warns, nil
}

// WireSchema returns the shape a source encodes and the wire carries.
//
// Invariant: the wire preserves the SOURCE type — including its nullability
// — so a sink's Kind-aware cast sees the true origin (a date as Date32,
// binary as Binary, not the already-cast target). The resolved schema
// dictates only the KIND, and only for a KindUnknown column, which has no
// Arrow representation of its own; the sink then re-applies that cast
// idempotently.
func WireSchema(source, resolved Schema) Schema {
	out := Schema{PrimaryKey: append([]string(nil), source.PrimaryKey...), Columns: make([]Column, 0, len(source.Columns))}
	for _, c := range source.Columns {
		if c.Type.Kind == KindUnknown {
			if rc, ok := resolved.Column(c.Name); ok {
				// Take the resolved Kind, keep the source nullability: the
				// wire's nullability is always the source's.
				nullable := c.Type.Nullable
				c.Type = rc.Type
				c.Type.Nullable = nullable
			}
		}
		out.Columns = append(out.Columns, c)
	}
	return out
}

// inPrimaryKey reports whether name is a member of the key list.
func inPrimaryKey(name string, pk []string) bool {
	for _, k := range pk {
		if k == name {
			return true
		}
	}
	return false
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
		if m.As == "" {
			return Schema{}, nil, fmt.Errorf("core: metadata column: destination name is empty")
		}
		if !validMetadataKey(m.From) {
			return Schema{}, nil, fmt.Errorf("core: metadata column %q: unknown metadata key %q (the catalog is closed)", m.As, m.From)
		}
		if srcNames[m.As] {
			return Schema{}, nil, fmt.Errorf("core: metadata column %q collides with a source column", m.As)
		}
		if seen[m.As] {
			return Schema{}, nil, fmt.Errorf("core: metadata column %q declared twice", m.As)
		}
		seen[m.As] = true
		resolved.Columns = append(resolved.Columns, Column{
			Name: m.As,
			Type: m.From.ColumnType(), // already nullable (see MetadataKey.ColumnType)
		})
	}
	return resolved, warns, nil
}

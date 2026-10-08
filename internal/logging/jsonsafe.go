package logging

import (
	"encoding/json"
	"fmt"
)

// maxJSONDepth bounds jsonSafeValueAny's recursion. A cyclic map logged as an
// attr is replaced by maxDepthMarker instead of recursing forever.
const maxJSONDepth = 8

const maxDepthMarker = "<max-depth>"

// JSONSafeAttrs recursively replaces values json.Marshal cannot encode (maps
// with non-string keys, channels, funcs, errors) with their text form, so the
// dashboard can marshal a record's attrs without a second, drifting
// sanitizer (issue #511).
func JSONSafeAttrs(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = jsonSafeValueAny(v, 1)
	}
	return out
}

func jsonSafeValueAny(v any, depth int) any {
	switch t := v.(type) {
	case nil, bool, string,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64, json.Number:
		return v
	case map[string]any:
		if depth >= maxJSONDepth {
			return maxDepthMarker
		}
		out := make(map[string]any, len(t))
		for k, vv := range t {
			out[k] = jsonSafeValueAny(vv, depth+1)
		}
		return out
	case []any:
		if depth >= maxJSONDepth {
			return maxDepthMarker
		}
		out := make([]any, len(t))
		for i, vv := range t {
			out[i] = jsonSafeValueAny(vv, depth+1)
		}
		return out
	default:
		// Channels, funcs, errors and flat maps — no map[string]any/[]any
		// recursion, so fmt.Sprint cannot cycle here.
		return fmt.Sprint(v)
	}
}

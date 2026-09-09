package transport

// T-1 (schema level): table-schema round-trip. EncodeTableSchema ->
// DecodeTableSchema must preserve every kind — including the distinction
// the row codec cannot make: KindTimestamp (naive, no TZ) vs
// KindTimestampTZ (UTC instant). Both decode to time.Time in rows, so
// only the SCHEMA level can catch a TZ-mapping regression.
// (The audit doc's T-13 is a different item: insert→delete→insert
// ordering under W-3.)

import (
	"testing"

	"github.com/maltzsama/urutau/core"
)

func TestTableSchemaRoundTripPreservesKinds(t *testing.T) {
	listElem := core.ColumnType{Kind: core.KindString, Nullable: true}
	mapKey := core.ColumnType{Kind: core.KindString}
	mapVal := core.ColumnType{Kind: core.KindInt64, Nullable: true}

	in := core.Schema{
		Columns: []core.Column{
			{Name: "c_bool", Type: core.ColumnType{Kind: core.KindBool}},
			{Name: "c_int32", Type: core.ColumnType{Kind: core.KindInt32}},
			{Name: "c_int64", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "c_uint64", Type: core.ColumnType{Kind: core.KindUInt64}},
			{Name: "c_f32", Type: core.ColumnType{Kind: core.KindFloat32}},
			{Name: "c_f64", Type: core.ColumnType{Kind: core.KindFloat64}},
			{Name: "c_dec", Type: core.ColumnType{Kind: core.KindDecimal, Precision: 12, Scale: 3}},
			{Name: "c_str", Type: core.ColumnType{Kind: core.KindString}},
			{Name: "c_json", Type: core.ColumnType{Kind: core.KindJSON}},
			{Name: "c_bin", Type: core.ColumnType{Kind: core.KindBinary}},
			{Name: "c_fsb", Type: core.ColumnType{Kind: core.KindFixedBinary, FixedSize: 16}},
			{Name: "c_uuid", Type: core.ColumnType{Kind: core.KindUUID}},
			{Name: "c_date", Type: core.ColumnType{Kind: core.KindDate}},
			{Name: "c_time", Type: core.ColumnType{Kind: core.KindTime}},
			{Name: "c_ts_naive", Type: core.ColumnType{Kind: core.KindTimestamp}},
			{Name: "c_ts_tz", Type: core.ColumnType{Kind: core.KindTimestampTZ}},
			{Name: "c_nullable", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
			{Name: "c_list", Type: core.ColumnType{Kind: core.KindList, Elem: &listElem}},
			{Name: "c_map", Type: core.ColumnType{Kind: core.KindMap, KeyType: &mapKey, ValueType: &mapVal}},
		},
		PrimaryKey: []string{"c_int64"},
	}

	b, err := EncodeTableSchema(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out, err := DecodeTableSchema(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(out.Columns) != len(in.Columns) {
		t.Fatalf("columns = %d, want %d", len(out.Columns), len(in.Columns))
	}
	got := make(map[string]core.ColumnType, len(out.Columns))
	for _, c := range out.Columns {
		got[c.Name] = c.Type
	}

	type want struct {
		kind core.Kind
		pre  int
		sca  int
		fx   int
		null bool
	}
	cases := map[string]want{
		"c_bool":     {kind: core.KindBool},
		"c_int32":    {kind: core.KindInt32},
		"c_int64":    {kind: core.KindInt64},
		"c_uint64":   {kind: core.KindUInt64},
		"c_f32":      {kind: core.KindFloat32},
		"c_f64":      {kind: core.KindFloat64},
		"c_dec":      {kind: core.KindDecimal, pre: 12, sca: 3},
		"c_str":      {kind: core.KindString},
		"c_json":     {kind: core.KindJSON},
		"c_bin":      {kind: core.KindBinary},
		"c_fsb":      {kind: core.KindFixedBinary, fx: 16},
		"c_uuid":     {kind: core.KindUUID},
		"c_date":     {kind: core.KindDate},
		"c_time":     {kind: core.KindTime},
		"c_ts_naive": {kind: core.KindTimestamp},
		"c_ts_tz":    {kind: core.KindTimestampTZ},
		"c_nullable": {kind: core.KindString, null: true},
	}
	for name, w := range cases {
		g, ok := got[name]
		if !ok {
			t.Errorf("column %q missing after round-trip", name)
			continue
		}
		if g.Kind != w.kind {
			t.Errorf("%s: kind = %v, want %v", name, g.Kind, w.kind)
		}
		if w.pre != 0 && (g.Precision != w.pre || g.Scale != w.sca) {
			t.Errorf("%s: precision/scale = %d/%d, want %d/%d", name, g.Precision, g.Scale, w.pre, w.sca)
		}
		if w.fx != 0 && g.FixedSize != w.fx {
			t.Errorf("%s: fixed size = %d, want %d", name, g.FixedSize, w.fx)
		}
		if g.Nullable != w.null {
			t.Errorf("%s: nullable = %v, want %v", name, g.Nullable, w.null)
		}
	}

	// The TZ distinction is the whole point of T-13: naive must not
	// collapse into TZ (or vice versa) — both read as time.Time in rows.
	if got["c_ts_naive"].Kind == got["c_ts_tz"].Kind {
		t.Fatal("KindTimestamp and KindTimestampTZ collapsed to the same kind")
	}

	// Composites: element/key/value survive with their own kinds+nullable.
	lst := got["c_list"]
	if lst.Kind != core.KindList || lst.Elem == nil || lst.Elem.Kind != core.KindString || !lst.Elem.Nullable {
		t.Errorf("c_list: got %+v, want list<string nullable>", lst)
	}
	mp := got["c_map"]
	if mp.Kind != core.KindMap || mp.KeyType == nil || mp.ValueType == nil ||
		mp.KeyType.Kind != core.KindString || mp.ValueType.Kind != core.KindInt64 || !mp.ValueType.Nullable {
		t.Errorf("c_map: got %+v, want map<string, int64 nullable>", mp)
	}
}

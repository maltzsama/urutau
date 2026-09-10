package transport

import (
	"bytes"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/ipc"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/rowchange"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

// encodeOne encodes a single-column batch and returns the reader.
func encodeOne(t *testing.T, ct core.ColumnType, v any) *BatchReader {
	t.Helper()
	cs := core.Schema{Columns: []core.Column{{Name: "c", Type: ct}}}
	rows := []rowchange.Change{{
		Op: rowchange.OpInsert, Table: "t",
		After: map[string]any{"c": v},
	}}
	body, _, err := EncodeBatch(rows, cs, &pb.BatchMeta{Table: "t"}, nil)
	if err != nil {
		t.Fatalf("EncodeBatch(%s, %T): %v", ct, v, err)
	}
	rd, err := ipc.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("ipc reader: %v", err)
	}
	if !rd.Next() {
		t.Fatal("empty record")
	}
	rec := rd.RecordBatch()
	rec.Retain()
	br, err := NewBatchReader(rec, nil)
	if err != nil {
		t.Fatalf("batch reader: %v", err)
	}
	return br
}

func equalValue(got, want any) bool {
	gt, gok := got.(time.Time)
	wt, wok := want.(time.Time)
	if gok && wok {
		return gt.Equal(wt)
	}
	return got == want
}

// The worker encodes against the resolved (cast-target) schema, so a source
// value can arrive in a Go type that differs from the target's canonical
// type. Each row below failed before the encoder learned the full matrix.
func TestEncoderAppliesCastMatrix(t *testing.T) {
	ts := time.Date(2024, 1, 2, 15, 4, 5, 0, time.UTC)
	midnight := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	micros := int64(15*3_600_000_000 + 4*60_000_000 + 5*1_000_000)
	cases := []struct {
		name string
		ct   core.ColumnType
		in   any
		want any
	}{
		// date/time/timestamp source values are time.Time (MySQL snapshot).
		{"date<-time", core.ColumnType{Kind: core.KindDate}, ts, int32(ts.Unix() / 86400)},
		{"timestamp<-time", core.ColumnType{Kind: core.KindTimestamp}, ts, ts},
		{"time<-time", core.ColumnType{Kind: core.KindTime}, ts, micros},
		// "to string always" from every scalar.
		{"string<-bool", core.ColumnType{Kind: core.KindString}, true, "true"},
		{"string<-int64", core.ColumnType{Kind: core.KindString}, int64(42), "42"},
		{"string<-float64", core.ColumnType{Kind: core.KindString}, float64(3.5), "3.5"},
		{"string<-time", core.ColumnType{Kind: core.KindString}, ts, "2024-01-02T15:04:05Z"},
		{"string<-map", core.ColumnType{Kind: core.KindString}, map[string]any{"a": float64(1)}, `{"a":1}`},
		// numeric -> decimal.
		{"decimal<-float", core.ColumnType{Kind: core.KindDecimal, Precision: 10, Scale: 2}, float64(3.14), "3.14"},
		// string temporal sources.
		{"timestamp<-string", core.ColumnType{Kind: core.KindTimestamp}, "2024-01-02 15:04:05", ts},
		{"date<-string", core.ColumnType{Kind: core.KindDate}, "2024-01-02", int32(midnight.Unix() / 86400)},
		{"time<-string", core.ColumnType{Kind: core.KindTime}, "15:04:05", micros},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rd := encodeOne(t, tc.ct, tc.in)
			got, ok := rd.Value("c", 0)
			if !ok {
				t.Fatalf("no value at row 0")
			}
			if !equalValue(got, tc.want) {
				t.Fatalf("round-trip = %#v (%T), want %#v (%T)", got, got, tc.want, tc.want)
			}
		})
	}
}

// The pipeline now encodes the SOURCE schema (core.WireSchema) and the sink
// casts with the wire Kind. These two cases were broken while the worker
// pre-cast to the target type: binary lost its encoding (became a raw
// string) and a date became an integer.
func TestSourceSchemaCastHandoff(t *testing.T) {
	// binary -> string(hex): the wire carries Binary, the sink hex-encodes.
	rd := encodeOne(t, core.ColumnType{Kind: core.KindBinary}, []byte{0xde, 0xad})
	kind, _ := rd.ColumnKind("c")
	v, _ := rd.Value("c", 0)
	got, err := (core.CastTarget{Type: core.ColumnType{Kind: core.KindString}, Encoding: "hex"}).Convert(kind, v)
	if err != nil || got != "dead" {
		t.Fatalf("binary -> string(hex) = %v, %v; want dead", got, err)
	}

	// date -> string: the wire carries Date32, the sink formats the date.
	days := int32(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix() / 86400)
	rd = encodeOne(t, core.ColumnType{Kind: core.KindDate}, days)
	kind, _ = rd.ColumnKind("c")
	v, _ = rd.Value("c", 0)
	got, err = (core.CastTarget{Type: core.ColumnType{Kind: core.KindString}}).Convert(kind, v)
	if err != nil || got != "2024-01-01" {
		t.Fatalf("date -> string = %v, %v; want 2024-01-01", got, err)
	}
}

// Package transport wire benchmarks — evidence for the #455 transport
// decision.
//
// Hypothesis (from the handoff): the coordinator "serializes the record into a
// bytes.Buffer, Flight ships bytes instead of a native record batch, and the
// worker deserializes it", so a native Flight record-batch stream would remove
// a copy. One run of these benchmarks on the author's machine (2000-row wire
// batch, id/name/amount, ~252 KB) gave:
//
//	EncodeRecord   ~154 us/op   ~700 KB/op   ~79 allocs/op
//	DecodeRecord    ~65 us/op   ~260 KB/op  ~123 allocs/op
//	ipc/rec ~ 1.00   (IPC body the same size as the record)
//
// Absolute numbers vary with CPU, Go and Arrow version — re-run to compare on
// yours. The RATIO is the point, and it is structural, not machine-dependent:
//
// Arrow Flight's wire format IS Arrow IPC, so "shipping a native record batch"
// still serializes to IPC once and deserializes once. There is no second copy
// to remove, no bandwidth win, and the transient allocations per batch are the
// unavoidable encode/decode, not a double materialization. The coordinator
// already releases the record after enqueue, so it holds only the body;
// holding the record instead is a wash. The bytes.Clone at codec.go is on the
// row path (readTypedValue), not the hot columnar path.
//
// CONCLUSION: the "zero-copy transport" frontier (#455) is NOT justified and
// was not implemented. These benchmarks stand as the evidence.
package transport

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/rowchange"
)

// recordSize sums the Arrow buffer bytes of a record (there is no
// RecordBatch.SizeInBytes; the accounting candidate for #579/#455).
func recordSize(rec arrow.RecordBatch) int {
	var n uint64
	for i := 0; i < int(rec.NumCols()); i++ {
		n += rec.Column(i).Data().SizeInBytes()
	}
	return int(n)
}

// benchRecord builds a wire record of n rows across a few columns, the shape
// the coordinator serializes per batch.
func benchRecord(b *testing.B, n int) (core.Schema, []rowchange.Change) {
	b.Helper()
	cs := core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "name", Type: core.ColumnType{Kind: core.KindString}},
			{Name: "amount", Type: core.ColumnType{Kind: core.KindFloat64}},
		},
		PrimaryKey: []string{"id"},
	}
	changes := make([]rowchange.Change, n)
	for i := range changes {
		changes[i] = rowchange.Change{
			Op:       rowchange.OpInsert,
			Table:    "raw.orders",
			Position: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-18818",
			CommitTS: time.Now(),
			After: map[string]any{
				"id":     int64(i),
				"name":   fmt.Sprintf("row-%08d-with-some-payload", i),
				"amount": float64(i) + 0.5,
			},
		}
	}
	return cs, changes
}

var benchBody []byte

// BenchmarkEncodeRecord measures the coordinator's eager serialization: the
// record → IPC bytes copy held in the queue (internal/coordinator enqueue).
func BenchmarkEncodeRecord(b *testing.B) {
	cs, changes := benchRecord(b, 2000)
	rec, err := RecordFromChanges(changes, cs, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer rec.Release()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		body, err := EncodeRecord(rec)
		if err != nil {
			b.Fatal(err)
		}
		benchBody = body
	}
	b.StopTimer()
	b.ReportMetric(float64(recordSize(rec)), "rec_size_B")
	b.ReportMetric(float64(len(benchBody)), "ipc_size_B")
	b.ReportMetric(float64(len(benchBody))/float64(recordSize(rec)), "ipc/rec")
}

// BenchmarkDecodeRecord measures the worker's deserialization of one body.
func BenchmarkDecodeRecord(b *testing.B) {
	cs, changes := benchRecord(b, 2000)
	rec, err := RecordFromChanges(changes, cs, nil)
	if err != nil {
		b.Fatal(err)
	}
	body, err := EncodeRecord(rec)
	rec.Release()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := ipc.NewReader(bytes.NewReader(body))
		if err != nil {
			b.Fatal(err)
		}
		got, err := r.Read()
		if err != nil {
			b.Fatal(err)
		}
		if got != nil {
			got.Release()
		}
		r.Release()
	}
}

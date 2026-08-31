// Flight data-plane codec: change batches travel as Arrow IPC records with
// a BatchMeta proto in the FlightData app_metadata. Rows use a stable wire
// schema — op, key (JSON tuple), before/after (JSON objects, nullable),
// position. JSON cannot distinguish whole floats from ints, so integral
// numbers decode as int64; the sink coerces to the column type from the
// table schema, which is authoritative.
package transport

import (
	"encoding/json"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/protobuf/proto"

	"github.com/maltzsama/urutau/internal/change"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

// ChangeSchema is the wire schema of one change row batch.
var ChangeSchema = arrow.NewSchema([]arrow.Field{
	{Name: "op", Type: arrow.PrimitiveTypes.Uint8, Nullable: false},
	{Name: "key", Type: arrow.BinaryTypes.String, Nullable: false},
	{Name: "before", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "after", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "position", Type: arrow.BinaryTypes.String, Nullable: false},
}, nil)

// EncodeBatch renders rows as an Arrow record; meta marshals into the
// FlightData app_metadata.
func EncodeBatch(rows []change.Change, meta *pb.BatchMeta) (arrow.RecordBatch, []byte, error) {
	metaBytes, err := proto.Marshal(meta)
	if err != nil {
		return nil, nil, fmt.Errorf("transport: marshal batch meta: %w", err)
	}

	bld := array.NewRecordBuilder(memory.DefaultAllocator, ChangeSchema)
	defer bld.Release()

	for i := range rows {
		bld.Field(0).(*array.Uint8Builder).Append(uint8(rows[i].Op))
		key, err := json.Marshal(rows[i].Key)
		if err != nil {
			return nil, nil, fmt.Errorf("transport: marshal key: %w", err)
		}
		bld.Field(1).(*array.StringBuilder).Append(string(key))
		appendJSON(bld.Field(2), rows[i].Before)
		appendJSON(bld.Field(3), rows[i].After)
		bld.Field(4).(*array.StringBuilder).Append(rows[i].Position)
	}
	return bld.NewRecordBatch(), metaBytes, nil
}

// DecodeBatch reads a record + app_metadata back into rows and meta.
// Window tags are per-batch (meta.Window): the caller routes snapshot rows
// to the window builder and stamps live rows with the batch's tag — a
// batch is always homogeneous by contract.
func DecodeBatch(rec arrow.RecordBatch, metaBytes []byte) ([]change.Change, *pb.BatchMeta, error) {
	meta := &pb.BatchMeta{}
	if err := proto.Unmarshal(metaBytes, meta); err != nil {
		return nil, nil, fmt.Errorf("transport: unmarshal batch meta: %w", err)
	}

	opCol, ok := rec.Column(0).(*array.Uint8)
	if !ok {
		return nil, nil, fmt.Errorf("transport: op column type %T", rec.Column(0))
	}
	keyCol, ok := rec.Column(1).(*array.String)
	if !ok {
		return nil, nil, fmt.Errorf("transport: key column type %T", rec.Column(1))
	}
	beforeCol, ok := rec.Column(2).(*array.String)
	if !ok {
		return nil, nil, fmt.Errorf("transport: before column type %T", rec.Column(2))
	}
	afterCol, ok := rec.Column(3).(*array.String)
	if !ok {
		return nil, nil, fmt.Errorf("transport: after column type %T", rec.Column(3))
	}
	posCol, ok := rec.Column(4).(*array.String)
	if !ok {
		return nil, nil, fmt.Errorf("transport: position column type %T", rec.Column(4))
	}

	rows := make([]change.Change, 0, rec.NumRows())
	for i := 0; i < int(rec.NumRows()); i++ {
		c := change.Change{
			Op:       change.Op(opCol.Value(i)),
			Position: posCol.Value(i),
			Table:    meta.Table,
		}
		if err := json.Unmarshal([]byte(keyCol.Value(i)), &c.Key); err != nil {
			return nil, nil, fmt.Errorf("transport: unmarshal key: %w", err)
		}
		normalizeJSON(c.Key)
		if !beforeCol.IsNull(i) {
			if err := json.Unmarshal([]byte(beforeCol.Value(i)), &c.Before); err != nil {
				return nil, nil, fmt.Errorf("transport: unmarshal before: %w", err)
			}
			normalizeMap(c.Before)
		}
		if !afterCol.IsNull(i) {
			if err := json.Unmarshal([]byte(afterCol.Value(i)), &c.After); err != nil {
				return nil, nil, fmt.Errorf("transport: unmarshal after: %w", err)
			}
			normalizeMap(c.After)
		}
		rows = append(rows, c)
	}
	return rows, meta, nil
}

func appendJSON(bld array.Builder, m map[string]any) {
	if m == nil {
		bld.AppendNull()
		return
	}
	b, err := json.Marshal(m)
	if err != nil {
		bld.AppendNull()
		return
	}
	bld.(*array.StringBuilder).Append(string(b))
}

// normalizeJSON restores integral JSON numbers to int64 in a key tuple.
func normalizeJSON(v []any) {
	for i, e := range v {
		if f, ok := e.(float64); ok && f == float64(int64(f)) {
			v[i] = int64(f)
		}
	}
}

// normalizeMap restores integral JSON numbers to int64 in a row image.
func normalizeMap(m map[string]any) {
	for k, e := range m {
		if f, ok := e.(float64); ok && f == float64(int64(f)) {
			m[k] = int64(f)
		}
	}
}

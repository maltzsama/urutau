package dataplane

import (
	"github.com/apache/arrow-go/v18/arrow"

	"github.com/maltzsama/urutau/dataplane"
)

// BatchBytes is a batch's in-memory size: the length of every buffer of every
// column, child arrays included. Rows carry payloads, so a row count alone is
// no bound on memory: batching limits check this as well (#437).
func BatchBytes(b *dataplane.Batch) int64 {
	if b == nil || b.Record == nil {
		return 0
	}
	var n int64
	for _, col := range b.Record.Columns() {
		n += arrayDataBytes(col.Data())
	}
	return n
}

func arrayDataBytes(d arrow.ArrayData) int64 {
	var n int64
	for _, buf := range d.Buffers() {
		if buf != nil {
			n += int64(buf.Len())
		}
	}
	for _, child := range d.Children() {
		n += arrayDataBytes(child)
	}
	return n
}

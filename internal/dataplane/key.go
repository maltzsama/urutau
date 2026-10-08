package dataplane

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// EncodeKey encodes a composite key as type-tagged binary payload concat —
// used by collapse to avoid hash collisions (CR-069 §3.2).
//
// pkIdxs are the pre-resolved data-column indices of the PK columns
// (resolved ONCE by the caller — M-9); pkCols ride along for error
// messages only. Null in any PK column is an error here — EncodeKey is
// the single authority for key validity (M-9).
//
// Each field is [type-byte][payload…]:
// fixed-width payloads for numerics/bool/date/time/timestamp, length-
// prefixed for string/binary/decimal, raw fixed width for FixedSizeBinary.
// Different types cannot collide (distinct tags); equal values of the same
// type always produce equal keys (NaN/±0 compare by bit pattern — M-11).
//
// INVARIANT: keys are EPHEMERAL — they live for the duration of one
// Collapse call and never persist or cross batches. If keys ever need to
// persist or be compared across batches, this design must be revisited
// (type widening across batches, e.g. int32→int64, would change keys).
// See website/docs/architecture/encode-key.md.
func EncodeKey(record arrow.RecordBatch, row int, pkIdxs []int, pkCols []string) ([]byte, error) {
	const (
		typeInt32   byte = 0x01
		typeInt64   byte = 0x02
		typeUInt64  byte = 0x03
		typeFloat32 byte = 0x04
		typeFloat64 byte = 0x05
		typeBool    byte = 0x06
		typeString  byte = 0x07
		typeDecimal byte = 0x08
		typeDate32  byte = 0x09
		typeTime64  byte = 0x0A
		// typeTimestamp covers every *array.Timestamp regardless of
		// timezone or unit — keys are per-batch and a column has a single
		// type within a batch, so unit/TZ normalization is unnecessary.
		typeTimestamp byte = 0x0B
		typeBinary    byte = 0x0C
		typeFSB       byte = 0x0D // FixedSizeBinary (UUID)
		// Narrow integer widths carry their OWN tag: reusing typeInt32 for an
		// Int16 payload would make the tag ambiguous with a truncated Int32
		// (issue #220).
		typeInt8   byte = 0x0E
		typeInt16  byte = 0x0F
		typeUInt8  byte = 0x10
		typeUInt16 byte = 0x11
		typeUInt32 byte = 0x12
	)
	var buf []byte
	var lenBuf [4]byte
	for i, col := range pkCols {
		if i >= len(pkIdxs) {
			return nil, fmt.Errorf("dataplane: encode key: pkIdxs (%d) shorter than pkCols (%d)", len(pkIdxs), len(pkCols))
		}
		idx := pkIdxs[i]
		if idx < 0 || idx >= int(record.NumCols()) {
			return nil, fmt.Errorf("dataplane: encode key: column %q index %d out of range", col, idx)
		}
		arr := record.Column(idx)
		if arr.IsNull(row) {
			return nil, fmt.Errorf("dataplane: encode key: null in PK column %q at row %d", col, row)
		}
		switch a := arr.(type) {
		case *array.Int8:
			buf = append(buf, typeInt8)
			buf = append(buf, byte(a.Value(row)))
		case *array.Int16:
			buf = append(buf, typeInt16)
			var v [2]byte
			binary.LittleEndian.PutUint16(v[:], uint16(a.Value(row)))
			buf = append(buf, v[:]...)
		case *array.Int32:
			buf = append(buf, typeInt32)
			binary.LittleEndian.PutUint32(lenBuf[:], uint32(a.Value(row)))
			buf = append(buf, lenBuf[:]...)
		case *array.Int64:
			buf = append(buf, typeInt64)
			var v [8]byte
			binary.LittleEndian.PutUint64(v[:], uint64(a.Value(row)))
			buf = append(buf, v[:]...)
		case *array.Uint8:
			buf = append(buf, typeUInt8)
			buf = append(buf, a.Value(row))
		case *array.Uint16:
			buf = append(buf, typeUInt16)
			var v [2]byte
			binary.LittleEndian.PutUint16(v[:], a.Value(row))
			buf = append(buf, v[:]...)
		case *array.Uint32:
			buf = append(buf, typeUInt32)
			binary.LittleEndian.PutUint32(lenBuf[:], a.Value(row))
			buf = append(buf, lenBuf[:]...)
		case *array.Uint64:
			buf = append(buf, typeUInt64)
			var v [8]byte
			binary.LittleEndian.PutUint64(v[:], a.Value(row))
			buf = append(buf, v[:]...)
		case *array.Float32:
			buf = append(buf, typeFloat32)
			binary.LittleEndian.PutUint32(lenBuf[:], math.Float32bits(a.Value(row)))
			buf = append(buf, lenBuf[:]...)
		case *array.Float64:
			buf = append(buf, typeFloat64)
			var v [8]byte
			binary.LittleEndian.PutUint64(v[:], math.Float64bits(a.Value(row)))
			buf = append(buf, v[:]...)
		case *array.Boolean:
			buf = append(buf, typeBool)
			if a.Value(row) {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}
		case *array.String:
			buf = append(buf, typeString)
			s := a.Value(row)
			binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(s)))
			buf = append(buf, lenBuf[:]...)
			buf = append(buf, s...)
		case *array.Date32:
			buf = append(buf, typeDate32)
			binary.LittleEndian.PutUint32(lenBuf[:], uint32(a.Value(row)))
			buf = append(buf, lenBuf[:]...)
		case *array.Time64:
			buf = append(buf, typeTime64)
			var v [8]byte
			binary.LittleEndian.PutUint64(v[:], uint64(a.Value(row)))
			buf = append(buf, v[:]...)
		case *array.Timestamp:
			buf = append(buf, typeTimestamp)
			var v [8]byte
			binary.LittleEndian.PutUint64(v[:], uint64(a.Value(row)))
			buf = append(buf, v[:]...)
		case *array.Decimal128:
			// Encode the 16 raw bytes instead of ValueStr's base-10 string:
			// no heap allocation on the Collapse hot path. Two decimals of
			// equal magnitude but different scale (1.5 vs 1.50) have distinct
			// bit patterns — exactly as ValueStr produces distinct strings, so
			// the collapse behaviour is unchanged (issue #223).
			buf = append(buf, typeDecimal)
			dec := a.Value(row)
			var raw [16]byte
			binary.LittleEndian.PutUint64(raw[0:8], dec.LowBits())
			binary.LittleEndian.PutUint64(raw[8:16], uint64(dec.HighBits()))
			buf = append(buf, raw[:]...)
		case *array.Binary:
			buf = append(buf, typeBinary)
			b := a.Value(row)
			binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(b)))
			buf = append(buf, lenBuf[:]...)
			buf = append(buf, b...)
		case *array.FixedSizeBinary:
			buf = append(buf, typeFSB)
			buf = append(buf, a.Value(row)...)
		default:
			return nil, fmt.Errorf("dataplane: encode key: unsupported column type %T for PK column %q", arr, col)
		}
	}
	return buf, nil
}

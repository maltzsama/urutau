// Package routing decides which partition owner holds a primary key, and
// only that. It is the single owner function the snapshot and the live
// stream both go through, so a key can never be owned by two workers at the
// same time.
//
// The decision is a pure function of (key, live worker set):
//
//	slot  = xxhash64(seed, EncodeKey(key)) % Slots
//	owner = argmax_w xxhash64(seed, slot ‖ w)   (rendezvous hashing)
//
// Nothing is persisted: Slots and the encoding are compile-time constants,
// and the slot-to-owner map is recomputed from the live owners on every boot
// and every re-slice. Rendezvous hashing gives the minimal-movement
// property: when a worker joins or leaves, only the keys it gains or loses
// change owner, and every other key keeps the same one (a fixed-slot modulo
// assignment would instead remap almost everything).
//
// EncodeKey is a canonical, type-tagged, length-framed encoding of a typed
// primary-key tuple. It is deliberately NOT dataplane.EncodeKey: that one is
// documented ephemeral (its result depends on the Arrow runtime width, so an
// int32 and an int64 of the same value can differ) and must never be compared
// across batches. Routing DOES need to compare across batches — the snapshot
// decode and the binlog decode of the same row must yield the same bytes —
// so this encoding normalises every integer width to one tag, a sign and a
// fixed-width big-endian magnitude.
package routing

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/cespare/xxhash/v2"
)

// Slots is the fixed number of hash slots (virtual nodes). It is a constant
// of the design, never persisted and never changed: a table's key always
// maps to the same slot for the life of the pipeline, independent of worker
// count. A re-slice moves slots between owners; it never re-hashes a key.
const Slots = 4096

// hashVersion is folded into the seed. Bump it only with a deliberate
// migration: changing it re-homes every key of every table, which is a full
// re-keying from the sink's point of view.
const hashVersion = 1

// seedPrefix is the fixed per-hash salt: the ASCII of "urutau" plus the
// version byte. Changing it changes every slot.
var seedPrefix = []byte{
	'u', 'r', 'u', 't', 'a', 'u', byte(hashVersion),
}

// Value tags. A tag distinguishes types that could otherwise share payload
// bytes (a date and an int are both int32 on the wire; an int and a string
// must never collide). Tags are part of the on-the-wire key contract: never
// renumber an existing one.
const (
	tagBool   byte = 0x01
	tagInt    byte = 0x02
	tagFloat  byte = 0x03
	tagString byte = 0x04
	tagBytes  byte = 0x05
	tagTime   byte = 0x06
)

// EncodeKey canonically encodes a primary-key tuple. Elements are encoded in
// order, each prefixed by its type tag and a 4-byte big-endian length, so a
// composite key cannot have two different tuples encode to the same bytes
// (length framing prevents [1, 23] and [12, 3] from colliding).
//
// A nil element is an error: routing a row with a null key column is a
// contract violation, and guessing an owner for it would be worse than
// failing the batch.
func EncodeKey(key []any) ([]byte, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("routing: empty primary key")
	}
	var buf []byte
	var lenBuf [4]byte
	for i, v := range key {
		if v == nil {
			return nil, fmt.Errorf("routing: key column %d is null", i)
		}
		tag, payload, err := encodeValue(v)
		if err != nil {
			return nil, fmt.Errorf("routing: key column %d: %w", i, err)
		}
		buf = append(buf, tag)
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))
		buf = append(buf, lenBuf[:]...)
		buf = append(buf, payload...)
	}
	return buf, nil
}

// encodeValue returns the type tag and payload for one key value. The
// accepted Go types are exactly the ones transport.readTypedValue produces
// for the canonical key kinds: bool, int32/int64/uint64 (and the other
// integer widths), float64, string, []byte, and time.Time.
func encodeValue(v any) (byte, []byte, error) {
	switch t := v.(type) {
	case bool:
		if t {
			return tagBool, []byte{1}, nil
		}
		return tagBool, []byte{0}, nil
	case int:
		return tagInt, encodeInteger(t < 0, magnitude(int64(t), t < 0)), nil
	case int8:
		return tagInt, encodeInteger(t < 0, magnitude(int64(t), t < 0)), nil
	case int16:
		return tagInt, encodeInteger(t < 0, magnitude(int64(t), t < 0)), nil
	case int32:
		return tagInt, encodeInteger(t < 0, magnitude(int64(t), t < 0)), nil
	case int64:
		return tagInt, encodeInteger(t < 0, magnitude(t, t < 0)), nil
	case uint:
		return tagInt, encodeInteger(false, uint64(t)), nil
	case uint8:
		return tagInt, encodeInteger(false, uint64(t)), nil
	case uint16:
		return tagInt, encodeInteger(false, uint64(t)), nil
	case uint32:
		return tagInt, encodeInteger(false, uint64(t)), nil
	case uint64:
		return tagInt, encodeInteger(false, t), nil
	case float32:
		tag, p := encodeFloat(float64(t))
		return tag, p, nil
	case float64:
		tag, p := encodeFloat(t)
		return tag, p, nil
	case string:
		return tagString, []byte(t), nil
	case []byte:
		return tagBytes, t, nil
	case time.Time:
		// Normalise to UTC nano so equal instants encode equal regardless of
		// location. The decoders already truncate to microsecond precision,
		// so the low three digits are a stable multiple of 1000.
		var p [8]byte
		binary.BigEndian.PutUint64(p[:], uint64(t.UTC().UnixNano()))
		return tagTime, p[:], nil
	default:
		return 0, nil, fmt.Errorf("unsupported key value type %T", v)
	}
}

// magnitude returns |v| as a uint64. For a negative value it is the
// two's-complement magnitude, so MinInt64 does not overflow.
func magnitude(v int64, neg bool) uint64 {
	if !neg {
		return uint64(v)
	}
	return uint64(-(v + 1)) + 1
}

// encodeInteger frames an exact integer as a sign byte plus an 8-byte
// big-endian magnitude. Every width and signedness of the same mathematical
// value encodes identically, which is what makes a snapshot int32 and a
// binlog int64 of the same key route to the same owner.
func encodeInteger(neg bool, mag uint64) []byte {
	p := make([]byte, 9)
	if neg {
		p[0] = 1
	}
	binary.BigEndian.PutUint64(p[1:], mag)
	return p
}

func encodeFloat(f float64) (byte, []byte) {
	var p [8]byte
	// Canonicalise -0.0 to +0.0 so a sign-of-zero difference cannot split a
	// key; NaN is left as-is (float primary keys are rejected upstream).
	if f == 0 {
		f = 0
	}
	binary.BigEndian.PutUint64(p[:], math.Float64bits(f))
	return tagFloat, p[:]
}

// Slot returns key's slot, a stable value in [0, Slots) for the life of the
// pipeline. It is derived from EncodeKey, so callers that already have the
// encoded key should use SlotOfKey to avoid re-encoding.
func Slot(key []any) (uint32, error) {
	enc, err := EncodeKey(key)
	if err != nil {
		return 0, err
	}
	return SlotOfKey(enc), nil
}

// SlotOfKey returns the slot of an already-encoded key.
func SlotOfKey(encoded []byte) uint32 {
	h := xxhash.New()
	_, _ = h.Write(seedPrefix)
	_, _ = h.Write(encoded)
	return uint32(h.Sum64() % Slots)
}

// Owner returns the index into workers of the owner of slot, using
// rendezvous hashing. Workers is the full live set; its order does not
// matter (the winner is chosen by hash, with the worker name as an
// order-independent tie-break). Returns -1 for an empty worker set.
func Owner(slot uint32, workers []string) int {
	if len(workers) == 0 {
		return -1
	}
	var sb [4]byte
	binary.BigEndian.PutUint32(sb[:], slot)

	bestIdx := 0
	best := rendezvousWeight(sb[:], workers[0])
	for i := 1; i < len(workers); i++ {
		w := rendezvousWeight(sb[:], workers[i])
		// Deterministic regardless of workers order: a greater weight wins,
		// and an exact tie goes to the lexicographically smaller name.
		if w > best || (w == best && workers[i] < workers[bestIdx]) {
			best, bestIdx = w, i
		}
	}
	return bestIdx
}

// OwnerOfKey is the whole decision in one call: encode, slot, owner.
func OwnerOfKey(key []any, workers []string) (int, error) {
	enc, err := EncodeKey(key)
	if err != nil {
		return -1, err
	}
	return Owner(SlotOfKey(enc), workers), nil
}

func rendezvousWeight(slotBytes []byte, worker string) uint64 {
	h := xxhash.New()
	_, _ = h.Write(seedPrefix)
	_, _ = h.Write(slotBytes)
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(worker))
	return h.Sum64()
}

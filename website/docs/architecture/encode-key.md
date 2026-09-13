---
sidebar_position: 2
---

# EncodeKey — design and invariant (supersedes the v4 `n:`/`f:` table)

> **Status:** implemented in `internal/dataplane/generator.go` (`EncodeKey`).
> **Supersedes:** the H-1 section of `DP-AUDIT v4` proposed value-space
> prefixes (`n:`, `f:`, `s:`, `bin:`) with stability under widening
> (`int32(5)` and `int64(5)` → the same key). The implementation uses
> **binary type-tags with fixed-width payloads**. This doc records the
> accepted deviation, the invariant the code guarantees, and the caveat
> that makes it safe.

## 1. What changed from v4

| | v4 (value space) | implemented (type-tag) |
|---|---|---|
| Encoding | `[text prefix][decimal/hex]` | `[1 byte tag][binary payload]` |
| `int32(5)` vs `int64(5)` | same key (`n:5`) | **different keys** (tags `0x01` vs `0x02`) |
| `float32(0.1)` vs `float64(0.1)` | same key (promotion to f64) | **different keys** (distinct bit patterns) |
| NaN / ±0 | NaN → error (M-11) | NaN/±0 compare by bit pattern — deterministic, M-11 dies |
| Performance | `fmt.Sprintf`/`FormatInt` per value | direct binary append, no formatting |

## 2. Why the type-tag is better here

The real consumer is **Collapse: per batch, in memory**. The key:

- **never persists** — it lives only inside one Collapse call;
- **never crosses batches** — a batch has one fixed type per column;
- **never crosses schema versions** — there is no real int32↔int64
  comparison.

In that regime the v4 widening argument ("ALTER INT→BIGINT splits a
logical row") does not materialize: a PK column's type change happens
**between** batches, and the key never outlives the batch. The type-tag:

1. is **more injective** — every type has its own tag, no value-space
   overlap (`bin:` covered Binary/FSB/UUID with raw bytes; distinct tags
   separate them even with identical bytes);
2. is **faster** — no decimal formatting of integers/floats;
3. makes NaN/±0 **deterministic by bit pattern** for free.

## 3. The invariant (what the code guarantees)

> `key(a) == key(b) ⟺ value(a) == value(b)` **within the same type** — and
> each PK column's type is constant within a batch.

Each field is `[type-byte][payload]`; the payload is length-prefixed where
width varies (string/binary/decimal) and fixed-width elsewhere. The
per-value length prefix kills the concatenation collision
(`"ab"+"c" == "a"+"bc"` — the dedicated CR-069 §3.2 adversary).

Tag table (see `generator.go`):

| Tag | Arrow type | Payload |
|---|---|---|
| `0x01`–`0x07` | Int32, Int64, Uint64, Float32, Float64, Bool, String | LE width-fixed; string length-prefixed |
| `0x08` | Decimal128 | canonical `ValueStr`, length-prefixed |
| `0x09`/`0x0A` | Date32, Time64 | LE 4/8 bytes |
| `0x0B` | Timestamp (any TZ/unit) | LE 8 bytes |
| `0x0C` | Binary | length-prefixed |
| `0x0D` | FixedSizeBinary (UUID) | raw width-fixed |

Null in a PK column is an **error** — `EncodeKey` is the single authority
for key validity (M-9); Collapse does not pre-check.

## 4. The caveat (what makes the deviation safe)

> **Keys are ephemeral.** If a key ever needs to **persist** or be
> **compared across batches/schema versions**, this design is back on the
> table: under type-tags, an int32→int64 widening changes the key of the
> same logical row — exactly the scenario that motivated the v4
> value-space prefix.

This record is the acceptance condition of the deviation (reviewer round,
revised T-9): the v4 "widening stability" sub-case **dies by design** —
`TestEncodeKeyTypeAntiCollision` now asserts the opposite (`int32(5) ≠
int64(5)`, `float32(0.1) ≠ float64(0.1)`, UUID ≠ Binary with identical
bytes).

## 5. Signature and complexity

```go
func EncodeKey(record arrow.RecordBatch, row int, pkIdxs []int, pkCols []string) ([]byte, error)
```

- `pkIdxs` are resolved **once** by the caller (M-9) — no per-row name
  lookup (was O(rows×cols));
- `pkCols` exist only for error messages.

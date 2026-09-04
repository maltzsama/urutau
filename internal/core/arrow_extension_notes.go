// Arrow canonical extension types — registration, not implementation.
//
// The Arrow Canonical Extension Types list was reviewed against the same
// admission criterion as the Kind set: a value enters the canonical system
// only when a supported source produces it AND a supported sink can hold it.
// Most of the list fails on one side or the other and is handled by the
// escape valve (KindUnknown + explicit cast). Three entries survived review;
// two were acted on, and the rest are registered here so the decision is
// conscious rather than a future surprise.
//
// ADOPTED — uuid. KindUUID travels as FixedSizeBinary(16) big-endian with no
// interpretation of the bytes, exactly the Arrow canonical form, and carries
// the arrow.uuid extension label on the wire so it is distinguishable from a
// fixed(16) byte column. See transport/schema.go.
//
// ADOPTED — json. KindJSON is stored as a string (the Arrow canonical
// storage) and its wire field carries the arrow.json extension label, so a
// consumer reading the RecordBatch directly knows the string is JSON without
// guessing.
//
// SPIKED SEPARATELY, NOT IMPLEMENTED — Parquet Variant (a future KindVariant
// for free-shape payloads, distinct from rigid Struct/List/Map). iceberg-go
// can write it, but the project's discipline is to read a write back with
// Trino before trusting it, and the reference Trino is not v3-ready in this
// window. A dedicated spike (write Variant via iceberg-go, read it back with
// Trino on format_version=3, confirm per-field navigation) gates promotion.
//
// REGISTERED, NO ACTION — timestamp_with_offset: each row keeps its own UTC
// offset, which no supported source produces (MySQL TIMESTAMP and Postgres
// timestamptz both normalize to UTC and discard the original offset). The
// target representation if a source ever preserves per-row offsets (Oracle
// TIMESTAMP WITH TIME ZONE, SQL Server datetimeoffset) is the Arrow
// struct{timestamp, offset_minutes} form — not a new invented one.
//
// REGISTERED, NO ACTION — Bool8 is a physical encoding (a byte per bool for
// zero-copy compat with systems that store bool in one byte), not a new
// semantic type; KindBool already covers the meaning.
//
// REGISTERED, NO ACTION — tensors (fixed/variable_shape_tensor) and GeoArrow:
// no supported source produces them natively (MySQL geometry already lands in
// the escape valve as opaque). Iceberg v3 is adding its own geometry types in
// the same maturation window as Variant; if those mature, geometry may gain a
// dedicated Kind via the same spike discipline. Until then it stays opaque.
package core

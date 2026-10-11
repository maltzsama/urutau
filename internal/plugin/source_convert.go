package plugin

import (
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/plugin/contract"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
)

// cdcRecordToWire projects one plugin CDC record (contract §8.1, before/
// after struct shaped) straight into the flat urutau wire schema. The
// before/after struct children become top-level data columns; op maps to
// __op; offset rides __pos; ts_source rides __commit_ts. Returns the wire
// record (nil if every row had an unknown op) and the last row's offset for
// the reader's position.
func cdcRecordToWire(rec arrow.RecordBatch, arrowSchema *arrow.Schema, ref core.TableRef, alloc memory.Allocator) (arrow.RecordBatch, string, error) {
	changes, err := cdcRecordToChanges(rec, arrowSchema, ref)
	if err != nil {
		return nil, "", err
	}
	if len(changes) == 0 {
		return nil, "", nil
	}
	// The wire schema is inferred from the change images — the plugin's
	// announced Arrow schema is struct-shaped, not the flat data schema, so
	// MergeSchema over the rows is the source of the flat column types.
	cs := transport.MergeSchema(changes, core.Schema{PrimaryKey: ref.PrimaryKey})
	wire, err := transport.RecordFromChanges(changes, cs, alloc)
	if err != nil {
		return nil, "", fmt.Errorf("plugin: encode wire batch: %w", err)
	}
	return wire, changes[len(changes)-1].Position, nil
}

// cdcRecordToChanges translates one Arrow CDC record batch into change events.
func cdcRecordToChanges(rec arrow.RecordBatch, arrowSchema *arrow.Schema, ref core.TableRef) ([]rowchange.Change, error) {
	nrows := int(rec.NumRows())
	changes := make([]rowchange.Change, 0, nrows)

	opIdx := columnIndex(arrowSchema, "op")
	beforeIdx := columnIndex(arrowSchema, "before")
	afterIdx := columnIndex(arrowSchema, "after")
	offsetIdx := columnIndex(arrowSchema, "offset")
	tsIdx := columnIndex(arrowSchema, "ts_source")

	for i := 0; i < nrows; i++ {
		op := readStringCol(rec, opIdx, i)
		offset := readBinaryCol(rec, offsetIdx, i)
		var commitTS time.Time
		if tsIdx >= 0 && !rec.Column(tsIdx).IsNull(i) {
			// §8.1 fixes ts_source at Timestamp(ns, "UTC") and the schema
			// gate enforces it — decode ticks AS NANOSECONDS. The old
			// UnixMicro read misinterpreted ns ticks by 1000x.
			tsCol := rec.Column(tsIdx).(*array.Timestamp)
			commitTS = tsCol.Value(i).ToTime(arrow.Nanosecond).UTC()
		}

		var chg rowchange.Change
		switch op {
		case string(contract.OpInsert):
			chg.Op = rowchange.OpInsert
		case string(contract.OpUpdate):
			chg.Op = rowchange.OpUpdate
		case string(contract.OpDelete):
			chg.Op = rowchange.OpDelete
		default:
			continue
		}

		chg.Table = ref.Target
		chg.Position = offset
		chg.CommitTS = commitTS
		chg.IngestTS = time.Now().UTC()

		if beforeIdx >= 0 && !rec.Column(beforeIdx).IsNull(i) {
			before, err := structToMap(rec.Column(beforeIdx), i)
			if err != nil {
				return nil, err
			}
			chg.Before = before
		}
		if afterIdx >= 0 && !rec.Column(afterIdx).IsNull(i) {
			after, err := structToMap(rec.Column(afterIdx), i)
			if err != nil {
				return nil, err
			}
			chg.After = after
			chg.Key = extractPK(ref.PrimaryKey, chg.After)
		}

		if chg.Key == nil && chg.Before != nil {
			chg.Key = extractPK(ref.PrimaryKey, chg.Before)
		}

		changes = append(changes, chg)
	}
	return changes, nil
}

// validateChangeSchema enforces contract §8.1 (change record, v1, fixed) on
// the plugin's announced schema — BEFORE any record is consumed (the
// sink-side §10 rule applied symmetrically: rejection at the first batch,
// not the tenth). A plugin is third-party code in any language: a wrong
// column type here used to panic the reader deep in the stream instead of
// failing the stream cleanly.
//
// Contract shape (exactly, in order; before MAY be omitted):
//
//	op        Utf8                    NOT NULL   ("c" | "u" | "d")
//	before    Struct<table columns>   NULL       (row null on insert)  [optional]
//	after     Struct<table columns>   NULL       (row null on delete)
//	offset    Binary                  NOT NULL   (opaque, §8.2)
//	ts_source Timestamp(ns, "UTC")    NULL
func validateChangeSchema(s *arrow.Schema) error {
	// before is the only optional field; when omitted, the rest shift up.
	names := []string{"op", "after", "offset", "ts_source"}
	if s.NumFields() == 5 {
		names = []string{"op", "before", "after", "offset", "ts_source"}
	} else if s.NumFields() != 4 {
		return fmt.Errorf("plugin schema: %d fields, want 4 (before omitted) or 5 per contract §8.1", s.NumFields())
	}

	checks := map[string]struct {
		is   func(arrow.DataType) bool
		desc string
		null bool
	}{
		"op":        {func(dt arrow.DataType) bool { return dt.ID() == arrow.STRING }, "Utf8", false},
		"before":    {func(dt arrow.DataType) bool { return dt.ID() == arrow.STRUCT }, "Struct", true},
		"after":     {func(dt arrow.DataType) bool { return dt.ID() == arrow.STRUCT }, "Struct", true},
		"offset":    {func(dt arrow.DataType) bool { return dt.ID() == arrow.BINARY }, "Binary", false},
		"ts_source": {func(dt arrow.DataType) bool { return dt.ID() == arrow.TIMESTAMP }, "Timestamp(ns, UTC)", true},
	}

	for i, name := range names {
		f := s.Field(i)
		if f.Name != name {
			return fmt.Errorf("plugin schema: field %d is %q, want %q per contract §8.1", i, f.Name, name)
		}
		want := checks[name]
		if !want.is(f.Type) {
			return fmt.Errorf("plugin schema: field %d %q is %s, want %s per contract §8.1", i, f.Name, f.Type, want.desc)
		}
		if f.Nullable != want.null {
			return fmt.Errorf("plugin schema: field %d %q: nullability %v, want %v per contract §8.1", i, f.Name, f.Nullable, want.null)
		}
	}
	if s.NumFields() == 5 {
		ts := s.Field(4).Type.(*arrow.TimestampType)
		if ts.Unit != arrow.Nanosecond || ts.TimeZone != "UTC" {
			return fmt.Errorf("plugin schema: field 4 ts_source is Timestamp(%s, %q), want Timestamp(ns, UTC) per contract §8.1", ts.Unit, ts.TimeZone)
		}
	}
	return nil
}

// recordMatchesAnnouncedSchema rejects mid-stream schema drift (contract §8:
// the schema is fixed within the stream). Checked on the first record of
// every stream; a plugin that announces one schema and ships another must
// fail the stream, not shift every column read by one.
func recordMatchesAnnouncedSchema(recSchema, announced *arrow.Schema) error {
	if recSchema.NumFields() != announced.NumFields() {
		return fmt.Errorf("plugin record: %d fields, announced %d — schema drifted mid-stream (contract §8)", recSchema.NumFields(), announced.NumFields())
	}
	for i := range recSchema.NumFields() {
		rf, af := recSchema.Field(i), announced.Field(i)
		if rf.Name != af.Name {
			return fmt.Errorf("plugin record: field %d is %q, announced %q — schema drifted mid-stream (contract §8)", i, rf.Name, af.Name)
		}
		if !arrow.TypeEqual(rf.Type, af.Type) {
			return fmt.Errorf("plugin record: field %q is %s, announced %s — schema drifted mid-stream (contract §8)", rf.Name, rf.Type, af.Type)
		}
	}
	return nil
}

package iceberg

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/codec"
	"github.com/apache/iceberg-go/table"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/sink"
)

// TableWriter is the Iceberg staging writer: the worker writes parquet and
// hands the coordinator an opaque descriptor, which commits the cycle
// (WK-001 C5).
var _ sink.StagingWriter = (*TableWriter)(nil)

// stagedMagic frames a WriteStaged descriptor (WK-001 C5). A leading byte so
// a malformed or foreign payload is rejected before decoding.
const stagedMagic = 0x57 // 'W'

// stagedPayload is the decoded form of a WriteStaged descriptor: the delete
// files and the data files one delivery produced. The coordinator aggregates
// the deletes of a whole cycle into one delete commit and the appends into
// one append commit (WK-001 C5).
type stagedPayload struct {
	deletes []iceberg.DataFile
	appends []iceberg.DataFile
}

// WriteStaged writes the batch's data files WITHOUT committing them and
// returns an opaque descriptor for CommitStaged. It is the Flink
// IcebergStreamWriter half of the model: the worker owns parquet writing,
// the coordinator owns the single catalog commit per cycle.
//
// The upsert path stages equality-delete files (a transaction that is never
// committed) plus the data files; the append path stages data files only.
// Nothing is visible in the table until CommitStaged. A worker that dies
// after writing leaves orphan parquet files, never referenced — the Flink
// behavior, not corruption.
func (w *TableWriter) WriteStaged(ctx context.Context, b *dataplane.Batch) ([]byte, error) {
	tbl, err := w.cat.LoadTable(ctx, w.ident)
	if err != nil {
		return nil, fmt.Errorf("iceberg: load %v: %w", w.ident, err)
	}
	spec, schema, version := tbl.Spec(), tbl.Schema(), tbl.Metadata().Version()

	upsertBatch, deleteBatch, err := splitByOp(ctx, b)
	if err != nil {
		return nil, err
	}
	if upsertBatch != nil {
		defer upsertBatch.Release()
	}
	if deleteBatch != nil {
		defer deleteBatch.Release()
	}

	var payload stagedPayload
	if b.Mode == dataplane.UpsertMode {
		keys, err := extractKeys([]*dataplane.Batch{upsertBatch, deleteBatch}, w.delCols)
		if err != nil {
			return nil, err
		}
		if len(keys) > 0 {
			rec, err := w.deleteRecord(keys)
			if err != nil {
				return nil, err
			}
			defer rec.Release()
			// WriteEqualityDeletes writes the delete files and stages them on
			// the transaction; we never commit it — CommitStaged does.
			files, err := tbl.NewTransaction().WriteEqualityDeletes(ctx, w.eqIDs, oneBatch(rec))
			if err != nil {
				return nil, fmt.Errorf("iceberg: stage deletes %v: %w", w.ident, err)
			}
			payload.deletes = files
		}
	}
	if upsertBatch != nil && upsertBatch.Record.NumRows() > 0 {
		rec, err := w.projectRecord(ctx, upsertBatch)
		if err != nil {
			return nil, err
		}
		defer rec.Release()
		files, err := w.writeDataFiles(ctx, tbl, rec)
		if err != nil {
			return nil, err
		}
		payload.appends = files
	}
	return encodeStaged(payload, spec, schema, version)
}

// writeDataFiles writes one projected record to parquet through the upstream
// WriteRecords — the same path AppendTable uses, so file location, rolling by
// target size, partition fanout and per-column stats are all honored — and
// returns the resulting DataFiles without committing.
func (w *TableWriter) writeDataFiles(ctx context.Context, tbl *table.Table, rec arrow.RecordBatch) ([]iceberg.DataFile, error) {
	var files []iceberg.DataFile
	for df, err := range table.WriteRecords(ctx, tbl, rec.Schema(), oneBatch(rec)) {
		if err != nil {
			return nil, fmt.Errorf("iceberg: write records %v: %w", w.ident, err)
		}
		files = append(files, df)
	}
	return files, nil
}

// encodeStaged frames a payload as its opaque descriptor. The partition
// spec, schema and format version are re-supplied by the decoder from the
// table (they belong to the table, not the descriptor).
func encodeStaged(p stagedPayload, spec iceberg.PartitionSpec, schema *iceberg.Schema, version int) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte(stagedMagic)
	if err := writeFileList(&buf, p.deletes, spec, schema, version); err != nil {
		return nil, err
	}
	if err := writeFileList(&buf, p.appends, spec, schema, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// writeFileList writes a length-prefixed list of DataFiles, each encoded by
// the upstream DataFile codec.
func writeFileList(buf *bytes.Buffer, files []iceberg.DataFile, spec iceberg.PartitionSpec, schema *iceberg.Schema, version int) error {
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(files)))
	buf.Write(n[:])
	for _, df := range files {
		b, err := codec.EncodeDataFile(df, spec, schema, version)
		if err != nil {
			return fmt.Errorf("iceberg: encode data file: %w", err)
		}
		binary.BigEndian.PutUint32(n[:], uint32(len(b)))
		buf.Write(n[:])
		buf.Write(b)
	}
	return nil
}

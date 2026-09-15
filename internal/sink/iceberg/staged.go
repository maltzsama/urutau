package iceberg

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/codec"
	"github.com/apache/iceberg-go/table"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/snapshot"
	"github.com/maltzsama/urutau/sink"
)

// TableWriter is the Iceberg staging writer: the worker writes parquet and
// hands the coordinator an opaque descriptor, which commits the cycle
// (WK-001 C5).
var _ sink.StagingWriter = (*TableWriter)(nil)

// Sink is the Iceberg staged committer: the coordinator hands it a cycle's
// descriptors and it commits them as one unit (WK-001 C5).
var _ sink.StagedCommitter = (*Sink)(nil)

// stagedMagic frames a WriteStaged descriptor (WK-001 C5). A leading byte so
// a malformed or foreign payload is rejected before decoding.
const stagedMagic = 0x57 // 'W'

// stagedPayload is the decoded form of a WriteStaged descriptor: the delete
// files and the data files one delivery produced, plus the snapshot state
// the batch carried. The coordinator aggregates the deletes of a whole cycle
// into one delete commit and the appends into one append commit (WK-001 C5).
type stagedPayload struct {
	deletes         []iceberg.DataFile
	appends         []iceberg.DataFile
	snapshotState   string
	snapshotPending []uint32
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

	payload := stagedPayload{snapshotState: b.SnapshotState, snapshotPending: b.SnapshotPending}
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

// CommitStaged commits one cycle's descriptors as a single unit: every delete
// file of the cycle in one commit, then every data file in the next, with the
// cycle's position on the LAST commit (WK-001 §4.2 invariants 3 and 4). The
// delete-before-append order is mandatory: staged together, iceberg-go gives
// the delete the higher sequence and it would erase the fresh rows.
//
// The snapshot state travels in the descriptors; the last non-empty one wins
// (all deliveries of a binlog batch share it).
func (s *Sink) CommitStaged(ctx context.Context, ref core.TableRef, staged [][]byte, pos string) error {
	ident := s.ident(ref.Target)
	tbl, err := s.cat.LoadTable(ctx, ident)
	if err != nil {
		return fmt.Errorf("iceberg: load %v: %w", ident, err)
	}
	spec, schema, version := tbl.Spec(), tbl.Schema(), tbl.Metadata().Version()

	var (
		deletes   []iceberg.DataFile
		appends   []iceberg.DataFile
		snapState string
		snapPend  []uint32
	)
	for _, d := range staged {
		p, err := decodeStaged(d, spec, schema, version)
		if err != nil {
			return fmt.Errorf("iceberg: staged %v: %w", ident, err)
		}
		deletes = append(deletes, p.deletes...)
		appends = append(appends, p.appends...)
		if p.snapshotState != "" {
			snapState = p.snapshotState
		}
		if p.snapshotPending != nil {
			snapPend = p.snapshotPending
		}
	}
	// Position on the last commit only: the append when there is one, else
	// the delete (a delete-only cycle).
	delPos := ""
	if len(appends) == 0 {
		delPos = pos
	}
	if len(deletes) > 0 {
		if err := s.commitStagedDeletes(ctx, ident, deletes, delPos, snapState, snapPend); err != nil {
			return err
		}
	}
	if len(appends) > 0 {
		if err := s.commitStagedAppends(ctx, ident, appends, pos, snapState, snapPend); err != nil {
			return err
		}
	}
	return nil
}

// commitStagedDeletes adds pre-built equality-delete files to the table in
// one snapshot, retrying the catalog on a retryable error.
func (s *Sink) commitStagedDeletes(ctx context.Context, ident table.Identifier, files []iceberg.DataFile, pos, snapshotState string, snapshotPending []uint32) error {
	p := props(pos)
	addSnapshotProps(p, snapshotState, snapshotPending)

	var lastErr error
	for attempt := 0; attempt < maxCommitTries; attempt++ {
		if attempt > 0 {
			if err := sleepCtx(ctx, backoffDuration(stagedBackoff, attempt)); err != nil {
				return err
			}
		}
		tbl, err := s.cat.LoadTable(ctx, ident)
		if err != nil {
			if !isRetryableError(err) {
				return err
			}
			lastErr = err
			continue
		}
		txn := tbl.NewTransaction()
		if err := txn.NewRowDelta(p).AddDeletes(files...).Commit(ctx); err != nil {
			if !isRetryableError(err) {
				return err
			}
			lastErr = err
			continue
		}
		if pos != "" {
			if err := txn.SetProperties(p); err != nil {
				return err
			}
		}
		if _, err := txn.Commit(ctx); err != nil {
			if !isRetryableError(err) {
				return err
			}
			lastErr = err
			continue
		}
		return nil
	}
	return fmt.Errorf("%w: staged delete commit on %v: %v", ErrCommitExhausted, ident, lastErr)
}

// commitStagedAppends adds pre-built data files to the table in one snapshot,
// retrying the catalog on a retryable error.
func (s *Sink) commitStagedAppends(ctx context.Context, ident table.Identifier, files []iceberg.DataFile, pos, snapshotState string, snapshotPending []uint32) error {
	p := props(pos)
	addSnapshotProps(p, snapshotState, snapshotPending)

	var lastErr error
	for attempt := 0; attempt < maxCommitTries; attempt++ {
		if attempt > 0 {
			if err := sleepCtx(ctx, backoffDuration(stagedBackoff, attempt)); err != nil {
				return err
			}
		}
		tbl, err := s.cat.LoadTable(ctx, ident)
		if err != nil {
			if !isRetryableError(err) {
				return err
			}
			lastErr = err
			continue
		}
		txn := tbl.NewTransaction()
		if err := txn.AddDataFiles(ctx, files, p); err != nil {
			if !isRetryableError(err) {
				return err
			}
			lastErr = err
			continue
		}
		if err := txn.SetProperties(p); err != nil {
			return err
		}
		if _, err := txn.Commit(ctx); err != nil {
			if !isRetryableError(err) {
				return err
			}
			lastErr = err
			continue
		}
		return nil
	}
	return fmt.Errorf("%w: staged append commit on %v: %v", ErrCommitExhausted, ident, lastErr)
}

// maxCommitTries and stagedBackoff mirror the TableWriter's retry policy for
// the staged commit path (the writer's fields are per-writer; the sink's
// committer has none).
const maxCommitTries = 5

var stagedBackoff = 200 * time.Millisecond

// addSnapshotProps merges the resumable-backfill state into the commit
// properties, matching the TableWriter's own commit paths.
func addSnapshotProps(p iceberg.Properties, state string, pending []uint32) {
	if state != "" {
		p["cdc.snapshot.state"] = state
	}
	if pending != nil {
		p["cdc.snapshot.pending"] = snapshot.EncodePending(pending)
	}
}

// encodeStaged frames a payload as its opaque descriptor. The partition
// spec, schema and format version are re-supplied by the decoder from the
// table (they belong to the table, not the descriptor).
func encodeStaged(p stagedPayload, spec iceberg.PartitionSpec, schema *iceberg.Schema, version int) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte(stagedMagic)
	writeString(&buf, p.snapshotState)
	writeUint32List(&buf, p.snapshotPending)
	if err := writeFileList(&buf, p.deletes, spec, schema, version); err != nil {
		return nil, err
	}
	if err := writeFileList(&buf, p.appends, spec, schema, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// decodeStaged is the inverse of encodeStaged; spec, schema and version must
// be the table's, matching the encoder.
func decodeStaged(data []byte, spec iceberg.PartitionSpec, schema *iceberg.Schema, version int) (stagedPayload, error) {
	if len(data) < 1 || data[0] != stagedMagic {
		return stagedPayload{}, fmt.Errorf("staged descriptor: bad magic")
	}
	r := bytes.NewReader(data[1:])
	state, err := readString(r)
	if err != nil {
		return stagedPayload{}, err
	}
	pending, err := readUint32List(r)
	if err != nil {
		return stagedPayload{}, err
	}
	deletes, err := readFileList(r, spec, schema, version)
	if err != nil {
		return stagedPayload{}, err
	}
	appends, err := readFileList(r, spec, schema, version)
	if err != nil {
		return stagedPayload{}, err
	}
	return stagedPayload{deletes: deletes, appends: appends, snapshotState: state, snapshotPending: pending}, nil
}

func writeString(buf *bytes.Buffer, s string) {
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(s)))
	buf.Write(n[:])
	buf.WriteString(s)
}

func readString(r *bytes.Reader) (string, error) {
	var n [4]byte
	if _, err := io.ReadFull(r, n[:]); err != nil {
		return "", fmt.Errorf("staged descriptor: %w", err)
	}
	b := make([]byte, binary.BigEndian.Uint32(n[:]))
	if _, err := io.ReadFull(r, b); err != nil {
		return "", fmt.Errorf("staged descriptor: %w", err)
	}
	return string(b), nil
}

func writeUint32List(buf *bytes.Buffer, xs []uint32) {
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(xs)))
	buf.Write(n[:])
	for _, x := range xs {
		binary.BigEndian.PutUint32(n[:], x)
		buf.Write(n[:])
	}
}

func readUint32List(r *bytes.Reader) ([]uint32, error) {
	var n [4]byte
	if _, err := io.ReadFull(r, n[:]); err != nil {
		return nil, fmt.Errorf("staged descriptor: %w", err)
	}
	count := binary.BigEndian.Uint32(n[:])
	xs := make([]uint32, 0, count)
	for i := uint32(0); i < count; i++ {
		if _, err := io.ReadFull(r, n[:]); err != nil {
			return nil, fmt.Errorf("staged descriptor: %w", err)
		}
		xs = append(xs, binary.BigEndian.Uint32(n[:]))
	}
	return xs, nil
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

// readFileList is the inverse of writeFileList.
func readFileList(r *bytes.Reader, spec iceberg.PartitionSpec, schema *iceberg.Schema, version int) ([]iceberg.DataFile, error) {
	var n [4]byte
	if _, err := io.ReadFull(r, n[:]); err != nil {
		return nil, fmt.Errorf("staged descriptor: %w", err)
	}
	count := binary.BigEndian.Uint32(n[:])
	files := make([]iceberg.DataFile, 0, count)
	for i := uint32(0); i < count; i++ {
		if _, err := io.ReadFull(r, n[:]); err != nil {
			return nil, fmt.Errorf("staged descriptor: %w", err)
		}
		b := make([]byte, binary.BigEndian.Uint32(n[:]))
		if _, err := io.ReadFull(r, b); err != nil {
			return nil, fmt.Errorf("staged descriptor: %w", err)
		}
		df, err := codec.DecodeDataFile(b, spec, schema, version)
		if err != nil {
			return nil, fmt.Errorf("iceberg: decode data file: %w", err)
		}
		files = append(files, df)
	}
	return files, nil
}

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
	opts := []table.WriteRecordOption{}
	if w.targetFileSize > 0 {
		opts = append(opts, table.WithTargetFileSize(w.targetFileSize))
	}
	for df, err := range table.WriteRecords(ctx, tbl, rec.Schema(), oneBatch(rec), opts...) {
		if err != nil {
			return nil, fmt.Errorf("iceberg: write records %v: %w", w.ident, err)
		}
		files = append(files, df)
	}
	return files, nil
}

// CommitStaged commits one cycle's descriptors as a single unit: every delete
// file and every data file of the cycle land in ONE RowDelta, with the cycle's
// position on that same commit (WK-001 §4.2 invariants 3 and 4). One RowDelta
// is the atomic unit — a crash between a separate delete commit and append
// commit would leave the deletes visible without their fresh rows. Within the
// snapshot every file shares the snapshot's sequence, so the equality delete
// (sequence S) applies only to rows written before it (sequence < S) and never
// erases the rows committed with it.
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
	if len(deletes) == 0 && len(appends) == 0 {
		// The cycle carried no data (e.g. an append-mode delete dropped by
		// the worker): still record the position so the resume advances past
		// it, instead of re-reading the same event on every boot.
		if pos == "" && snapState == "" && snapPend == nil {
			return nil
		}
		return s.commitStagedProps(ctx, ident, pos, snapState, snapPend)
	}
	return s.commitStaged(ctx, ident, deletes, appends, pos, snapState, snapPend)
}

// commitStagedProps records a cycle's position and snapshot state when the
// cycle carried no data files, retrying the catalog on a retryable error.
func (s *Sink) commitStagedProps(ctx context.Context, ident table.Identifier, pos, snapshotState string, snapshotPending []uint32) error {
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
	return fmt.Errorf("%w: staged property commit on %v: %w", ErrCommitExhausted, ident, lastErr)
}

// commitStaged commits a cycle's delete AND data files as ONE atomic snapshot
// with the cycle's position, retrying the catalog on a retryable error. One
// RowDelta carrying both kinds is the atomic unit: a crash between a separate
// delete commit and append commit would leave the deletes visible without
// their fresh rows. Within the snapshot every file shares the snapshot's
// sequence, so the equality delete (sequence S) applies only to rows written
// before it (sequence < S) and never erases the rows committed with it.
func (s *Sink) commitStaged(ctx context.Context, ident table.Identifier, deletes, appends []iceberg.DataFile, pos, snapshotState string, snapshotPending []uint32) error {
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
		rd := txn.NewRowDelta(p)
		if len(deletes) > 0 {
			rd.AddDeletes(deletes...)
		}
		if len(appends) > 0 {
			rd.AddRows(appends...)
		}
		if err := rd.Commit(ctx); err != nil {
			if !isRetryableError(err) {
				return err
			}
			lastErr = err
			continue
		}
		// The position is also a table property (the fast read path); it is
		// already in the snapshot summary, so losing this commit only costs
		// the walk-back.
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
	return fmt.Errorf("%w: staged commit on %v: %w", ErrCommitExhausted, ident, lastErr)
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

// encodeStaged frames a payload as its opaque descriptor: a magic byte, a
// fingerprint of the partition spec and schema the files were encoded against,
// the snapshot state (string + pending list) and two length-prefixed file
// lists (deletes, appends).
func encodeStaged(p stagedPayload, spec iceberg.PartitionSpec, schema *iceberg.Schema, version int) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte(stagedMagic)
	// The decoder re-supplies the table's CURRENT spec and schema; a change
	// between staging and commit makes iceberg-go's codec silently mis-type
	// partition values, so fingerprint both and reject a mismatch loudly
	// (issue #124).
	writeUint32(&buf, uint32(spec.ID()))
	writeString(&buf, schemaString(schema))
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

// schemaString renders a schema for the descriptor fingerprint. A nil schema
// (tests, or an unpartitioned edge) is the empty string.
func schemaString(schema *iceberg.Schema) string {
	if schema == nil {
		return ""
	}
	return schema.String()
}

// decodeStaged is the inverse of encodeStaged; spec, schema and version must
// be the table's, matching the encoder.
func decodeStaged(data []byte, spec iceberg.PartitionSpec, schema *iceberg.Schema, version int) (stagedPayload, error) {
	if len(data) < 1 || data[0] != stagedMagic {
		return stagedPayload{}, fmt.Errorf("staged descriptor: bad magic")
	}
	r := bytes.NewReader(data[1:])
	specID, err := readUint32(r)
	if err != nil {
		return stagedPayload{}, err
	}
	if int(specID) != spec.ID() {
		return stagedPayload{}, fmt.Errorf("staged descriptor: partition spec changed since staging (staged %d, table %d)", specID, spec.ID())
	}
	encSchema, err := readString(r)
	if err != nil {
		return stagedPayload{}, err
	}
	if encSchema != schemaString(schema) {
		return stagedPayload{}, fmt.Errorf("staged descriptor: table schema changed since staging")
	}
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

func writeUint32(buf *bytes.Buffer, x uint32) {
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], x)
	buf.Write(n[:])
}

func readUint32(r *bytes.Reader) (uint32, error) {
	var n [4]byte
	if _, err := io.ReadFull(r, n[:]); err != nil {
		return 0, fmt.Errorf("staged descriptor: %w", err)
	}
	return binary.BigEndian.Uint32(n[:]), nil
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
	size := int(binary.BigEndian.Uint32(n[:]))
	if size > r.Len() {
		return "", fmt.Errorf("staged descriptor: string length %d exceeds %d remaining bytes", size, r.Len())
	}
	b := make([]byte, size)
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
	count := int(binary.BigEndian.Uint32(n[:]))
	// Each entry is 4 bytes: a count that cannot fit in what remains is
	// corrupt, and must not drive an allocation.
	if count > r.Len()/4 {
		return nil, fmt.Errorf("staged descriptor: list count %d exceeds %d remaining bytes", count, r.Len())
	}
	xs := make([]uint32, 0, count)
	for i := 0; i < count; i++ {
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
	count := int(binary.BigEndian.Uint32(n[:]))
	// Each entry carries at least its 4-byte length: reject a count that
	// cannot fit before allocating.
	if count > r.Len()/4 {
		return nil, fmt.Errorf("staged descriptor: file count %d exceeds %d remaining bytes", count, r.Len())
	}
	files := make([]iceberg.DataFile, 0, count)
	for i := 0; i < count; i++ {
		if _, err := io.ReadFull(r, n[:]); err != nil {
			return nil, fmt.Errorf("staged descriptor: %w", err)
		}
		size := int(binary.BigEndian.Uint32(n[:]))
		if size > r.Len() {
			return nil, fmt.Errorf("staged descriptor: data file %d length %d exceeds %d remaining bytes", i, size, r.Len())
		}
		b := make([]byte, size)
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

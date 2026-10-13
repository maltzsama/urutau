package iceberg

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/dv"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

// Positional deletes. In this mode the writer removes an old row version by
// marking its position in a deletion vector instead of writing an equality
// delete on its key. A reader then drops the row by position, with no join
// against delete files.
//
// There is no key → position index. The positions are resolved at commit
// time, against the table the attempt loaded: the data files whose key range
// can hold one of the batch's keys are read, key columns only, and every row
// whose key is in the batch is marked. A data file carries at most one
// deletion vector, so a file that already has one gets a merged replacement
// and the old one is removed in the same snapshot.

// positionScanRows is how many rows of key columns one read holds while a
// data file is matched against the batch's keys.
const positionScanRows = 64 << 10

// vectorDelta is the deletion vectors one commit adds and the ones they
// supersede.
type vectorDelta struct {
	add, remove []iceberg.DataFile
}

// stageRowDelta stages the commit's row-level change on txn: the rows it adds
// and the removal of the old versions of keys, as the equality delete files
// already written or, in positional mode, as deletion vectors resolved against
// tbl. It stages nothing when there is nothing to add or remove.
func (w *TableWriter) stageRowDelta(ctx context.Context, tbl *table.Table, txn *table.Transaction, p iceberg.Properties, keys [][]any, equality, rows []iceberg.DataFile) error {
	deletes := equality
	var superseded []iceberg.DataFile
	if w.positional && len(keys) > 0 {
		vd, err := w.deletionVectors(ctx, tbl, keys)
		if err != nil {
			return err
		}
		deletes, superseded = vd.add, vd.remove
	}
	if len(deletes) == 0 && len(rows) == 0 {
		return nil
	}
	rd := txn.NewRowDelta(p)
	if len(deletes) > 0 {
		rd.AddDeletes(deletes...)
	}
	if len(superseded) > 0 {
		rd.RemoveDeletes(superseded...)
	}
	if len(rows) > 0 {
		rd.AddRows(rows...)
	}
	return rd.Commit(ctx)
}

// deletionVectors resolves keys to row positions in tbl's current data files
// and writes the deletion vectors that mark them.
func (w *TableWriter) deletionVectors(ctx context.Context, tbl *table.Table, keys [][]any) (vectorDelta, error) {
	if v := tbl.Metadata().Version(); v < formatVersion {
		return vectorDelta{}, fmt.Errorf("iceberg: %v: positional deletes need format-version %d, the table is %d", w.ident, formatVersion, v)
	}
	want, within, err := w.keySet(keys)
	if err != nil {
		return vectorDelta{}, err
	}
	// The plan keeps only the data files whose statistics admit a key of the
	// batch; the rest are never opened.
	started := time.Now()
	tasks, err := tbl.Scan(table.WithRowFilter(within)).PlanFiles(ctx)
	if err != nil {
		return vectorDelta{}, fmt.Errorf("iceberg: %v: plan files: %w", w.ident, err)
	}
	planned := time.Now()
	fs, err := tbl.FS(ctx)
	if err != nil {
		return vectorDelta{}, fmt.Errorf("iceberg: %v: file io: %w", w.ident, err)
	}
	wfs, ok := fs.(iceio.WriteFileIO)
	if !ok {
		return vectorDelta{}, fmt.Errorf("iceberg: %v: the table's file io cannot write", w.ident)
	}
	meta := tbl.Metadata()
	vectors := dv.NewDVWriter(wfs, func(id int32) *iceberg.PartitionSpec { return meta.PartitionSpecByID(int(id)) })

	// One entry per data file: a large file is planned as several splits.
	seen := make(map[string]struct{}, len(tasks))
	files := tasks[:0:0]
	for _, task := range tasks {
		path := task.File.FilePath()
		if _, dup := seen[path]; dup {
			continue
		}
		seen[path] = struct{}{}
		files = append(files, task)
	}
	w.filesMatched += len(files)
	// The files are read side by side: a read is a round trip to the object
	// store, and one after another they were the whole cost of a commit
	// (#864). want is only read from here on.
	found := make([][]int64, len(files))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(positionReaders)
	for i, task := range files {
		g.Go(func() error {
			hits, err := w.matchingPositions(gctx, fs, task.File, want)
			found[i] = hits
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return vectorDelta{}, err
	}
	read := time.Now()

	// A data file that already has a deletion vector gets a merged one, so
	// the old vector is read first. One Puffin file holds the vectors of a
	// whole commit: they are read file by file, each file's footer decoded
	// once for all the vectors wanted from it, and the files side by side.
	// Read one vector at a time, the footers alone were most of a commit
	// (#864).
	old := make([]*dv.RoaringPositionBitmap, len(files))
	byPuffin := map[string][]int{} // Puffin path → indexes into files
	for i, task := range files {
		if len(found[i]) == 0 || len(task.DeletionVectorFiles) == 0 {
			continue
		}
		path := task.DeletionVectorFiles[0].FilePath()
		byPuffin[path] = append(byPuffin[path], i)
	}
	g, _ = errgroup.WithContext(ctx)
	g.SetLimit(positionReaders)
	for path, idx := range byPuffin {
		g.Go(func() error {
			wanted := make([]iceberg.DataFile, len(idx))
			for j, i := range idx {
				wanted[j] = files[i].DeletionVectorFiles[0]
			}
			marked, err := dv.ReadDVs(fs, wanted)
			if err != nil {
				return fmt.Errorf("iceberg: %v: read the deletion vectors in %s: %w", w.ident, path, err)
			}
			for j, i := range idx {
				old[i] = marked[j]
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return vectorDelta{}, err
	}
	merged := time.Now()

	var out vectorDelta
	vectored := 0 // data files this commit writes a deletion vector for
	for i, task := range files {
		path, hits := task.File.FilePath(), found[i]
		if len(hits) == 0 {
			continue
		}
		specID, partition := int32(task.File.SpecID()), task.File.Partition()
		if marked := old[i]; marked != nil {
			hits = unmarked(hits, marked)
			if len(hits) == 0 {
				continue // every match is a row an earlier commit already removed
			}
			vectors.Load(path, marked, specID, partition)
			out.remove = append(out.remove, task.DeletionVectorFiles...)
		}
		if err := vectors.Add(path, hits, specID, partition); err != nil {
			return vectorDelta{}, fmt.Errorf("iceberg: %v: mark positions in %s: %w", w.ident, path, err)
		}
		vectored++
	}

	locations, err := table.LoadLocationProvider(tbl.Location(), tbl.Properties())
	if err != nil {
		return vectorDelta{}, fmt.Errorf("iceberg: %v: location provider: %w", w.ident, err)
	}
	out.add, err = vectors.Flush(ctx, locations.NewDataLocation(uuid.NewString()+"-deletes.puffin"))
	if err != nil {
		return vectorDelta{}, fmt.Errorf("iceberg: %v: write deletion vectors: %w", w.ident, err)
	}
	// What resolving positions cost this commit: the data files the plan
	// could not rule out were each read for their key columns.
	// and where the time went: planning the scan, reading the files, reading
	// the vectors they already had, and writing the new ones.
	slog.Info("iceberg: positions resolved", "table", w.ident, "keys", len(keys),
		"files_read", len(seen), "files_marked", vectored, "vectors_replaced", len(out.remove),
		"plan_ms", planned.Sub(started).Milliseconds(), "read_ms", read.Sub(planned).Milliseconds(),
		"old_vectors_ms", merged.Sub(read).Milliseconds(), "write_ms", time.Since(merged).Milliseconds())
	return out, nil
}

// positionReaders is how many data files a commit reads at once to resolve
// positions. Each reader holds one file's key columns, a batch at a time.
const positionReaders = 8

// unmarked returns the positions not yet set in marked.
func unmarked(positions []int64, marked *dv.RoaringPositionBitmap) []int64 {
	fresh := positions[:0]
	for _, p := range positions {
		if !marked.Contains(uint64(p)) {
			fresh = append(fresh, p)
		}
	}
	return fresh
}

// keySet encodes the batch's keys, typed as the table's key columns, for
// lookup while a data file is read. It also returns a filter over the key
// columns, which the scan plan uses to skip the partitions and the data files
// that cannot hold any of the keys.
func (w *TableWriter) keySet(keys [][]any) (map[string]struct{}, iceberg.BooleanExpression, error) {
	rec, err := w.deleteRecord(keys)
	if err != nil {
		return nil, nil, err
	}
	defer rec.Release()
	set := make(map[string]struct{}, rec.NumRows())
	var buf []byte
	for row := range int(rec.NumRows()) {
		buf, err = appendRowKey(buf[:0], rec.Columns(), row)
		if err != nil {
			return nil, nil, fmt.Errorf("iceberg: %v: key: %w", w.ident, err)
		}
		set[string(buf)] = struct{}{}
	}
	// Every key column narrows the plan: a file is read only when each of
	// them admits one of the batch's values.
	within := iceberg.BooleanExpression(iceberg.AlwaysTrue{})
	for i, name := range w.delCols {
		within = iceberg.NewAnd(within, keyFilter(name, rec.Column(i)))
	}
	return set, within, nil
}

// keyInMax is the most distinct values of a key column a plan filter lists
// one by one. Up to it the filter is "name in (values)", which rules out a
// data file or a partition that holds none of them even when it lies between
// two that do; past it the filter falls back to the values' range, as a list
// that long costs more to evaluate against every file than it saves.
const keyInMax = 200

// keyFilter is the plan filter of one key column: its distinct values when
// they are few, their range otherwise.
func keyFilter(name string, col arrow.Array) iceberg.BooleanExpression {
	if col.Len() == 0 || col.NullN() > 0 {
		return iceberg.AlwaysTrue{}
	}
	ref := iceberg.Reference(name)
	switch c := col.(type) {
	case *array.Int64:
		if vals, ok := distinctUpTo(c.Len(), c.Value, keyInMax); ok {
			return iceberg.IsIn(ref, vals...)
		}
	case *array.Int32:
		if vals, ok := distinctUpTo(c.Len(), c.Value, keyInMax); ok {
			return iceberg.IsIn(ref, vals...)
		}
	case *array.String:
		if vals, ok := distinctUpTo(c.Len(), c.Value, keyInMax); ok {
			return iceberg.IsIn(ref, vals...)
		}
	}
	return keyRange(name, col)
}

// distinctUpTo returns the distinct values of a column of n rows, or false
// when there are more than limit.
func distinctUpTo[T comparable](n int, value func(int) T, limit int) ([]T, bool) {
	seen := make(map[T]struct{}, min(n, limit))
	out := make([]T, 0, min(n, limit))
	for i := range n {
		v := value(i)
		if _, dup := seen[v]; dup {
			continue
		}
		if len(out) == limit {
			return nil, false
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out, true
}

// keyRange is "name between the column's smallest and largest value" for the
// key types whose order the file statistics share, and "everything" for the
// others: a filter that is too wide only costs a read, one that is too narrow
// would leave an old row version alive.
func keyRange(name string, col arrow.Array) iceberg.BooleanExpression {
	if col.Len() == 0 || col.NullN() > 0 {
		return iceberg.AlwaysTrue{}
	}
	ref := iceberg.Reference(name)
	switch c := col.(type) {
	case *array.Int64:
		lo, hi := c.Value(0), c.Value(0)
		for i := 1; i < c.Len(); i++ {
			lo, hi = min(lo, c.Value(i)), max(hi, c.Value(i))
		}
		return iceberg.NewAnd(iceberg.GreaterThanEqual(ref, lo), iceberg.LessThanEqual(ref, hi))
	case *array.Int32:
		lo, hi := c.Value(0), c.Value(0)
		for i := 1; i < c.Len(); i++ {
			lo, hi = min(lo, c.Value(i)), max(hi, c.Value(i))
		}
		return iceberg.NewAnd(iceberg.GreaterThanEqual(ref, lo), iceberg.LessThanEqual(ref, hi))
	case *array.String:
		lo, hi := c.Value(0), c.Value(0)
		for i := 1; i < c.Len(); i++ {
			lo, hi = min(lo, c.Value(i)), max(hi, c.Value(i))
		}
		return iceberg.NewAnd(iceberg.GreaterThanEqual(ref, lo), iceberg.LessThanEqual(ref, hi))
	}
	return iceberg.AlwaysTrue{}
}

// matchingPositions reads the key columns of one data file, in file order, and
// returns the position of every row whose key is in want. Positions are
// physical: rows an existing delete already removed are still counted.
func (w *TableWriter) matchingPositions(ctx context.Context, fs iceio.IO, df iceberg.DataFile, want map[string]struct{}) ([]int64, error) {
	if df.FileFormat() != iceberg.ParquetFile {
		return nil, fmt.Errorf("iceberg: %v: positional deletes read Parquet data files, %s is %s", w.ident, df.FilePath(), df.FileFormat())
	}
	f, err := fs.Open(df.FilePath())
	if err != nil {
		return nil, fmt.Errorf("iceberg: %v: open %s: %w", w.ident, df.FilePath(), err)
	}
	defer func() { _ = f.Close() }()
	pq, err := file.NewParquetReader(f)
	if err != nil {
		return nil, fmt.Errorf("iceberg: %v: read %s: %w", w.ident, df.FilePath(), err)
	}
	defer func() { _ = pq.Close() }()

	leaves, names, err := keyLeaves(pq, w.eqIDs)
	if err != nil {
		return nil, fmt.Errorf("iceberg: %v: %s: %w", w.ident, df.FilePath(), err)
	}
	fr, err := pqarrow.NewFileReader(pq, pqarrow.ArrowReadProperties{BatchSize: positionScanRows}, memory.DefaultAllocator)
	if err != nil {
		return nil, fmt.Errorf("iceberg: %v: read %s: %w", w.ident, df.FilePath(), err)
	}
	rr, err := fr.GetRecordReader(ctx, leaves, nil)
	if err != nil {
		return nil, fmt.Errorf("iceberg: %v: read %s: %w", w.ident, df.FilePath(), err)
	}
	defer rr.Release()

	var (
		hits []int64
		buf  []byte
		pos  int64
		cols = make([]arrow.Array, len(names))
	)
	for rr.Next() {
		rec := rr.RecordBatch()
		for i, name := range names {
			idx := rec.Schema().FieldIndices(name)
			if len(idx) != 1 {
				return nil, fmt.Errorf("iceberg: %v: %s: key column %q is not a single column of the file", w.ident, df.FilePath(), name)
			}
			cols[i] = rec.Column(idx[0])
		}
		for row := range int(rec.NumRows()) {
			buf, err = appendRowKey(buf[:0], cols, row)
			if err != nil {
				return nil, fmt.Errorf("iceberg: %v: %s: key: %w", w.ident, df.FilePath(), err)
			}
			if _, ok := want[string(buf)]; ok {
				hits = append(hits, pos)
			}
			pos++
		}
	}
	if err := rr.Err(); err != nil {
		return nil, fmt.Errorf("iceberg: %v: read %s: %w", w.ident, df.FilePath(), err)
	}
	return hits, nil
}

// keyLeaves finds the file's leaf columns for the key field ids, in key order.
// The lookup is by field id, so a renamed column still resolves.
func keyLeaves(pq *file.Reader, fieldIDs []int) (leaves []int, names []string, err error) {
	sc := pq.MetaData().Schema
	byID := make(map[int32]int, sc.NumColumns())
	for i := range sc.NumColumns() {
		byID[sc.Column(i).SchemaNode().FieldID()] = i
	}
	for _, id := range fieldIDs {
		i, ok := byID[int32(id)]
		if !ok {
			return nil, nil, fmt.Errorf("key field id %d is not in the file", id)
		}
		leaves = append(leaves, i)
		names = append(names, sc.Column(i).Name())
	}
	return leaves, names, nil
}

// appendRowKey appends one row's key tuple to buf in a form that is equal for
// equal keys whether the columns come from the batch or from a data file:
// every integer width as eight bytes, every string or binary flavour as
// length-prefixed bytes. A null or an unsupported key type is an error —
// guessing would mark the wrong rows.
func appendRowKey(buf []byte, cols []arrow.Array, row int) ([]byte, error) {
	for _, col := range cols {
		if col.IsNull(row) {
			return nil, fmt.Errorf("null in key column of type %s", col.DataType())
		}
		switch c := col.(type) {
		case *array.Int64:
			buf = appendKeyInt(buf, c.Value(row))
		case *array.Int32:
			buf = appendKeyInt(buf, int64(c.Value(row)))
		case *array.Int16:
			buf = appendKeyInt(buf, int64(c.Value(row)))
		case *array.Int8:
			buf = appendKeyInt(buf, int64(c.Value(row)))
		case *array.Date32:
			buf = appendKeyInt(buf, int64(c.Value(row)))
		case *array.Timestamp:
			buf = appendKeyInt(buf, int64(c.Value(row)))
		case *array.Time64:
			buf = appendKeyInt(buf, int64(c.Value(row)))
		case *array.Boolean:
			v := int64(0)
			if c.Value(row) {
				v = 1
			}
			buf = appendKeyInt(buf, v)
		case *array.String:
			buf = appendKeyBytes(buf, []byte(c.Value(row)))
		case *array.LargeString:
			buf = appendKeyBytes(buf, []byte(c.Value(row)))
		case *array.Binary:
			buf = appendKeyBytes(buf, c.Value(row))
		case *array.LargeBinary:
			buf = appendKeyBytes(buf, c.Value(row))
		case *array.FixedSizeBinary:
			buf = appendKeyBytes(buf, c.Value(row))
		case *array.Decimal128:
			v := c.Value(row)
			buf = appendKeyInt(buf, v.HighBits())
			buf = appendKeyInt(buf, int64(v.LowBits()))
		default:
			return nil, fmt.Errorf("key column of type %s is not supported by positional deletes", col.DataType())
		}
	}
	return buf, nil
}

func appendKeyInt(buf []byte, v int64) []byte {
	return binary.BigEndian.AppendUint64(append(buf, 'i'), uint64(v))
}

func appendKeyBytes(buf, v []byte) []byte {
	buf = binary.AppendUvarint(append(buf, 'b'), uint64(len(v)))
	return append(buf, v...)
}

// keyedCycle identifies a commit whose deletes are resolved per attempt: its
// delete files differ from one attempt to the next, so the identity is the
// keys it removes, the rows it adds and its position.
func (w *TableWriter) keyedCycle(keys [][]any, rows []iceberg.DataFile, pos string) (string, error) {
	h := sha256.New()
	if len(keys) > 0 {
		rec, err := w.deleteRecord(keys)
		if err != nil {
			return "", err
		}
		defer rec.Release()
		var buf []byte
		for row := range int(rec.NumRows()) {
			buf, err = appendRowKey(buf[:0], rec.Columns(), row)
			if err != nil {
				return "", fmt.Errorf("iceberg: %v: key: %w", w.ident, err)
			}
			_, _ = h.Write(buf)
			_, _ = h.Write([]byte{0})
		}
	}
	_, _ = h.Write([]byte(cycleKey(nil, rows, pos)))
	return hex.EncodeToString(h.Sum(nil)), nil
}

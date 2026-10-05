// Package source.mysql implements the MySQL replication source on top of
// go-mysql/canal: a single binlog reader that decodes row events into
// rowchange.Change, positions them at their transaction GTID, and exposes the
// synced and master positions for the DBLog watermark logic.
package mysql

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/position"
)

// TableRef is the source-agnostic table mapping (kept here as an alias for
// the package's public surface).
type TableRef = core.TableRef

// Config dials one MySQL instance. One replication connection per source —
// the invariant the whole design stands on.
type Config struct {
	Addr      string // host:port
	User      string
	Password  string
	ServerID  uint32
	Heartbeat time.Duration
	Tables    []TableRef
	Logger    *slog.Logger
	// TLSConfig, when non-nil, secures the replication connection (issue
	// #138). Nil means plaintext.
	TLSConfig *tls.Config
	// TimeLocation is the operator's temporal location, applied to decoded
	// DATETIME/TIMESTAMP/DATE values so they match the snapshot query (issue
	// #139). Nil means UTC.
	TimeLocation *time.Location
	// MaxReconnectAttempts bounds the canal reconnect budget (#182). 0 means
	// infinite retry in go-mysql, which would hide a permanently broken
	// stream; the adapter resolves the spec's default before it reaches here.
	MaxReconnectAttempts int
	// Projections is the per-source read projection (#183), keyed by
	// "db.table": the columns to emit and the compiled filter. Built by the
	// adapter from the introspected table.
	Projections map[string]projection
	// UpsertTargets marks the target tables that maintain keyed state. An
	// UPDATE that changes the primary key emits a delete of the old key only
	// for these; append targets keep the old row.
	UpsertTargets map[string]bool
	// Schemas is each target table's canonical (projected) schema, the shape
	// the direct encoder builds. The engine's SetSourceSchemas replaces
	// KindUnknown with the cast-resolved type before Start (#455).
	Schemas map[string]core.Schema
}

// Reader wraps a canal instance and decodes its row events.
type Reader struct {
	cfg   Config
	canal *canal.Canal
	// batchOut is the columnar output: the reader decodes binlog rows straight
	// into Arrow builders and emits ready wire batches (#455). There is no
	// row-change channel and no map[string]any on the live path.
	batchOut chan<- *dataplane.Batch
	bySrc    map[string]TableRef // "db.table" → ref (PK + target)
	// projections is the per-source read projection (#183): the columns to
	// emit and the compiled filter, applied on each decoded row.
	projections map[string]projection
	// encoders is the per-target-table Arrow builder, built at Start from the
	// resolved schemas. encoderOrder preserves first-appearance order so a
	// multi-table transaction emits tables deterministically.
	encoders     map[string]*tableEncoder
	encoderOrder []string
	// pending holds the records materialized so far for the transaction being
	// decoded; their positions are finalized (safe vs commit) when it ends.
	pending []heldRec
	// safePos is the position of the last fully emitted transaction (or the
	// resume point before the first). Rows are stamped with it at decode; only
	// a transaction's final record is restamped with its own position.
	safePos string
	mu      sync.Mutex
	curSet  *position.GTID // accumulated GTID set through the current transaction
	curGTID string         // curSet.String() — the position rows of this txn carry
	// curCommitTS is the transaction's commit time, captured on the GTID
	// event and stamped onto every row of the transaction (issue #137).
	curCommitTS time.Time

	// done is closed once by StartFromGTID on the way out. OnRow and the txn
	// close select on it alongside every send: without it a stalled consumer
	// would block OnRow forever on canal's own event-loop goroutine, which
	// stops canal reading binlog events and makes Close()/ctx cancellation
	// hang — only a reader on the other end can unblock a channel send. See
	// issue #114.
	done     chan struct{}
	doneOnce sync.Once

	// txnRows reports whether the transaction being decoded emitted a row
	// yet. Only canal's event goroutine reads and writes it.
	txnRows bool

	canal.DummyEventHandler // unimplemented hooks are no-ops
}

// OpenWindow is part of the SourceReader contract. The DBLog window tag is
// applied by the coordinator's gate to the batches it holds (WindowTag on the
// wire) — not by the source — so the reader keeps no window state.
func (r *Reader) OpenWindow(_ context.Context, _ uint32) {}

// ClearWindow is part of the SourceReader contract; see OpenWindow.
func (r *Reader) ClearWindow() {}

// New builds the reader but does not start it. Rows decode straight into
// Arrow and are emitted on batchOut as ready wire batches (#455).
func New(ctx context.Context, cfg Config, batchOut chan<- *dataplane.Batch) (*Reader, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	incl := make([]string, 0, len(cfg.Tables))
	bySrc := make(map[string]TableRef, len(cfg.Tables))
	for _, t := range cfg.Tables {
		incl = append(incl, fmt.Sprintf(`^%s$`, regexp.QuoteMeta(t.Source)))
		bySrc[t.Source] = t
	}

	c, err := canal.NewCanal(canalConfig(cfg, incl))
	if err != nil {
		return nil, fmt.Errorf("mysql: new canal: %w", err)
	}

	r := &Reader{
		cfg:         cfg,
		canal:       c,
		batchOut:    batchOut,
		bySrc:       bySrc,
		projections: cfg.Projections,
		encoders:    make(map[string]*tableEncoder, len(cfg.Tables)),
		done:        make(chan struct{}),
	}
	c.SetEventHandler(r)
	return r, nil
}

// SetResume records the position the stream resumes from: the safe position a
// partial piece of the first transaction carries.
func (r *Reader) SetResume(pos string) { r.safePos = pos }

// SetSourceSchemas installs the cast-resolved canonical schemas before Start.
// The direct encoder must see the resolved kinds: an unresolved KindUnknown
// column cannot be built into an Arrow type.
func (r *Reader) SetSourceSchemas(resolved map[string]core.Schema) {
	for target, cs := range resolved {
		if _, known := r.cfg.Schemas[target]; known {
			r.cfg.Schemas[target] = cs
		}
	}
}

// canalConfig renders the neutral Config into go-mysql's canal.Config — split
// out so the replication wiring (including TLS) is testable without dialing.
func canalConfig(cfg Config, includeRegex []string) *canal.Config {
	return &canal.Config{
		Addr:              cfg.Addr,
		User:              cfg.User,
		Password:          cfg.Password,
		ServerID:          cfg.ServerID,
		Flavor:            "mysql",
		HeartbeatPeriod:   cfg.Heartbeat,
		ReadTimeout:       60 * time.Second,
		IncludeTableRegex: includeRegex,
		// ParseTime makes go-mysql return time.Time for DATETIME/TIMESTAMP,
		// matching the snapshot query (parseTime=true); normalizeCol then puts
		// them in the operator's location (issue #139).
		ParseTime: true,
		Logger:    cfg.Logger,
		TLSConfig: cfg.TLSConfig,
		// A finite reconnect budget: 0 would retry a permanently broken
		// stream forever instead of surfacing the failure (#182).
		MaxReconnectAttempts: cfg.MaxReconnectAttempts,
	}
}

// stop closes done exactly once, unblocking any OnRow send in progress.
func (r *Reader) stop() {
	r.doneOnce.Do(func() { close(r.done) })
}

// StartFromGTID begins streaming from the given GTID set, blocking until
// the stream ends or the context is cancelled. Call in a goroutine.
func (r *Reader) StartFromGTID(ctx context.Context, start *position.GTID) error {
	r.cfg.Logger.Info("reader start", "from", start.String())
	r.setStart(start)

	// The position contract carries its own GTID set; convert it back to
	// go-mysql's type at this boundary — the mysql source is the only place
	// that knows go-mysql.
	raw, err := gomysql.ParseMysqlGTIDSet(start.String())
	if err != nil {
		return fmt.Errorf("mysql: convert start gtid: %w", err)
	}
	startSet, ok := raw.(*gomysql.MysqlGTIDSet)
	if !ok {
		return fmt.Errorf("mysql: unexpected gtid set type %T", raw)
	}

	// canal.StartFromGTID runs the stream to completion — it must be the
	// ONLY sync loop. A second canal.Run() would kill the first
	// connection ("kill last connection" / "Sync was closed").
	done := make(chan error, 1)
	go func() { done <- r.canal.StartFromGTID(startSet) }()

	select {
	case <-ctx.Done():
		r.stop() // unblock any OnRow send parked on r.out before Close
		r.canal.Close()
		return ctx.Err()
	case err := <-done:
		r.stop()
		// errReaderStopped surfaces here when Close() (not ctx.Done, this
		// case) triggered the shutdown while OnRow was mid-send: canal
		// propagates OnRow's error out of its own sync loop, so it arrives
		// as this call's own error rather than through ctx.Done(). It is
		// an orderly stop, not a stream failure.
		if err != nil && !errors.Is(err, errReaderStopped) {
			// 1236 ER_MASTER_FATAL_ERROR_READING_BINLOG: the binlog was
			// purged or the position is invalid — a distinct, actionable
			// condition, not a generic stream end (#182).
			var myErr *gomysql.MyError
			if errors.As(err, &myErr) && myErr.Code == gomysql.ER_MASTER_FATAL_ERROR_READING_BINLOG {
				return fmt.Errorf("mysql: binlog purged or position invalid (error %d): %w", myErr.Code, err)
			}
			return fmt.Errorf("mysql: stream ended: %w", err)
		}
		return nil
	}
}

// Synced reports the reader's current synced GTID set.
func (r *Reader) Synced() position.Position {
	set := r.canal.SyncedGTIDSet()
	if g, err := position.ParseGTID(set.String()); err == nil {
		return g
	}
	return position.MustGTID("")
}

// Master reports the master's current executed GTID set (the caught-up
// target for the DBLog window). go-mysql's GetMasterGTIDSet has no ctx
// variant, so it runs in a goroutine and this call gives up when ctx is done
// (the query itself is bounded by the canal connection's read timeout).
func (r *Reader) Master(ctx context.Context) (position.Position, error) {
	type result struct {
		set *position.GTID
		err error
	}
	ch := make(chan result, 1)
	go func() {
		set, err := r.canal.GetMasterGTIDSet()
		if err != nil {
			ch <- result{err: fmt.Errorf("mysql: master gtid: %w", err)}
			return
		}
		ch <- result{set: position.MustGTID(set.String())}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		return res.set, res.err
	}
}

// Close stops the reader and its replication connection. Safe to call
// before StartFromGTID unblocks the same shutdown path it uses.
func (r *Reader) Close() {
	r.stop()
	r.canal.Close()
}

// SetConfirmed is a no-op for MySQL: binlog retention is time/size-based,
// not consumer-confirmed, so there is no slot to hold back.
func (r *Reader) SetConfirmed(_ func() position.Position) {}

// ── canal.EventHandler ──────────────────────────────────────────────

// OnGTID records the transaction's GTID and commit time, carried by every
// row of the transaction.
func (r *Reader) OnGTID(header *replication.EventHeader, e gomysql.BinlogGTIDEvent) error {
	next, err := e.GTIDNext()
	if err != nil {
		return fmt.Errorf("mysql: gtid next: %w", err)
	}
	g, err := position.ParseGTID(next.String())
	if err != nil {
		return fmt.Errorf("mysql: gtid next parse: %w", err)
	}
	// The transaction commit time: microsecond precision from the GTID event
	// on MySQL 8.0.1+, else the event header's second-precision timestamp.
	// MySQL 8.0.1+ always carries OriginalCommitTimestamp, so the header is a
	// fallback for older servers (and MariaDB, whose GTID event is a different
	// concrete type). See issue #137.
	commitTS := time.Unix(int64(header.Timestamp), 0).UTC()
	if ge, ok := e.(*replication.GTIDEvent); ok {
		if t := ge.OriginalCommitTime(); !t.IsZero() {
			commitTS = t.UTC()
		}
	}
	r.mu.Lock()
	r.curCommitTS = commitTS
	r.mu.Unlock()
	r.mergeGTID(g)
	return nil
}

// setStart seeds the cumulative resume set from start, as the reader's own
// copy: mergeGTID advances the set in place, and start belongs to the caller
// (the coordinator keeps and reads it).
func (r *Reader) setStart(start *position.GTID) {
	own := position.MustGTID(start.String())
	r.mu.Lock()
	defer r.mu.Unlock()
	r.curSet = own
}

// mergeGTID folds one transaction GTID into the cumulative resume set. A
// resume position is a CUMULATIVE set — "everything up to and including this
// transaction" ("uuid:1-N") — not the single transaction GTID: starting from
// a lone {N} makes the master replay everything except N.
func (r *Reader) mergeGTID(g *position.GTID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.curSet == nil {
		r.curSet = position.MustGTID("")
	}
	r.curSet.Add(g)
	r.curGTID = r.curSet.String()
}

// OnRow decodes one row event straight into the target tables' Arrow builders.
func (r *Reader) OnRow(e *canal.RowsEvent) error {
	ref, ok := r.bySrc[e.Table.Schema+"."+e.Table.Name]
	if !ok {
		return nil // excluded by regex; double-check only
	}

	r.mu.Lock()
	pos := r.curGTID
	commitTS := r.curCommitTS
	r.mu.Unlock()

	if dbg := r.cfg.Logger; dbg != nil && os.Getenv("URUTAU_DEBUG_READER") != "" {
		dbg.Info("row", "table", ref.Source, "action", e.Action, "pos", pos, "nrows", len(e.Rows))
	}

	switch e.Action {
	case canal.InsertAction:
		for _, row := range e.Rows {
			if err := r.appendChange(ref, e.Table, rowchange.OpInsert, row, nil, pos, commitTS); err != nil {
				return err
			}
		}
	case canal.DeleteAction:
		for _, row := range e.Rows {
			if err := r.appendChange(ref, e.Table, rowchange.OpDelete, row, nil, pos, commitTS); err != nil {
				return err
			}
		}
	case canal.UpdateAction:
		// Rows come as [before, after] pairs.
		for i := 0; i+1 < len(e.Rows); i += 2 {
			if err := r.appendChange(ref, e.Table, rowchange.OpUpdate, e.Rows[i+1], e.Rows[i], pos, commitTS); err != nil {
				return err
			}
		}
	}
	return nil
}

// encoder returns (building once) the target table's Arrow builder. A table
// image whose column count no longer matches the one the mapping was built
// from is source drift: fail loud so the operator declares the change and
// resumes, exactly where the native row shape exists.
func (r *Reader) encoder(ref TableRef, tbl *schema.Table) (*tableEncoder, error) {
	if te, ok := r.encoders[ref.Target]; ok {
		return te, nil
	}
	cs, ok := r.cfg.Schemas[ref.Target]
	if !ok || len(cs.Columns) == 0 {
		return nil, fmt.Errorf("mysql: table %s has no canonical schema", ref.Target)
	}
	te, err := newTableEncoder(ref.Target, cs, tbl, r.projections[ref.Source])
	if err != nil {
		return nil, fmt.Errorf("mysql: %s: %w", ref.Source, err)
	}
	r.encoders[ref.Target] = te
	r.encoderOrder = append(r.encoderOrder, ref.Target)
	return te, nil
}

// appendChange applies the read filter/projection and appends the row image to
// the target's builder as one Arrow row, with no map[string]any in between.
// It mirrors the row path's semantics: a filtered-out row emits nothing; an
// UPDATE that leaves the filter emits a delete of the before image; an upsert
// target's primary-key change emits a delete of the OLD key as well.
func (r *Reader) appendChange(ref TableRef, tbl *schema.Table, op rowchange.Op, after, before []any, pos string, commitTS time.Time) error {
	// A partial JSON after-image cannot be decoded: with
	// binlog_row_value_options=PARTIAL_JSON an UPDATE carries a JSON column as
	// a diff, which go-mysql exposes as a *replication.JsonDiff, not the full
	// document. The boot preflight rejects the option globally, but it is also
	// settable per session, which a preflight cannot see — this is the
	// decode-time backstop.
	if err := rejectPartialJSON(ref, after); err != nil {
		return err
	}
	if before != nil {
		if err := rejectPartialJSON(ref, before); err != nil {
			return err
		}
	}
	te, err := r.encoder(ref, tbl)
	if err != nil {
		return err
	}
	loc := r.loc()
	proj := r.projections[ref.Source]

	switch op {
	case rowchange.OpDelete:
		// The binlog puts the deleted row image in the after slot.
		keep, err := proj.keep(after, tbl, loc)
		if err != nil {
			return err
		}
		if !keep {
			return nil
		}
		return r.appendImage(te, tbl, op, after, pos, commitTS)
	case rowchange.OpUpdate:
		afterKeep, err := proj.keep(after, tbl, loc)
		if err != nil {
			return err
		}
		beforeKeep := false
		if before != nil {
			if beforeKeep, err = proj.keep(before, tbl, loc); err != nil {
				return err
			}
		}
		switch {
		case !afterKeep && !beforeKeep:
			return nil
		case beforeKeep && !afterKeep:
			// The row left the filter: emit a delete so an upsert target
			// removes the now-excluded row instead of keeping a stale copy.
			return r.appendImage(te, tbl, rowchange.OpDelete, before, pos, commitTS)
		default:
			// In upsert mode an UPDATE that changes the primary key must
			// delete the OLD key: the update's own delete is built from the
			// new key, so without this the old row survives forever. Append
			// targets keep the old row by design.
			if before != nil && r.cfg.UpsertTargets[ref.Target] {
				oldKey := r.keyOf(ref, tbl, before)
				newKey := r.keyOf(ref, tbl, after)
				if rowchange.KeyString(oldKey) != rowchange.KeyString(newKey) {
					if err := r.appendImage(te, tbl, rowchange.OpDelete, before, pos, commitTS); err != nil {
						return err
					}
				}
			}
			return r.appendImage(te, tbl, rowchange.OpUpdate, after, pos, commitTS)
		}
	default: // insert
		keep, err := proj.keep(after, tbl, loc)
		if err != nil {
			return err
		}
		if !keep {
			return nil
		}
		return r.appendImage(te, tbl, op, after, pos, commitTS)
	}
}

// appendImage writes one row image into the builder. A delete needs a primary
// key in the canonical schema, or the equality delete would match nothing
// (C-8). The row is stamped with the current SAFE position; only the
// transaction's final record is restamped when it closes.
// rejectPartialJSON fails loud when a row image carries a partial JSON value.
// go-mysql decodes a PARTIAL_UPDATE_ROWS_EVENT's JSON column as a
// *replication.JsonDiff (or a slice of them): the binlog does not carry the
// full document, so urutau cannot reconstruct the after-image and would
// otherwise write a partial value. Rejecting is the only safe outcome.
func rejectPartialJSON(ref TableRef, image []any) error {
	for i, v := range image {
		switch v.(type) {
		case *replication.JsonDiff, []*replication.JsonDiff:
			return fmt.Errorf("mysql: %s: column %d arrived as a partial JSON diff (binlog_row_value_options=PARTIAL_JSON); the full document is not in the binlog — unset PARTIAL_JSON (globally and for the writing session)", ref.Source, i)
		}
	}
	return nil
}

func (r *Reader) appendImage(te *tableEncoder, tbl *schema.Table, op rowchange.Op, image []any, pos string, commitTS time.Time) error {
	if op == rowchange.OpDelete && len(te.enc.Schema().PrimaryKey) == 0 {
		return fmt.Errorf("sourcepull: batch %q carries a delete but the schema has no primary key — declare it and resume", te.target)
	}
	meta := transport.RowMeta{Op: op, Position: r.safePos, CommitTS: commitTS, IngestTS: time.Now()}
	if err := te.appendRow(image, tbl, r.loc(), meta); err != nil {
		return err
	}
	r.txnRows = true
	if te.rows >= maxDirectRows || te.bytes >= maxDirectBytes {
		r.pending = append(r.pending, heldRec{target: te.target, rec: te.materialize()})
	}
	return nil
}

// keyOf builds a row's primary-key tuple from the raw image, in spec order.
func (r *Reader) keyOf(ref TableRef, tbl *schema.Table, row []any) []any {
	key := make([]any, 0, len(ref.PrimaryKey))
	for _, pk := range ref.PrimaryKey {
		if idx := tbl.FindColumn(pk); idx >= 0 && idx < len(row) {
			key = append(key, row[idx])
		} else {
			key = append(key, nil)
		}
	}
	return key
}

// closeTxn materializes the transaction's remaining rows, stamps only its
// final record with the transaction position, and emits every record in
// order. Earlier pieces carry the previous safe position, so an ack cannot
// advance the durable checkpoint past rows a later piece still owes (#456).
func (r *Reader) closeTxn() error {
	for _, target := range r.encoderOrder {
		if rec := r.encoders[target].materialize(); rec != nil {
			r.pending = append(r.pending, heldRec{target: target, rec: rec})
		}
	}
	if len(r.pending) == 0 {
		return nil
	}
	last := &r.pending[len(r.pending)-1]
	rebuilt, err := transport.WithPosition(last.rec, r.curGTID)
	if err != nil {
		r.releasePending()
		return err
	}
	last.rec.Release()
	last.rec = rebuilt

	pending := r.pending
	r.pending = nil
	for i, hb := range pending {
		b := &dataplane.Batch{Table: hb.target, Record: hb.rec, Mode: dataplane.UpsertMode}
		if err := r.sendBatch(b); err != nil {
			// Release this record and every record not yet sent: a failed
			// send must not leak the transaction's Arrow buffers.
			for _, rest := range pending[i:] {
				if rest.rec != nil {
					rest.rec.Release()
				}
			}
			return err
		}
	}
	r.safePos = r.curGTID
	return nil
}

// releasePending drops the buffered records of the transaction being decoded.
func (r *Reader) releasePending() {
	for _, hb := range r.pending {
		if hb.rec != nil {
			hb.rec.Release()
		}
	}
	r.pending = nil
}

// sendBatch hands one record to the coordinator/runner, or reports the orderly
// stop when the reader is shutting down first.
func (r *Reader) sendBatch(b *dataplane.Batch) error {
	select {
	case r.batchOut <- b:
		return nil
	case <-r.done:
		return errReaderStopped
	}
}

// OnPosSynced marks the end of a transaction: canal calls it after an XID
// event, a non-transactional COMMIT and a DDL. A transaction that emitted
// rows is then closed with OpTxnEnd, so the puller never batches part of it
// (#456). Canal.Close also calls it, with a nil header, from outside the
// event goroutine and possibly mid-transaction: that call ends nothing.
func (r *Reader) OnPosSynced(header *replication.EventHeader, _ gomysql.Position, _ gomysql.GTIDSet, _ bool) error {
	if header == nil || !r.txnRows {
		return nil
	}
	r.txnRows = false
	return r.closeTxn()
}

// errReaderStopped is emit's sentinel for "the reader is shutting down" —
// not a stream failure, so callers must not treat it as one.
var errReaderStopped = fmt.Errorf("mysql: reader stopped")

// OnDDL surfaces DDL statements seen on the stream. The query is the
// authoritative statement. Row data is not produced here — schema drift on
// the data path (ADD COLUMN etc.) is caught by the worker's drift check
// comparing each change against the introspected schema, which is what
// pauses the pipeline. This hook logs for operator visibility.
func (r *Reader) OnDDL(_ *replication.EventHeader, _ gomysql.Position, q *replication.QueryEvent) error {
	r.cfg.Logger.Warn("mysql: DDL detected", "query", string(q.Query))
	return nil
}

// loc returns the operator's temporal location, defaulting to UTC.
func (r *Reader) loc() *time.Location {
	if r.cfg.TimeLocation == nil {
		return time.UTC
	}
	return r.cfg.TimeLocation
}

// normalizeCol converts one binlog value using its column definition.
func normalizeCol(col schema.TableColumn, v any, loc *time.Location) any {
	if loc == nil {
		loc = time.UTC
	}
	switch col.Type {
	case schema.TYPE_ENUM:
		return decodeEnum(col, v)
	case schema.TYPE_SET:
		return decodeSet(col, v)
	case schema.TYPE_BINARY:
		// go-mysql maps binary(n) and varbinary(n) here. A fixed BINARY(n)
		// arrives without its trailing 0x00 padding and the target is
		// FixedSizeBinary(n), whose builder panics on a short value; repad to
		// the declared width. A varbinary (FixedSize 0) keeps its
		// byte-preserving string form, as before (#562).
		b, ok := v.([]byte)
		if !ok || col.FixedSize == 0 {
			return normalize(v)
		}
		if n := int(col.FixedSize); len(b) < n {
			padded := make([]byte, n)
			copy(padded, b)
			return padded
		}
		return b
	case schema.TYPE_STRING:
		// Text columns carry their bytes in the column's own character set,
		// unconverted (see charset.go). TYPE_BINARY is deliberately not here:
		// its bytes are not text and reinterpreting them would corrupt them.
		if b, ok := v.([]byte); ok {
			return decodeString(b, col.Collation)
		}

		return normalize(v)
	case schema.TYPE_TIMESTAMP:
		// An instant: go-mysql returns it in the process's Local zone. Express
		// it in the operator's location so it matches the snapshot query
		// (issue #139).
		if t, ok := v.(time.Time); ok {
			return t.In(loc)
		}
		if s, ok := v.(string); ok && isZeroTemporal(s) {
			return time.Time{} // match the snapshot's zero value
		}

		return normalize(v)
	case schema.TYPE_DATETIME:
		// No zone: go-mysql tags the wall clock UTC. Re-tag the SAME wall clock
		// to the operator's location — do NOT convert the instant — so it
		// matches the snapshot's parseTime interpretation of a naive DATETIME.
		if t, ok := v.(time.Time); ok {
			return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), loc)
		}
		if s, ok := v.(string); ok && isZeroTemporal(s) {
			return time.Time{}
		}

		return normalize(v)
	case schema.TYPE_DATE:
		// go-mysql returns DATE as a "2006-01-02" string regardless of
		// ParseTime; the snapshot returns a time.Time at midnight. Normalize to
		// a time.Time in the operator's location.
		if s, ok := v.(string); ok {
			if isZeroTemporal(s) {
				return time.Time{}
			}
			if t, err := time.ParseInLocation("2006-01-02", s, loc); err == nil {
				return t
			}
		}

		return normalize(v)
	default:
		return normalize(v)
	}
}

// decodeEnum turns the binlog's 1-based member index into the member string.
// MySQL stores an ENUM as its ordinal, so the raw event carries int64(2) for
// the second member — which the target column (mapped to KindString by
// mapColumnType) would otherwise stringify to "2" instead of "b". No error is
// raised anywhere on that path, so the wrong value lands silently.
//
// Index 0 is MySQL's marker for a value rejected on insert (non-strict mode
// stores the empty string), and an index past the member list means the
// column was altered between the TableMapEvent and this decode: both fall
// back to the raw value rather than inventing a member.
func decodeEnum(col schema.TableColumn, v any) any {
	idx, ok := v.(int64)
	if !ok || len(col.EnumValues) == 0 {
		return normalize(v)
	}
	if idx == 0 {
		return ""
	}
	if idx < 0 || int(idx) > len(col.EnumValues) {
		return normalize(v)
	}
	return col.EnumValues[idx-1]
}

// decodeSet turns the binlog's bitmask into the comma-joined member list, in
// declaration order — the same text the backfill's SELECT returns. Bit 0 is
// the first member: SET('x','y','z') holding 'x,z' arrives as 0b101.
//
// Bits past the member list mean the column was altered between the
// TableMapEvent and this decode; they are ignored rather than dropping the
// members that did resolve.
func decodeSet(col schema.TableColumn, v any) any {
	mask, ok := v.(int64)
	if !ok || len(col.SetValues) == 0 {
		return normalize(v)
	}
	members := make([]string, 0, len(col.SetValues))
	for bit := 0; bit < len(col.SetValues) && bit < 64; bit++ {
		if mask&(1<<uint(bit)) != 0 {
			members = append(members, col.SetValues[bit])
		}
	}
	return strings.Join(members, ",")
}

// isZeroTemporal reports whether a go-mysql temporal string is MySQL's zero
// value ("0000-00-00" or "0000-00-00 00:00:00[.frac]"). The snapshot driver
// (parseTime) returns these as time.Time{}, so the CDC must too, or the same
// column has a different Go type by path.
func isZeroTemporal(s string) bool { return strings.HasPrefix(s, "0000-00-00") }

// normalize converts driver-native []byte cells to strings so the canonical
// value space sees text, not raw bytes.
func normalize(v any) any {
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return v
}

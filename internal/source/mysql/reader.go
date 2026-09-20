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
	"github.com/maltzsama/urutau/internal/rowchange"
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
}

// Reader wraps a canal instance and decodes its row events.
type Reader struct {
	cfg   Config
	canal *canal.Canal
	out   chan<- rowchange.Change
	bySrc map[string]TableRef // "db.table" → ref (PK + target)
	// projections is the per-source read projection (#183): the columns to
	// emit and the compiled filter, applied on each decoded row.
	projections map[string]projection
	mu          sync.Mutex
	curSet      *position.GTID // accumulated GTID set through the current transaction
	curGTID     string         // curSet.String() — the position rows of this txn carry
	curTxn      *position.GTID // single GTID of the transaction being decoded (window check)
	// curCommitTS is the transaction's commit time, captured on the GTID
	// event and stamped onto every row of the transaction (issue #137).
	curCommitTS time.Time

	winMu    sync.Mutex
	winChunk uint32 // chunkID of the open DBLog window, when winOpen
	winOpen  bool
	winLow   *position.GTID // source watermark captured at OpenWindow

	// done is closed once, by StartFromGTID on the way out (whether it
	// exits via ctx.Done() or the stream ending on its own). OnRow selects
	// on it alongside every send to r.out: without this, a stalled
	// consumer blocks OnRow forever on canal's own event-loop goroutine,
	// which stops canal from reading further binlog events (no GTIDs, no
	// heartbeats, no rotates) and makes Close()/ctx cancellation hang,
	// because neither can unblock a goroutine parked on a channel send —
	// only a reader on the other end can. See issue #114.
	done     chan struct{}
	doneOnce sync.Once

	canal.DummyEventHandler // unimplemented hooks are no-ops
}

// OpenWindow opens the DBLog window for chunkID. The reader captures its
// source watermark — the master's executed GTID set — and, from now on, tags
// decoded events whose transaction is strictly past that watermark InWindow
// for the chunk, until ClearWindow. Events at or before the watermark are
// already reflected in the chunk SELECT and must not be tagged. The tag is
// applied synchronously at decode — no event can escape the window by racing
// a channel pull.
func (r *Reader) OpenWindow(ctx context.Context, chunkID uint32) {
	var low *position.GTID
	if g, ok := r.Synced().(*position.GTID); ok {
		low = g
	}
	if m, err := r.Master(ctx); err == nil {
		if g, ok := m.(*position.GTID); ok {
			low = g
		}
	}
	r.winMu.Lock()
	r.winOpen = true
	r.winChunk = chunkID
	r.winLow = low
	r.winMu.Unlock()
}

// ClearWindow closes the DBLog window opened by OpenWindow.
func (r *Reader) ClearWindow() {
	r.winMu.Lock()
	r.winOpen = false
	r.winLow = nil
	r.winMu.Unlock()
}

// New builds the reader but does not start it.
func New(ctx context.Context, cfg Config, out chan<- rowchange.Change) (*Reader, error) {
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

	r := &Reader{cfg: cfg, canal: c, out: out, bySrc: bySrc, projections: cfg.Projections, done: make(chan struct{})}
	c.SetEventHandler(r)
	return r, nil
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
	r.mu.Lock()
	r.curSet = start
	r.mu.Unlock()

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
	r.curTxn = g
	r.curCommitTS = commitTS
	r.mu.Unlock()
	r.mergeGTID(g)
	return nil
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

// OnRow decodes one row event into rowchange.Change values and emits them.
func (r *Reader) OnRow(e *canal.RowsEvent) error {
	ref, ok := r.bySrc[e.Table.Schema+"."+e.Table.Name]
	if !ok {
		return nil // excluded by regex; double-check only
	}

	r.mu.Lock()
	pos := r.curGTID
	txn := r.curTxn
	commitTS := r.curCommitTS
	r.winMu.Lock()
	var win *rowchange.Window
	// Only events strictly past the low watermark are InWindow: an event at
	// or before low is already reflected in the chunk SELECT (or in an
	// earlier chunk), so tagging it would resurrect a stale value. A missing
	// watermark falls back to tagging everything — over-tagging is safe.
	if r.winOpen && (r.winLow == nil || txn == nil || !r.winLow.Contains(txn)) {
		win = &rowchange.Window{ChunkID: r.winChunk, InWindow: true}
	}
	r.winMu.Unlock()
	r.mu.Unlock()

	if dbg := r.cfg.Logger; dbg != nil && os.Getenv("URUTAU_DEBUG_READER") != "" {
		dbg.Info("row", "table", ref.Source, "action", e.Action, "pos", pos,
			"nrows", len(e.Rows), "win", win)
	}

	switch e.Action {
	case canal.InsertAction:
		for _, row := range e.Rows {
			c, emit, err := r.decode(ref, e.Table, rowchange.OpInsert, row, nil, pos, commitTS)
			if err != nil {
				return err
			}
			if !emit {
				continue
			}
			c.Window = win
			if err := r.emit(c); err != nil {
				return err
			}
		}
	case canal.DeleteAction:
		for _, row := range e.Rows {
			c, emit, err := r.decode(ref, e.Table, rowchange.OpDelete, row, nil, pos, commitTS)
			if err != nil {
				return err
			}
			if !emit {
				continue
			}
			c.Window = win
			if err := r.emit(c); err != nil {
				return err
			}
		}
	case canal.UpdateAction:
		// Rows come as [before, after] pairs.
		for i := 0; i+1 < len(e.Rows); i += 2 {
			c, emit, err := r.decode(ref, e.Table, rowchange.OpUpdate, e.Rows[i+1], e.Rows[i], pos, commitTS)
			if err != nil {
				return err
			}
			if !emit {
				continue
			}
			c.Window = win
			if err := r.emit(c); err != nil {
				return err
			}
		}
	}
	return nil
}

// emit sends c to the reader's output, or returns an error if the reader is
// shutting down first. A bare `r.out <- c` would block OnRow — and with it
// canal's whole event loop — for as long as a stalled consumer takes to
// drain the channel, with Close()/context cancellation unable to break it
// out (see the done field's doc comment, issue #114). Returning the error
// makes canal tear the stream down instead of continuing to feed a reader
// that is on its way out.
func (r *Reader) emit(c rowchange.Change) error {
	select {
	case r.out <- c:
		return nil
	case <-r.done:
		return errReaderStopped
	}
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

// decode maps one row (in table column order) to a rowchange, applying the
// table's read projection and filter (#183). It returns emit=false when the
// filter excludes the row. key is built from the spec primary key columns, in
// spec order. commitTS is the transaction's commit time, carried onto every
// row of the transaction.
//
// The binlog puts the deleted row in the after slot for a DELETE and the old
// row in before / the new row in after for an UPDATE.
func (r *Reader) decode(ref TableRef, tbl *schema.Table, op rowchange.Op, after, before []any, pos string, commitTS time.Time) (rowchange.Change, bool, error) {
	proj := r.projections[ref.Source]
	loc := r.loc()
	c := rowchange.Change{
		Op:       op,
		Table:    ref.Target,
		Position: pos,
		CommitTS: commitTS,
		IngestTS: time.Now(),
	}
	keyFrom := func(row []any) []any {
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

	switch op {
	case rowchange.OpDelete:
		full := rowToMap(tbl, after, loc)
		keep, err := proj.keep(full)
		if err != nil {
			return rowchange.Change{}, false, err
		}
		if !keep {
			return rowchange.Change{}, false, nil
		}
		c.Key = keyFrom(after)
		c.Before = proj.project(full)
		return c, true, nil
	case rowchange.OpUpdate:
		fullAfter := rowToMap(tbl, after, loc)
		var fullBefore map[string]any
		if before != nil {
			fullBefore = rowToMap(tbl, before, loc)
		}
		afterKeep, err := proj.keep(fullAfter)
		if err != nil {
			return rowchange.Change{}, false, err
		}
		beforeKeep := false
		if fullBefore != nil {
			if beforeKeep, err = proj.keep(fullBefore); err != nil {
				return rowchange.Change{}, false, err
			}
		}
		switch {
		case !afterKeep && !beforeKeep:
			return rowchange.Change{}, false, nil
		case beforeKeep && !afterKeep:
			// The row left the filter: emit a delete so an upsert target
			// removes the now-excluded row instead of keeping a stale copy.
			c.Op = rowchange.OpDelete
			c.Key = keyFrom(before)
			c.Before = proj.project(fullBefore)
			return c, true, nil
		default:
			c.Key = keyFrom(after)
			c.After = proj.project(fullAfter)
			if fullBefore != nil {
				c.Before = proj.project(fullBefore)
			}
			return c, true, nil
		}
	default: // insert
		full := rowToMap(tbl, after, loc)
		keep, err := proj.keep(full)
		if err != nil {
			return rowchange.Change{}, false, err
		}
		if !keep {
			return rowchange.Change{}, false, nil
		}
		c.Key = keyFrom(after)
		c.After = proj.project(full)
		return c, true, nil
	}
}

// loc returns the operator's temporal location, defaulting to UTC.
func (r *Reader) loc() *time.Location {
	if r.cfg.TimeLocation == nil {
		return time.UTC
	}
	return r.cfg.TimeLocation
}

// rowToMap maps a row (in table column order) to column-name → value.
// The binlog yields []byte for string columns; normalize them to string so
// the writer's scalar type switch accepts them.
//
// ENUM and SET need the column definition, not just the value: the binlog
// encodes them numerically (see decodeEnum/decodeSet), while the backfill's
// SELECT returns them as text. Both paths feed the same target column, so
// decoding here is what keeps a CDC row and a snapshot row of the same
// source row identical.
func rowToMap(tbl *schema.Table, row []any, loc *time.Location) map[string]any {
	out := make(map[string]any, len(tbl.Columns))
	for i, col := range tbl.Columns {
		if i >= len(row) {
			continue
		}
		out[col.Name] = normalizeCol(col, row[i], loc)
	}
	return out
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

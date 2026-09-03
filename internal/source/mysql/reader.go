// Package source.mysql implements the MySQL replication source on top of
// go-mysql/canal: a single binlog reader that decodes row events into
// change.Change, positions them at their transaction GTID, and exposes the
// synced and master positions for the DBLog watermark logic.
package mysql

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"

	"github.com/maltzsama/urutau/internal/change"
	"github.com/maltzsama/urutau/internal/core"
	"github.com/maltzsama/urutau/internal/position"
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
}

// Reader wraps a canal instance and decodes its row events.
type Reader struct {
	cfg     Config
	canal   *canal.Canal
	out     chan<- change.Change
	bySrc   map[string]TableRef // "db.table" → ref (PK + target)
	mu      sync.Mutex
	curSet  *position.GTID // accumulated GTID set through the current transaction
	curGTID string         // curSet.String() — the position rows of this txn carry
	curTxn  *position.GTID // single GTID of the transaction being decoded (window check)

	winMu    sync.Mutex
	winChunk uint32 // chunkID of the open DBLog window, when winOpen
	winOpen  bool
	winLow   *position.GTID // source watermark captured at OpenWindow

	lastDDL string // most recent DDL statement (SchemaMismatch trigger)

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
func New(ctx context.Context, cfg Config, out chan<- change.Change) (*Reader, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	incl := make([]string, 0, len(cfg.Tables))
	bySrc := make(map[string]TableRef, len(cfg.Tables))
	for _, t := range cfg.Tables {
		incl = append(incl, fmt.Sprintf(`^%s$`, regexp.QuoteMeta(t.Source)))
		bySrc[t.Source] = t
	}

	c, err := canal.NewCanal(&canal.Config{
		Addr:              cfg.Addr,
		User:              cfg.User,
		Password:          cfg.Password,
		ServerID:          cfg.ServerID,
		Flavor:            "mysql",
		HeartbeatPeriod:   cfg.Heartbeat,
		ReadTimeout:       60 * time.Second,
		IncludeTableRegex: incl,
		ParseTime:         false,
		Logger:            cfg.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("mysql: new canal: %w", err)
	}

	r := &Reader{cfg: cfg, canal: c, out: out, bySrc: bySrc}
	c.SetEventHandler(r)
	return r, nil
}

// StartFromGTID begins streaming from the given GTID set, blocking until
// the stream ends or the context is cancelled. Call in a goroutine.
func (r *Reader) StartFromGTID(ctx context.Context, start *position.GTID) error {
	r.cfg.Logger.Info("reader start", "from", start.String())
	r.mu.Lock()
	r.curSet = start
	r.mu.Unlock()

	// canal.StartFromGTID runs the stream to completion — it must be the
	// ONLY sync loop. A second canal.Run() would kill the first
	// connection ("kill last connection" / "Sync was closed").
	done := make(chan error, 1)
	go func() { done <- r.canal.StartFromGTID(start.Raw()) }()

	select {
	case <-ctx.Done():
		r.canal.Close()
		return ctx.Err()
	case err := <-done:
		if err != nil {
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

// Close stops the reader and its replication connection.
func (r *Reader) Close() { r.canal.Close() }

// ── canal.EventHandler ──────────────────────────────────────────────

func (r *Reader) OnGTID(_ *replication.EventHeader, e gomysql.BinlogGTIDEvent) error {
	next, err := e.GTIDNext()
	if err != nil {
		return fmt.Errorf("mysql: gtid next: %w", err)
	}
	g, err := position.ParseGTID(next.String())
	if err != nil {
		return fmt.Errorf("mysql: gtid next parse: %w", err)
	}
	r.mu.Lock()
	r.curTxn = g
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

func (r *Reader) OnRow(e *canal.RowsEvent) error {
	ref, ok := r.bySrc[e.Table.Schema+"."+e.Table.Name]
	if !ok {
		return nil // excluded by regex; double-check only
	}

	r.mu.Lock()
	pos := r.curGTID
	txn := r.curTxn
	r.winMu.Lock()
	var win *change.Window
	// Only events strictly past the low watermark are InWindow: an event at
	// or before low is already reflected in the chunk SELECT (or in an
	// earlier chunk), so tagging it would resurrect a stale value. A missing
	// watermark falls back to tagging everything — over-tagging is safe.
	if r.winOpen && (r.winLow == nil || txn == nil || !r.winLow.Contains(txn)) {
		win = &change.Window{ChunkID: r.winChunk, InWindow: true}
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
			c := r.decode(ref, e.Table, change.OpInsert, row, nil, pos)
			c.Window = win
			r.out <- c
		}
	case canal.DeleteAction:
		for _, row := range e.Rows {
			c := r.decode(ref, e.Table, change.OpDelete, row, nil, pos)
			c.Window = win
			r.out <- c
		}
	case canal.UpdateAction:
		// Rows come as [before, after] pairs.
		for i := 0; i+1 < len(e.Rows); i += 2 {
			c := r.decode(ref, e.Table, change.OpUpdate, e.Rows[i+1], e.Rows[i], pos)
			c.Window = win
			r.out <- c
		}
	}
	return nil
}

// OnDDL surfaces DDL statements seen on the stream (design §14). The DDL is
// routed to the coordinator for the SchemaMismatch pause; here we log it and
// expose it on the reader so the pipeline can decide. The query is the
// authoritative statement; the table is best-effort from the query text.
func (r *Reader) OnDDL(_ *replication.EventHeader, _ gomysql.Position, q *replication.QueryEvent) error {
	r.cfg.Logger.Warn("mysql: DDL detected", "query", string(q.Query))
	r.mu.Lock()
	r.lastDDL = string(q.Query)
	r.mu.Unlock()
	return nil
}

// LastDDL returns the most recent DDL statement seen on the stream.
func (r *Reader) LastDDL() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastDDL
}

// decode maps one row (in table column order) to a change. key is built from
// the spec primary key columns, in spec order.
func (r *Reader) decode(ref TableRef, tbl *schema.Table, op change.Op, after, before []any, pos string) change.Change {
	c := change.Change{
		Op:       op,
		Table:    ref.Target,
		Position: pos,
	}

	key := make([]any, 0, len(ref.PrimaryKey))
	for _, pk := range ref.PrimaryKey {
		if idx := tbl.FindColumn(pk); idx >= 0 {
			key = append(key, after[idx])
		} else {
			key = append(key, nil)
		}
	}
	c.Key = key

	if op == change.OpDelete {
		if before != nil {
			c.Before = rowToMap(tbl, before)
		}
		return c
	}
	c.After = rowToMap(tbl, after)
	if before != nil {
		c.Before = rowToMap(tbl, before)
	}
	return c
}

// rowToMap maps a row (in table column order) to column-name → value.
// The binlog yields []byte for string columns; normalize them to string so
// the writer's scalar type switch accepts them.
func rowToMap(tbl *schema.Table, row []any) map[string]any {
	out := make(map[string]any, len(tbl.Columns))
	for i, col := range tbl.Columns {
		if i >= len(row) {
			continue
		}
		out[col.Name] = normalize(row[i])
	}
	return out
}

func normalize(v any) any {
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return v
}

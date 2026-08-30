// Package source.mysql implements the MySQL replication source on top of
// go-mysql/canal: a single binlog reader that decodes row events into
// change.Change, positions them at their transaction GTID, and exposes the
// synced and master positions for the DBLog watermark logic.
package mysql

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"

	"github.com/maltzsama/urutau/internal/change"
	"github.com/maltzsama/urutau/internal/position"
)

// TableRef maps one source table to its target and primary key.
type TableRef struct {
	Source     string // "db.table"
	Target     string // "raw.orders"
	PrimaryKey []string
}

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
	curGTID string // GTID of the transaction being decoded

	canal.DummyEventHandler // unimplemented hooks are no-ops
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
	if err := r.canal.StartFromGTID(start.Raw()); err != nil {
		return fmt.Errorf("mysql: start from gtid %s: %w", start, err)
	}
	done := make(chan error, 1)
	go func() { done <- r.canal.Run() }()

	select {
	case <-ctx.Done():
		r.canal.Close()
		return ctx.Err()
	case err := <-done:
		return fmt.Errorf("mysql: stream ended: %w", err)
	}
}

// Synced reports the reader's current synced GTID set.
func (r *Reader) Synced() *position.GTID {
	set := r.canal.SyncedGTIDSet()
	if g, err := position.ParseGTID(set.String()); err == nil {
		return g
	}
	return position.MustGTID("")
}

// Master reports the master's current executed GTID set (the caught-up
// target for the DBLog window).
func (r *Reader) Master() (*position.GTID, error) {
	set, err := r.canal.GetMasterGTIDSet()
	if err != nil {
		return nil, fmt.Errorf("mysql: master gtid: %w", err)
	}
	return position.MustGTID(set.String()), nil
}

// Close stops the reader and its replication connection.
func (r *Reader) Close() { r.canal.Close() }

// ── canal.EventHandler ──────────────────────────────────────────────

func (r *Reader) OnGTID(_ *replication.EventHeader, e gomysql.BinlogGTIDEvent) error {
	set, err := e.GTIDNext()
	if err != nil {
		return fmt.Errorf("mysql: gtid next: %w", err)
	}
	r.mu.Lock()
	r.curGTID = set.String()
	r.mu.Unlock()
	return nil
}

func (r *Reader) OnRow(e *canal.RowsEvent) error {
	ref, ok := r.bySrc[e.Table.Schema+"."+e.Table.Name]
	if !ok {
		return nil // excluded by regex; double-check only
	}

	r.mu.Lock()
	pos := r.curGTID
	r.mu.Unlock()

	switch e.Action {
	case canal.InsertAction:
		for _, row := range e.Rows {
			r.out <- r.decode(ref, e.Table, change.OpInsert, row, nil, pos)
		}
	case canal.DeleteAction:
		for _, row := range e.Rows {
			r.out <- r.decode(ref, e.Table, change.OpDelete, row, nil, pos)
		}
	case canal.UpdateAction:
		// Rows come as [before, after] pairs.
		for i := 0; i+1 < len(e.Rows); i += 2 {
			r.out <- r.decode(ref, e.Table, change.OpUpdate, e.Rows[i+1], e.Rows[i], pos)
		}
	}
	return nil
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
func rowToMap(tbl *schema.Table, row []any) map[string]any {
	out := make(map[string]any, len(tbl.Columns))
	for i, col := range tbl.Columns {
		if i < len(row) {
			out[col.Name] = row[i]
		}
	}
	return out
}

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	pglogrepl "github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/maltzsama/urutau/internal/change"
	"github.com/maltzsama/urutau/internal/position"
	"github.com/maltzsama/urutau/internal/source/dblog"
)

// statusInterval is how often the reader reports its applied LSN back to
// the server, advancing the slot's confirmed_flush point.
const statusInterval = 10 * time.Second

// Logical message type bytes (pgoutput protocol).
const (
	msgBegin    = 'B'
	msgCommit   = 'C'
	msgInsert   = 'I'
	msgUpdate   = 'U'
	msgDelete   = 'D'
	msgRelation = 'R'
	msgTruncate = 'T'
)

// Config dials one PostgreSQL instance. One replication connection per
// source — the invariant the whole design stands on. DB is the query
// connection used for introspection and position reads.
type Config struct {
	URI      string
	DB       *sql.DB
	SlotName string
	Tables   []dblog.TableRef
	Logger   *slog.Logger
}

// relEntry binds a pgoutput relation id to its introspected state and the
// pipeline's target mapping.
type relEntry struct {
	state *TableState
	ref   dblog.TableRef
}

// Reader wraps one logical-decoding connection and decodes pgoutput row
// changes into change.Change, positioned at their commit LSN.
type Reader struct {
	cfg     Config
	db      *sql.DB
	conn    *pgx.Conn
	out     chan<- change.Change
	bySrc   map[string]dblog.TableRef // "schema.table" → ref (PK + target)
	states  map[string]*TableState    // "schema.table" → introspected state
	relByID map[uint32]relEntry       // relation id → state, from Relation messages

	// Transaction buffer: rows stream inside a transaction before its
	// commit LSN is known, so they accumulate and flush at Commit.
	txn []*change.Change

	mu     sync.Mutex
	synced *position.LSN // end LSN of the last committed transaction

	winMu    sync.Mutex
	winChunk uint32
	winOpen  bool
}

// New introspects the tables, performs the server-side setup (replica
// identity, publication, slot), and opens the replication connection —
// but does not start streaming.
func New(ctx context.Context, cfg Config, out chan<- change.Change) (*Reader, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.DB == nil {
		return nil, fmt.Errorf("postgres: reader: query db required")
	}
	if err := EnsureSetup(ctx, cfg.DB, cfg.SlotName, cfg.Tables); err != nil {
		return nil, err
	}

	bySrc := make(map[string]dblog.TableRef, len(cfg.Tables))
	states := make(map[string]*TableState, len(cfg.Tables))
	for _, ref := range cfg.Tables {
		schema, table, ok := splitSource(ref.Source)
		if !ok {
			return nil, fmt.Errorf("postgres: reader: source %q must be schema.table", ref.Source)
		}
		st, err := QueryTable(ctx, cfg.DB, schema, table)
		if err != nil {
			return nil, fmt.Errorf("postgres: reader: introspect %s: %w", ref.Source, err)
		}
		bySrc[ref.Source] = ref
		states[ref.Source] = st
	}

	connCfg, err := pgx.ParseConfig(cfg.URI)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse uri: %w", err)
	}
	// Logical replication speaks a variant protocol; the server must know
	// this connection is a replication one.
	connCfg.RuntimeParams["replication"] = "database"
	conn, err := pgx.ConnectConfig(ctx, connCfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: replication connect: %w", err)
	}

	return &Reader{
		cfg:     cfg,
		db:      cfg.DB,
		conn:    conn,
		out:     out,
		bySrc:   bySrc,
		states:  states,
		relByID: map[uint32]relEntry{},
		synced:  position.MustLSN("0/0"),
	}, nil
}

// MarkWindow opens the DBLog window for chunkID: every row event decoded
// from now on is tagged InWindow for that chunk, until ClearWindow. The
// tag is applied synchronously at decode — no event can escape the window
// by racing a channel pull.
func (r *Reader) MarkWindow(chunkID uint32) {
	r.winMu.Lock()
	r.winOpen = true
	r.winChunk = chunkID
	r.winMu.Unlock()
}

// ClearWindow closes the DBLog window opened by MarkWindow.
func (r *Reader) ClearWindow() {
	r.winMu.Lock()
	r.winOpen = false
	r.winMu.Unlock()
}

// Synced reports the end LSN of the last committed transaction.
func (r *Reader) Synced() position.Position {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.synced
}

// Master reports the server's current WAL write position (the caught-up
// target for the DBLog window).
func (r *Reader) Master() (position.Position, error) {
	var raw string
	if err := r.db.QueryRowContext(context.Background(),
		`SELECT pg_current_wal_lsn()::text`).Scan(&raw); err != nil {
		return nil, fmt.Errorf("postgres: current wal lsn: %w", err)
	}
	return position.ParseLSN(raw)
}

// StartFromLSN begins streaming from the given LSN, blocking until the
// stream ends or ctx is cancelled. Call in a goroutine. An LSN of 0/0
// starts at the slot's confirmed point.
func (r *Reader) StartFromLSN(ctx context.Context, at *position.LSN) error {
	start := at
	if start == nil {
		start = position.MustLSN("0/0")
	}
	r.cfg.Logger.Info("reader start", "from", start.String())

	err := pglogrepl.StartReplication(ctx, r.conn.PgConn(), r.cfg.SlotName, pglogrepl.LSN(*start),
		pglogrepl.StartReplicationOptions{
			Mode: pglogrepl.LogicalReplication,
			// PluginArgs are joined verbatim into the options list, so
			// each pair carries its own quoting: ("name" 'value', …).
			PluginArgs: []string{
				`"proto_version" '1'`,
				`"publication_names" '` + publicationFor(r.cfg.SlotName) + `'`,
			},
		})
	if err != nil {
		return fmt.Errorf("postgres: start replication: %w", err)
	}

	nextStatus := time.Now().Add(statusInterval)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		recvCtx, cancel := context.WithDeadline(ctx, nextStatus)
		rawMsg, err := r.conn.PgConn().ReceiveMessage(recvCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// The receive deadline is the status tick: report the applied
			// LSN and keep going.
			if errors.Is(err, context.DeadlineExceeded) {
				if serr := r.sendStandby(ctx); serr != nil {
					return serr
				}
				nextStatus = time.Now().Add(statusInterval)
				continue
			}
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) {
				return fmt.Errorf("postgres: stream error %s: %s", pgErr.Code, pgErr.Message)
			}
			return fmt.Errorf("postgres: receive: %w", err)
		}
		nextStatus = time.Now().Add(statusInterval)

		copyData, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}
		switch copyData.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			ka, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("postgres: keepalive: %w", err)
			}
			// The server's WAL end is a delivered floor while no
			// transaction is buffering: everything committed before it has
			// already been streamed. This is what closes the DBLog window
			// on an idle source (the heartbeat analogue).
			r.advanceSyncedFloor(ka.ServerWALEnd)
			if ka.ReplyRequested {
				if serr := r.sendStandby(ctx); serr != nil {
					return serr
				}
			}
		case pglogrepl.XLogDataByteID:
			walData, err := pglogrepl.ParseXLogData(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("postgres: xlog data: %w", err)
			}
			r.advanceSyncedFloor(walData.ServerWALEnd)
			if err := r.handleXLogData(ctx, walData); err != nil {
				return err
			}
		}
	}
}

// Close stops the reader and its replication connection.
func (r *Reader) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = r.conn.Close(ctx)
}

// handleXLogData decodes one logical message payload. payload[0] is the
// message type byte; the decoders below expect the body after it.
func (r *Reader) handleXLogData(ctx context.Context, xld pglogrepl.XLogData) error {
	payload := xld.WALData
	if len(payload) == 0 {
		return nil
	}
	body := payload[1:]
	switch payload[0] {
	case msgBegin:
		r.txn = r.txn[:0]
	case msgRelation:
		if err := r.handleRelation(body); err != nil {
			return err
		}
	case msgInsert:
		if err := r.handleInsert(body); err != nil {
			return err
		}
	case msgUpdate:
		if err := r.handleUpdate(body); err != nil {
			return err
		}
	case msgDelete:
		if err := r.handleDelete(body); err != nil {
			return err
		}
	case msgCommit:
		return r.handleCommit(ctx, xld.WALStart+pglogrepl.LSN(len(xld.WALData)))
	case msgTruncate:
		r.cfg.Logger.Warn("postgres: truncate received; ignored (not part of the scalar milestone)")
	default:
		// Type, Origin, LogicalMessage and friends: not row data.
	}
	return nil
}

func (r *Reader) handleRelation(payload []byte) error {
	msg := &pglogrepl.RelationMessage{}
	if err := msg.Decode(payload); err != nil {
		return fmt.Errorf("postgres: relation: %w", err)
	}
	src := msg.Namespace + "." + msg.RelationName
	ref, ok := r.bySrc[src]
	if !ok {
		return nil // not ours; publication membership filters already
	}
	return r.bindRelation(msg.RelationID, src, ref)
}

func (r *Reader) bindRelation(relID uint32, src string, ref dblog.TableRef) error {
	st, ok := r.states[src]
	if !ok {
		return fmt.Errorf("postgres: relation %s: no introspected state", src)
	}
	r.relByID[relID] = relEntry{state: st, ref: ref}
	return nil
}

func (r *Reader) handleInsert(payload []byte) error {
	msg := &pglogrepl.InsertMessage{}
	if err := msg.Decode(payload); err != nil {
		return fmt.Errorf("postgres: insert: %w", err)
	}
	entry, ok := r.relByID[msg.RelationID]
	if !ok {
		return nil
	}
	row, err := tupleToMap(entry.state, msg.Tuple, nil)
	if err != nil {
		return err
	}
	r.enqueue(entry, change.OpInsert, row, nil)
	return nil
}

func (r *Reader) handleUpdate(payload []byte) error {
	msg := &pglogrepl.UpdateMessage{}
	if err := msg.Decode(payload); err != nil {
		return fmt.Errorf("postgres: update: %w", err)
	}
	entry, ok := r.relByID[msg.RelationID]
	if !ok {
		return nil
	}
	row, err := tupleToMap(entry.state, msg.NewTuple, msg.OldTuple)
	if err != nil {
		return err
	}
	var before map[string]any
	if msg.OldTuple != nil {
		before, err = tupleToMap(entry.state, msg.OldTuple, nil)
		if err != nil {
			return err
		}
	}
	r.enqueue(entry, change.OpUpdate, row, before)
	return nil
}

func (r *Reader) handleDelete(payload []byte) error {
	msg := &pglogrepl.DeleteMessage{}
	if err := msg.Decode(payload); err != nil {
		return fmt.Errorf("postgres: delete: %w", err)
	}
	entry, ok := r.relByID[msg.RelationID]
	if !ok {
		return nil
	}
	// OldTuple holds either the full old row (REPLICA IDENTITY FULL) or
	// just the identity key ('K') — both decode the same way.
	if msg.OldTuple == nil {
		return fmt.Errorf("postgres: delete %s: no old tuple", entry.ref.Source)
	}
	before, err := tupleToMap(entry.state, msg.OldTuple, nil)
	if err != nil {
		return err
	}
	r.enqueue(entry, change.OpDelete, nil, before)
	return nil
}

// enqueue decodes one row change into the transaction buffer. The window
// tag is applied here — at decode time — while the commit position is
// only stamped at Commit, when the LSN is known.
func (r *Reader) enqueue(entry relEntry, op change.Op, after, before map[string]any) {
	c := change.Change{
		Op:    op,
		Table: entry.ref.Target,
	}
	switch op {
	case change.OpDelete:
		c.Before = before
		c.Key = keyFrom(entry.state, entry.ref, before)
	default:
		c.After = after
		c.Key = keyFrom(entry.state, entry.ref, after)
		if before != nil {
			c.Before = before
		}
	}
	if w := r.currentWindow(); w != nil {
		c.Window = w
	}
	r.txn = append(r.txn, &c)
}

// handleCommit flushes the transaction buffer: every buffered row is
// stamped with the commit LSN and handed to the channel, and only then
// does the synced position advance.
func (r *Reader) handleCommit(ctx context.Context, endLSN pglogrepl.LSN) error {
	pos := position.LSN(endLSN)
	for _, c := range r.txn {
		c.Position = pos.String()
		select {
		case r.out <- *c:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	r.txn = r.txn[:0]

	r.mu.Lock()
	r.synced = &pos
	r.mu.Unlock()

	return r.sendStandby(ctx)
}

// advanceSyncedFloor raises the synced position to lsn while no
// transaction is buffering. Mid-transaction the floor must wait: releasing
// a window before the open transaction commits would be unsound.
func (r *Reader) advanceSyncedFloor(lsn pglogrepl.LSN) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.txn) > 0 {
		return
	}
	if position.LSN(lsn) > *r.synced {
		next := position.LSN(lsn)
		r.synced = &next
	}
}

// sendStandby reports the applied LSN, advancing the slot's confirmed
// point server-side.
func (r *Reader) sendStandby(ctx context.Context) error {
	r.mu.Lock()
	cur := *r.synced
	r.mu.Unlock()
	return pglogrepl.SendStandbyStatusUpdate(ctx, r.conn.PgConn(), pglogrepl.StandbyStatusUpdate{
		WALWritePosition: pglogrepl.LSN(cur),
	})
}

func (r *Reader) currentWindow() *change.Window {
	r.winMu.Lock()
	defer r.winMu.Unlock()
	if !r.winOpen {
		return nil
	}
	return &change.Window{ChunkID: r.winChunk, InWindow: true}
}

// keyFrom builds the key tuple from the PK columns, in spec order.
func keyFrom(st *TableState, ref dblog.TableRef, row map[string]any) []any {
	key := make([]any, 0, len(ref.PrimaryKey))
	for _, pk := range ref.PrimaryKey {
		if v, ok := row[pk]; ok {
			key = append(key, v)
		} else {
			key = append(key, nil)
		}
	}
	return key
}

func splitSource(s string) (schema, table string, ok bool) {
	return strings.Cut(s, ".")
}

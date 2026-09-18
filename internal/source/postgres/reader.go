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

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	pglogrepl "github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
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
	ConnCfg  *ConnConfig // resolved from nested config (nil when using URI)
	DB       *sql.DB
	SlotName string
	Tables   []source.TableRef
	Logger   *slog.Logger
	// RetryCount is the transient-error reconnect budget for the
	// replication stream (#166). 0 disables reconnect.
	RetryCount int
	// Filters and Columns are the per-source read projection (#162/#163),
	// keyed by "schema.table". The reader compiles the filter with the
	// introspected column types, so numeric columns compare numerically.
	Filters map[string]*spec.Filter
	Columns map[string][]string
}

// Projection is a table's source-side read projection: the columns to emit
// and the compiled filter a row must satisfy. Both are applied on the
// decoded map, before the Arrow hot-path.
type Projection struct {
	Columns []string
	program *vm.Program
}

// newProjection builds a projection, compiling the structured filter (#163)
// to an expr program once per table, using the introspected column types.
func newProjection(columns []string, f *spec.Filter, st *TableState) (Projection, error) {
	p := Projection{Columns: columns}
	prog, err := compileFilterExpr(f, st)
	if err != nil {
		return Projection{}, err
	}
	p.program = prog
	return p, nil
}

// hasFilter reports whether a filter is configured.
func (p Projection) hasFilter() bool { return p.program != nil }

// keep reports whether a full decoded row satisfies the filter. The filter is
// evaluated on the FULL row (its columns may be excluded from the
// projection).
func (p Projection) keep(full map[string]any) (bool, error) {
	if p.program == nil {
		return true, nil
	}
	out, err := expr.Run(p.program, map[string]any{"row": full})
	if err != nil {
		return false, fmt.Errorf("postgres: filter: %w", err)
	}
	ok, isBool := out.(bool)
	if !isBool {
		return false, fmt.Errorf("postgres: filter: non-bool result %T", out)
	}
	return ok, nil
}

// project narrows a full decoded row to the selected columns. An empty
// projection returns the row unchanged.
func (p Projection) project(full map[string]any) map[string]any {
	if len(p.Columns) == 0 {
		return full
	}
	out := make(map[string]any, len(p.Columns))
	for _, c := range p.Columns {
		out[c] = full[c]
	}
	return out
}

// relEntry binds a pgoutput relation id to its introspected state and the
// pipeline's target mapping.
type relEntry struct {
	state *TableState
	ref   source.TableRef
	proj  Projection
}

// Reader wraps one logical-decoding connection and decodes pgoutput row
// changes into rowchange.Change, positioned at their commit LSN.
type Reader struct {
	cfg     Config
	db      *sql.DB
	conn    *pgx.Conn
	connCfg *pgx.ConnConfig // owned by the reader; reused to reconnect
	retries int             // transient-error reconnect budget (#166)
	out     chan<- rowchange.Change
	bySrc   map[string]source.TableRef // "schema.table" → ref (PK + target)
	states  map[string]*TableState     // "schema.table" → introspected state
	relByID map[uint32]relEntry        // relation id → state, from Relation messages
	// projections is the per-source read projection (#162/#163).
	projections map[string]Projection

	// Transaction buffer: rows stream inside a transaction before its
	// commit LSN is known, so they accumulate and flush at Commit.
	txn []*rowchange.Change

	// curLSN is the current transaction's final (commit) LSN, from the Begin
	// message. The DBLog window uses it to tag only transactions committed
	// after the low watermark. Loop-goroutine only, like txn.
	curLSN position.LSN

	mu     sync.Mutex
	synced *position.LSN // end LSN of the last committed transaction

	// confirmed is set by the pipeline: the minimum position committed to
	// the sink across every table this reader feeds. sendStandby uses it
	// instead of synced to avoid advancing the slot past uncommitted data.
	confirmed func() position.Position

	winMu    sync.Mutex
	winChunk uint32
	winOpen  bool
	winLow   *position.LSN // source watermark (pg_current_wal_lsn) at OpenWindow

	// loopCancel stops the replication loop; loopDone tells Close it has
	// fully left pgx.
	loopCancel context.CancelFunc
	loopDone   chan struct{}
}

// New introspects the tables, performs the server-side setup (replica
// identity, publication, slot), and opens the replication connection —
// but does not start streaming.
func New(ctx context.Context, cfg Config, out chan<- rowchange.Change) (*Reader, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.DB == nil {
		return nil, fmt.Errorf("postgres: reader: query db required")
	}
	if err := EnsureSetup(ctx, cfg.DB, cfg.SlotName, cfg.Tables); err != nil {
		return nil, err
	}

	bySrc := make(map[string]source.TableRef, len(cfg.Tables))
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

	// Compile the per-table read projection with the introspected column
	// types, so a numeric column compares numerically.
	projections := make(map[string]Projection, len(cfg.Tables))
	for _, ref := range cfg.Tables {
		cols := cfg.Columns[ref.Source]
		f := cfg.Filters[ref.Source]
		if len(cols) == 0 && f == nil {
			continue
		}
		p, perr := newProjection(cols, f, states[ref.Source])
		if perr != nil {
			return nil, fmt.Errorf("postgres: reader: projection %s: %w", ref.Source, perr)
		}
		projections[ref.Source] = p
	}

	var connCfg *pgx.ConnConfig
	if cfg.ConnCfg != nil && cfg.ConnCfg.ConnConfig != nil {
		// Deep-copy before mutating: the source's ConnConfig is shared with
		// the query connector (stdlib.GetConnector keeps a shallow copy), so
		// writing "replication" into the shared RuntimeParams map would leak
		// the replication mode into every query connection — and pgx rejects
		// the extended protocol on a replication connection.
		connCfg = cfg.ConnCfg.ConnConfig.Copy()
	} else {
		var err error
		connCfg, err = pgx.ParseConfig(cfg.URI)
		if err != nil {
			return nil, fmt.Errorf("postgres: parse uri: %w", err)
		}
	}
	// Logical replication speaks a variant protocol; the server must know
	// this connection is a replication one.
	connCfg.RuntimeParams["replication"] = "database"
	conn, err := pgx.ConnectConfig(ctx, connCfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: replication connect: %w", err)
	}

	return &Reader{
		cfg:         cfg,
		db:          cfg.DB,
		conn:        conn,
		connCfg:     connCfg,
		retries:     cfg.RetryCount,
		out:         out,
		bySrc:       bySrc,
		states:      states,
		relByID:     map[uint32]relEntry{},
		projections: projections,
		synced:      position.MustLSN("0/0"),
	}, nil
}

// OpenWindow opens the DBLog window for chunkID. The reader captures its
// source watermark — the server's current WAL position — and, from now on,
// tags decoded events whose commit LSN is strictly past that watermark
// InWindow for the chunk, until ClearWindow. Transactions at or before the
// watermark are already reflected in the chunk SELECT (or an earlier chunk)
// and must not be tagged. The tag is applied synchronously at decode — no
// event can escape the window by racing a channel pull. The watermark is an
// LSN (64-bit, monotonic), never the 32-bit transaction xid — the pgoutput
// Begin message only carries the raw xid, which wraps every ~4 billion
// transactions; the LSN does not.
func (r *Reader) OpenWindow(ctx context.Context, chunkID uint32) {
	var low *position.LSN
	if m, err := r.Master(ctx); err == nil {
		if lsn, ok := m.(*position.LSN); ok {
			low = lsn
		}
	} else {
		r.cfg.Logger.Warn("postgres: window open without lsn watermark", "err", err)
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

// Synced reports the end LSN of the last committed transaction.
func (r *Reader) Synced() position.Position {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.synced
}

// Master reports the server's current WAL write position (the caught-up
// target for the DBLog window).
func (r *Reader) Master(ctx context.Context) (position.Position, error) {
	var raw string
	if err := r.db.QueryRowContext(ctx,
		`SELECT pg_current_wal_lsn()::text`).Scan(&raw); err != nil {
		return nil, fmt.Errorf("postgres: current wal lsn: %w", err)
	}
	return position.ParseLSN(raw)
}

// StartFromLSN begins streaming from the given LSN, blocking until the
// stream ends or ctx is cancelled. Call in a goroutine. An LSN of 0/0
// starts at the slot's confirmed point.
//
// A transient stream failure (network blip, 08xxx/53xxx, admin shutdown) is
// retried: the replication connection is re-dialed and the stream resumes
// from the last position committed to the sink, up to r.retries times
// (#166). A permanent failure is returned immediately.
func (r *Reader) StartFromLSN(ctx context.Context, at *position.LSN) error {
	// The loop runs on its own cancelable ctx: Close stops it and waits
	// for loopDone, so the conn is never closed mid-call from two
	// goroutines.
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	r.mu.Lock()
	r.loopCancel, r.loopDone = cancel, done
	r.mu.Unlock()
	defer func() {
		cancel()
		close(done)
	}()

	start := at
	if start == nil {
		start = position.MustLSN("0/0")
	}
	r.cfg.Logger.Info("reader start", "from", start.String())

	// New already opened a live connection, so the first pass streams
	// directly. needConnect is set after a stream loss: the next pass
	// redials first. A failed redial consumes an attempt and is retried
	// with backoff, so a sustained outage is tolerated within the budget
	// instead of ending the stream on the first failed redial.
	needConnect := false
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if needConnect {
			if derr := r.dialReplication(ctx); derr != nil {
				if !isTransient(derr) || attempt >= r.retries {
					return derr
				}
				r.cfg.Logger.Warn("postgres: replication reconnect failed",
					"attempt", attempt+1, "err", derr)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(retryBackoff(attempt)):
				}
				continue
			}
		}

		err := r.runReplication(ctx, start)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !isTransient(err) || attempt >= r.retries {
			return err
		}
		r.cfg.Logger.Warn("postgres: replication stream lost, reconnecting",
			"attempt", attempt+1, "from", r.Synced().String(), "err", err)
		// Resume from the last position committed to the sink. At worst
		// this replays a transaction, which the idempotent commit absorbs;
		// it never skips data. With no commit yet, 0/0 lets the slot pick
		// its confirmed_flush.
		if p := r.confirmedLSN(); p != 0 {
			start = &p
		} else {
			start = position.MustLSN("0/0")
		}
		needConnect = true
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryBackoff(attempt)):
		}
	}
}

// runReplication opens the logical replication stream and consumes it until
// the stream ends or ctx is cancelled. It returns the terminating error;
// StartFromLSN decides whether to reconnect.
func (r *Reader) runReplication(ctx context.Context, start *position.LSN) error {
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
			// Wrap the PgError so isTransient can classify its SQLSTATE
			// and decide whether a reconnect is warranted.
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) {
				return fmt.Errorf("postgres: stream error %s: %w", pgErr.Code, pgErr)
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

// dialReplication closes the (dead) replication connection and dials a fresh
// one. The in-flight transaction buffer is dropped: the connection died
// before its commit, so those rows were never committed and must not be
// flushed. Runs only in the StartFromLSN goroutine.
func (r *Reader) dialReplication(ctx context.Context) error {
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = r.conn.Close(closeCtx)
	cancel()

	conn, err := pgx.ConnectConfig(ctx, r.connCfg)
	if err != nil {
		return fmt.Errorf("postgres: replication reconnect: %w", err)
	}
	r.conn = conn
	r.txn = r.txn[:0]
	return nil
}

// Close stops the reader and its replication connection. It cancels the
// replication loop first and waits for it to exit pgx, so the conn close
// never races an in-flight receive.
func (r *Reader) Close() {
	r.mu.Lock()
	cancel, done := r.loopCancel, r.loopDone
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		select {
		case <-r.loopDone:
		case <-time.After(5 * time.Second):
			// The loop is stuck outside pgx (e.g. a full out channel);
			// closing the conn below is safe from it.
		}
	}
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
		begin := &pglogrepl.BeginMessage{}
		if err := begin.Decode(body); err != nil {
			return fmt.Errorf("postgres: begin: %w", err)
		}
		r.txn = r.txn[:0]
		r.curLSN = position.LSN(begin.FinalLSN)
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
		commit := &pglogrepl.CommitMessage{}
		if err := commit.Decode(body); err != nil {
			return fmt.Errorf("postgres: commit: %w", err)
		}
		// CommitLSN is the authoritative end of the transaction. The
		// transport LSN (WALStart + len(WALData)) is only the end of the
		// bytes received — using it as the commit position makes the
		// synced watermark drift from pg_current_wal_lsn, which the
		// caught-up proof compares against.
		return r.handleCommit(ctx, commit.CommitLSN)
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

func (r *Reader) bindRelation(relID uint32, src string, ref source.TableRef) error {
	st, ok := r.states[src]
	if !ok {
		return fmt.Errorf("postgres: relation %s: no introspected state", src)
	}
	r.relByID[relID] = relEntry{state: st, ref: ref, proj: r.projections[src]}
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
	keep, err := entry.proj.keep(row)
	if err != nil {
		return err
	}
	if !keep {
		return nil
	}
	r.enqueue(entry, rowchange.OpInsert, entry.proj.project(row), nil)
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

	if entry.proj.hasFilter() {
		afterMatch, err := entry.proj.keep(row)
		if err != nil {
			return err
		}
		beforeMatch := false
		if before != nil {
			beforeMatch, err = entry.proj.keep(before)
			if err != nil {
				return err
			}
		}
		switch {
		case !beforeMatch && !afterMatch:
			// The row is outside the filter before and after: nothing to do.
			return nil
		case beforeMatch && !afterMatch:
			// The row left the filter: emit a delete so an upsert target
			// removes the now-excluded row instead of keeping a stale copy.
			r.enqueue(entry, rowchange.OpDelete, nil, entry.proj.project(before))
			return nil
		}
	}
	r.enqueue(entry, rowchange.OpUpdate, entry.proj.project(row), entry.proj.project(before))
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
	keep, err := entry.proj.keep(before)
	if err != nil {
		return err
	}
	if !keep {
		return nil
	}
	r.enqueue(entry, rowchange.OpDelete, nil, entry.proj.project(before))
	return nil
}

// enqueue decodes one row change into the transaction buffer. The window
// tag is applied here — at decode time — while the commit position is
// only stamped at Commit, when the LSN is known.
func (r *Reader) enqueue(entry relEntry, op rowchange.Op, after, before map[string]any) {
	c := rowchange.Change{
		Op:       op,
		Table:    entry.ref.Target,
		IngestTS: time.Now(),
	}
	switch op {
	case rowchange.OpDelete:
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
// point server-side. It reports the confirmed (committed) position, not
// the decoded position, to prevent Postgres from discarding WAL for
// events still in flight to the sink.
func (r *Reader) sendStandby(ctx context.Context) error {
	cur := r.confirmedLSN()
	return pglogrepl.SendStandbyStatusUpdate(ctx, r.conn.PgConn(), pglogrepl.StandbyStatusUpdate{
		WALWritePosition: pglogrepl.LSN(cur),
	})
}

// SetConfirmed installs the confirmed-position callback.
func (r *Reader) SetConfirmed(f func() position.Position) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.confirmed = f
}

// confirmedLSN returns the minimum committed position across all tables,
// or 0/0 if no commit has happened yet. Returning 0 means "don't advance"
// — Postgres ignores retrograde updates.
func (r *Reader) confirmedLSN() position.LSN {
	r.mu.Lock()
	f := r.confirmed
	r.mu.Unlock()
	if f == nil {
		return 0
	}
	p := f()
	if p == nil {
		return 0
	}
	lsn, ok := p.(*position.LSN)
	if !ok || lsn == nil {
		return 0
	}
	return *lsn
}

func (r *Reader) currentWindow() *rowchange.Window {
	r.winMu.Lock()
	defer r.winMu.Unlock()
	if !r.winOpen {
		return nil
	}
	// Only transactions committed past the low watermark's LSN are
	// InWindow: an older transaction is already reflected in the chunk
	// SELECT (or an earlier chunk), so tagging it would resurrect a stale
	// value. A missing watermark falls back to tagging everything —
	// over-tagging is safe. The comparison is on the 64-bit commit LSN
	// (from the Begin message), not the 32-bit xid that wraps.
	if r.winLow != nil && r.curLSN <= *r.winLow {
		return nil
	}
	return &rowchange.Window{ChunkID: r.winChunk, InWindow: true}
}

// keyFrom builds the key tuple from the PK columns, in spec order.
func keyFrom(st *TableState, ref source.TableRef, row map[string]any) []any {
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

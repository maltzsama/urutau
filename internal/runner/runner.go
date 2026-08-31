// Package runner wires the collapsed process: one binary runs the source
// reader, the DBLog snapshot, and the worker in a single process (local
// mode). The gRPC/Flight split arrives with multi-worker support.
package runner

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	_ "github.com/go-sql-driver/mysql"

	"github.com/maltzsama/urutau/internal/change"
	"github.com/maltzsama/urutau/internal/eventlog"
	"github.com/maltzsama/urutau/internal/position"
	foziceberg "github.com/maltzsama/urutau/internal/sink/iceberg"
	"github.com/maltzsama/urutau/internal/source/dblog"
	"github.com/maltzsama/urutau/internal/source/mysql"
	"github.com/maltzsama/urutau/internal/source/postgres"
	"github.com/maltzsama/urutau/internal/spec"
	"github.com/maltzsama/urutau/internal/worker"
)

// Config carries the runtime knobs for a local run.
type Config struct {
	ServerID      uint32
	Heartbeat     time.Duration
	ChunkSize     int
	WindowTimeout time.Duration
	CaughtUpPoll  time.Duration
	MaxRows       int
	MaxInterval   time.Duration
	// Eventlog, when set, records a per-run JSONL audit trail in S3
	// (lifecycle + commit events). Nil disables the trail.
	Eventlog *eventlog.Config
	Logger   *slog.Logger
}

// Run executes the collapsed pipeline for a validated spec until ctx is
// cancelled or a terminal error occurs.
// Run builds the collapsed pipeline, runs the snapshot phase, and blocks
// until ctx is cancelled or a terminal error occurs.
func Run(ctx context.Context, s *spec.Spec, cfg Config) error {
	r, err := NewRunner(ctx, s, cfg)
	if err != nil {
		return err
	}
	return r.Run(ctx)
}

// streamSource is the replication reader surface the collapsed runner
// drives: the DBLog watermark surface plus start/stop of the stream.
type streamSource interface {
	dblog.SourceReader
	// Start begins streaming at the given position, blocking until the
	// stream ends or ctx is cancelled. Call in a goroutine.
	Start(ctx context.Context, at position.Position) error
	Close()
}

// sourceAdapter bundles the per-source pieces of the collapsed runner:
// query access, introspection, chunking, the replication reader, and the
// position codec. New sources plug in here; everything downstream —
// catalog, worker, DBLog orchestrator, audit trail — is shared.
type sourceAdapter interface {
	// OpenQuery opens the SQL connection for chunk SELECTs, schema
	// introspection, and position queries.
	OpenQuery(ctx context.Context) (*sql.DB, error)
	// Introspect resolves one spec table into its ref and Iceberg schema.
	Introspect(ctx context.Context, db *sql.DB, t spec.Table) (dblog.TableRef, *iceberg.Schema, error)
	// NewChunker builds the chunk SELECT source for one table.
	NewChunker(db *sql.DB, source, pk string, chunkSize int) (dblog.ChunkSource, error)
	// NewReader builds the replication reader over the pipeline's tables.
	NewReader(ctx context.Context, db *sql.DB, refs []dblog.TableRef, out chan<- change.Change) (streamSource, error)
	// InitialPosition is the stream start for a first boot (no resume).
	InitialPosition(ctx context.Context, db *sql.DB) (position.Position, error)
	// ParsePosition decodes a stored cdc.position string.
	ParsePosition(s string) (position.Position, error)
}

func adapterFor(s *spec.Spec, cfg Config) (sourceAdapter, error) {
	switch s.Source.Kind {
	case "mysql":
		return mysqlAdapter{spec: s, cfg: cfg}, nil
	case "postgres":
		return postgresAdapter{spec: s, cfg: cfg}, nil
	default:
		return nil, fmt.Errorf("runner: unsupported source kind %q", s.Source.Kind)
	}
}

// ── MySQL adapter ────────────────────────────────────────────────────

type mysqlAdapter struct {
	spec *spec.Spec
	cfg  Config
}

func (a mysqlAdapter) OpenQuery(ctx context.Context) (*sql.DB, error) {
	conn, err := parseMySQLURI(a.spec.Source.URI)
	if err != nil {
		return nil, err
	}
	return sql.Open("mysql", conn.queryDSN())
}

func (a mysqlAdapter) Introspect(ctx context.Context, db *sql.DB, t spec.Table) (dblog.TableRef, *iceberg.Schema, error) {
	schemaName, tableName, ok := strings.Cut(t.Source, ".")
	if !ok {
		return dblog.TableRef{}, nil, fmt.Errorf("runner: source %q must be db.table", t.Source)
	}
	st, err := mysql.QueryTable(ctx, db, schemaName, tableName)
	if err != nil {
		return dblog.TableRef{}, nil, fmt.Errorf("runner: introspect %s: %w", t.Source, err)
	}
	pk := t.PrimaryKey
	if len(pk) == 0 && len(st.PKColumns) > 0 {
		for _, idx := range st.PKColumns {
			pk = append(pk, st.Columns[idx].Name)
		}
	}
	is, err := mysql.IcebergSchema(st)
	if err != nil {
		return dblog.TableRef{}, nil, fmt.Errorf("runner: schema %s: %w", t.Source, err)
	}
	return dblog.TableRef{Source: t.Source, Target: t.Target, PrimaryKey: pk}, is, nil
}

func (a mysqlAdapter) NewChunker(db *sql.DB, source, pk string, chunkSize int) (dblog.ChunkSource, error) {
	return mysql.NewChunker(db, source, pk, chunkSize)
}

func (a mysqlAdapter) NewReader(ctx context.Context, db *sql.DB, refs []dblog.TableRef, out chan<- change.Change) (streamSource, error) {
	conn, err := parseMySQLURI(a.spec.Source.URI)
	if err != nil {
		return nil, err
	}
	rdr, err := mysql.New(ctx, mysql.Config{
		Addr:      conn.addr(),
		User:      conn.user,
		Password:  conn.password,
		ServerID:  a.cfg.ServerID,
		Heartbeat: a.cfg.Heartbeat,
		Tables:    refs,
		Logger:    a.cfg.Logger,
	}, out)
	if err != nil {
		return nil, err
	}
	return mysqlStream{Reader: rdr}, nil
}

// InitialPosition returns the master's executed GTID set — the same query
// canal's GetMasterGTIDSet issues, without spinning a replication handle.
func (a mysqlAdapter) InitialPosition(ctx context.Context, db *sql.DB) (position.Position, error) {
	var gtid string
	if err := db.QueryRowContext(ctx, `SELECT @@GLOBAL.gtid_executed`).Scan(&gtid); err != nil {
		return nil, fmt.Errorf("mysql: gtid_executed: %w", err)
	}
	return position.ParseGTID(gtid)
}

// ParsePosition decodes a stored cdc.position as a GTID set.
func (a mysqlAdapter) ParsePosition(s string) (position.Position, error) {
	return position.ParseGTID(s)
}

// mysqlStream adapts the concrete canal reader to the streamSource
// interface (its watermark surface already matches).
type mysqlStream struct {
	*mysql.Reader
}

// Start begins the stream at the given GTID set.
func (s mysqlStream) Start(ctx context.Context, at position.Position) error {
	g, ok := at.(*position.GTID)
	if !ok {
		return fmt.Errorf("mysql: start position must be a GTID set, got %T", at)
	}
	return s.StartFromGTID(ctx, g)
}

// ── PostgreSQL adapter ───────────────────────────────────────────────

type postgresAdapter struct {
	spec *spec.Spec
	cfg  Config
}

func (a postgresAdapter) OpenQuery(ctx context.Context) (*sql.DB, error) {
	return sql.Open("pgx", a.spec.Source.URI)
}

func (a postgresAdapter) Introspect(ctx context.Context, db *sql.DB, t spec.Table) (dblog.TableRef, *iceberg.Schema, error) {
	schemaName, tableName, ok := strings.Cut(t.Source, ".")
	if !ok {
		return dblog.TableRef{}, nil, fmt.Errorf("runner: source %q must be db.table", t.Source)
	}
	st, err := postgres.QueryTable(ctx, db, schemaName, tableName)
	if err != nil {
		return dblog.TableRef{}, nil, fmt.Errorf("runner: introspect %s: %w", t.Source, err)
	}
	pk := t.PrimaryKey
	if len(pk) == 0 && len(st.PKColumns) > 0 {
		for _, idx := range st.PKColumns {
			pk = append(pk, st.Columns[idx].Name)
		}
	}
	is, err := postgres.IcebergSchema(st)
	if err != nil {
		return dblog.TableRef{}, nil, fmt.Errorf("runner: schema %s: %w", t.Source, err)
	}
	return dblog.TableRef{Source: t.Source, Target: t.Target, PrimaryKey: pk}, is, nil
}

func (a postgresAdapter) NewChunker(db *sql.DB, source, pk string, chunkSize int) (dblog.ChunkSource, error) {
	return postgres.NewChunker(db, source, pk, chunkSize)
}

func (a postgresAdapter) NewReader(ctx context.Context, db *sql.DB, refs []dblog.TableRef, out chan<- change.Change) (streamSource, error) {
	slot := a.spec.Source.SlotName
	if slot == "" {
		return nil, fmt.Errorf("runner: postgres source requires slotName")
	}
	rdr, err := postgres.New(ctx, postgres.Config{
		URI:      a.spec.Source.URI,
		DB:       db,
		SlotName: slot,
		Tables:   refs,
		Logger:   a.cfg.Logger,
	}, out)
	if err != nil {
		return nil, err
	}
	return pgStream{Reader: rdr}, nil
}

// InitialPosition returns the slot's confirmed LSN: a first boot starts
// streaming from the slot's consistency point — everything after it flows
// through the stream, everything before it belongs to the snapshot.
func (a postgresAdapter) InitialPosition(ctx context.Context, db *sql.DB) (position.Position, error) {
	return postgres.ConfirmedLSN(ctx, db, a.spec.Source.SlotName)
}

// ParsePosition decodes a stored cdc.position as an LSN.
func (a postgresAdapter) ParsePosition(s string) (position.Position, error) {
	return position.ParseLSN(s)
}

// pgStream adapts the concrete pgoutput reader to the streamSource
// interface.
type pgStream struct {
	*postgres.Reader
}

// Start begins the stream at the given LSN.
func (s pgStream) Start(ctx context.Context, at position.Position) error {
	l, ok := at.(*position.LSN)
	if !ok {
		return fmt.Errorf("postgres: start position must be an LSN, got %T", at)
	}
	return s.StartFromLSN(ctx, l)
}

// ── Relay ────────────────────────────────────────────────────────────

// relay pumps reader events into the worker's ingest channel and releases
// the DBLog chunk markers. Window tagging happens in the reader at decode
// time; the marker's Release first drains the pump, so every event decoded
// inside the window is already enqueued ahead of it.
type relay struct {
	ingest   chan<- change.Change
	window   *worker.Worker
	flushReq chan chan struct{}
}

func newRelay(ingest chan<- change.Change, window *worker.Worker) *relay {
	return &relay{ingest: ingest, window: window, flushReq: make(chan chan struct{}, 1)}
}

func (r *relay) Release(table string, chunkID uint32, at position.Position) {
	req := make(chan struct{})
	r.flushReq <- req
	<-req
	r.ingest <- change.Change{
		Table:    table,
		Position: at.String(),
		Window:   &change.Window{ChunkID: chunkID, Closes: true},
	}
}

func (r *relay) AddWindowRows(target string, chunkID uint32, rows []change.Change) error {
	return r.window.AddWindowRows(target, chunkID, rows)
}

// run routes reader events into the worker's ingest channel.
func (r *relay) run(ctx context.Context, out <-chan change.Change) error {
	for {
		select {
		case c, ok := <-out:
			if !ok {
				return nil
			}
			select {
			case r.ingest <- c:
			case <-ctx.Done():
				return ctx.Err()
			}
		case req := <-r.flushReq:
			// Drain everything already decoded into ingest, then ack.
		drain:
			for {
				select {
				case c, ok := <-out:
					if !ok {
						break drain
					}
					select {
					case r.ingest <- c:
					case <-ctx.Done():
						close(req)
						return ctx.Err()
					}
				default:
					break drain
				}
			}
			close(req)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// ── Positions and catalog ────────────────────────────────────────────

func resumeFrom(ctx context.Context, cat *rest.Catalog, adapt sourceAdapter, s *spec.Spec, refs []dblog.TableRef) (position.Position, []dblog.TableRef, error) {
	var positions []position.Position
	var needsSnapshot []dblog.TableRef
	for _, ref := range refs {
		tbl, err := cat.LoadTable(ctx, targetIdent(s, ref.Target))
		if err != nil {
			return nil, nil, fmt.Errorf("runner: load %s: %w", ref.Target, err)
		}
		if pos := tbl.Properties()["cdc.position"]; pos != "" {
			p, err := adapt.ParsePosition(pos)
			if err != nil {
				return nil, nil, fmt.Errorf("runner: %s cdc.position %q: %w", ref.Target, pos, err)
			}
			positions = append(positions, p)
		} else {
			needsSnapshot = append(needsSnapshot, ref)
		}
	}
	if len(positions) == 0 {
		return nil, needsSnapshot, nil
	}
	return position.Min(positions), needsSnapshot, nil
}

func catalogConfig(s *spec.Spec) foziceberg.Config {
	return foziceberg.Config{
		URI:          s.Sink.URI,
		Warehouse:    s.Sink.Warehouse,
		ClientID:     s.Sink.ClientID,
		ClientSecret: s.Sink.ClientSecret,
		Scope:        s.Sink.Scope,
	}
}

func targetIdent(s *spec.Spec, target string) table.Identifier {
	if ns, name, ok := strings.Cut(target, "."); ok {
		return table.Identifier{ns, name}
	}
	return table.Identifier{s.Sink.Namespace, target}
}

// introspectAll resolves each spec table through the adapter, so the
// pipeline knows the PK (equality key) and the target shape before writing
// anything.
func introspectAll(ctx context.Context, adapt sourceAdapter, qdb *sql.DB, s *spec.Spec) ([]dblog.TableRef, map[string]*iceberg.Schema, error) {
	refs := make([]dblog.TableRef, 0, len(s.Tables))
	schemas := make(map[string]*iceberg.Schema, len(s.Tables))
	for _, t := range s.Tables {
		ref, is, err := adapt.Introspect(ctx, qdb, t)
		if err != nil {
			return nil, nil, err
		}
		refs = append(refs, ref)
		schemas[t.Source] = is
	}
	return refs, schemas, nil
}

// ── Collapsed pipeline ───────────────────────────────────────────────

// mysqlConn is the parsed MySQL source URI.
type mysqlConn struct {
	user, password, host, port, db string
}

func parseMySQLURI(uri string) (*mysqlConn, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("runner: source uri: %w", err)
	}
	if u.Scheme != "mysql" {
		return nil, fmt.Errorf("runner: source uri scheme %q, want mysql", u.Scheme)
	}
	c := &mysqlConn{user: u.User.Username()}
	if p, ok := u.User.Password(); ok {
		c.password = p
	}
	c.host = u.Hostname()
	if c.host == "" {
		return nil, fmt.Errorf("runner: source uri %q lacks host", uri)
	}
	if p := u.Port(); p != "" {
		c.port = p
	} else {
		c.port = "3306"
	}
	c.db = strings.TrimPrefix(u.Path, "/")
	if c.db == "" {
		return nil, fmt.Errorf("runner: source uri %q lacks /db", uri)
	}
	return c, nil
}

func (c *mysqlConn) addr() string { return c.host + ":" + c.port }

func (c *mysqlConn) queryDSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true", c.user, c.password, c.addr(), c.db)
}

func resumeOrNone(p position.Position) string {
	if p == nil {
		return "none"
	}
	return p.String()
}

// Runner wraps the collapsed pipeline and exposes metrics like
// dropped rows by window (proof of caught-up state).
type Runner struct {
	w                                *worker.Worker
	log                              *slog.Logger
	ev                               *eventlog.Run
	rdr                              streamSource
	closeQuery                       func()
	streamErr, workerErr, routerDone <-chan error
}

// emit posts one lifecycle event to the audit trail, best-effort by
// contract: a lost event is logged and the pipeline carries on.
func (r *Runner) emit(kind string, fields map[string]any) {
	if r.ev == nil {
		return
	}
	if err := r.ev.Emit(context.Background(), kind, fields); err != nil {
		r.log.Warn("eventlog: emit failed", "kind", kind, "err", err)
	}
}

// NewRunner sets up the collapsed pipeline (catalog, writers, worker,
// reader) and runs the DBLog snapshot phase for tables without a committed
// position. It returns once snapshots are done and the stream is live; Run
// then blocks until cancellation or a terminal error. Resources are owned
// by the Runner and released when Run returns.
func NewRunner(ctx context.Context, s *spec.Spec, cfg Config) (r *Runner, err error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	log := cfg.Logger

	// Audit trail first: job_started marks the boot, and a startup failure
	// still seals the trail with job_stopped.
	var ev *eventlog.Run
	if cfg.Eventlog != nil {
		e, eerr := eventlog.New(ctx, *cfg.Eventlog)
		if eerr != nil {
			return nil, eerr
		}
		ev = e
		_ = ev.Emit(ctx, eventlog.KindJobStarted, map[string]any{
			"pipeline": s.Pipeline, "source": s.Source.Kind, "tables": len(s.Tables),
		})
		defer func() {
			if r == nil {
				_ = ev.Emit(ctx, eventlog.KindJobStopped, map[string]any{"reason": "startup_failed"})
				ev.Close()
			}
		}()
	}

	adapt, err := adapterFor(s, cfg)
	if err != nil {
		return nil, err
	}

	// One query connection for chunk SELECTs, schema introspection, and the
	// position queries (caught-up proof, slot state).
	qdb, err := adapt.OpenQuery(ctx)
	if err != nil {
		return nil, fmt.Errorf("runner: open query db: %w", err)
	}

	// Resolve source tables and their Iceberg schemas.
	refs, schemas, err := introspectAll(ctx, adapt, qdb, s)
	if err != nil {
		return nil, err
	}

	// Catalog + writers, ensuring tables exist.
	cat, err := foziceberg.NewCatalog(ctx, catalogConfig(s))
	if err != nil {
		return nil, fmt.Errorf("runner: catalog: %w", err)
	}
	if err := foziceberg.EnsureNamespace(ctx, cat, table.Identifier{s.Sink.Namespace}); err != nil {
		return nil, err
	}

	writers := make(map[string]*foziceberg.TableWriter, len(refs))
	for _, ref := range refs {
		ident := targetIdent(s, ref.Target)
		if err := foziceberg.EnsureTable(ctx, cat, ident, schemas[ref.Source]); err != nil {
			return nil, fmt.Errorf("runner: ensure %s: %w", ref.Target, err)
		}
		wr, err := foziceberg.NewTableWriter(ctx, cat, ident, ref.PrimaryKey)
		if err != nil {
			return nil, fmt.Errorf("runner: writer %s: %w", ref.Target, err)
		}
		writers[ref.Target] = wr
	}

	// Worker + ingest channel.
	ingest := make(chan change.Change, 1024)
	w := worker.New(worker.Config{MaxRows: cfg.MaxRows, MaxInterval: cfg.MaxInterval})
	for target, wr := range writers {
		w.Register(target, wr)
	}
	r = &Runner{w: w, log: log, ev: ev, closeQuery: func() { _ = qdb.Close() }}
	w.OnCommit(func(b change.Batch, rows int) {
		log.Info("commit", "table", b.Table, "rows", rows,
			"upserts", len(b.Upserts), "deletes", len(b.Deletes), "position", b.Position)
		r.emit(eventlog.KindCommit, map[string]any{
			"table": b.Table, "rows": rows,
			"upserts": len(b.Upserts), "deletes": len(b.Deletes), "position": b.Position,
		})
	})
	workerErr := make(chan error, 1)
	go func() { workerErr <- w.Run(ctx, ingest) }()

	resume, needsSnapshot, err := resumeFrom(ctx, cat, adapt, s, refs)
	if err != nil {
		_ = qdb.Close()
		return nil, err
	}
	log.Info("resume", "from", resumeOrNone(resume), "snapshot_tables", len(needsSnapshot))
	r.emit(eventlog.KindResume, map[string]any{
		"from": resumeOrNone(resume), "snapshot_tables": len(needsSnapshot),
	})

	// Reader (one replication connection) → relay → ingest. The reader
	// constructor also performs the source's server-side setup (Postgres
	// slot and publication).
	out := make(chan change.Change, 1024)
	rdr, err := adapt.NewReader(ctx, qdb, refs, out)
	if err != nil {
		_ = qdb.Close()
		return nil, err
	}
	r.rdr = rdr

	router := newRelay(ingest, w)
	routerDone := make(chan error, 1)
	go func() { routerDone <- router.run(ctx, out) }()

	start := resume
	if start == nil {
		if m, err := adapt.InitialPosition(ctx, qdb); err != nil {
			return nil, fmt.Errorf("runner: initial position: %w", err)
		} else {
			start = m
		}
	}

	streamErr := make(chan error, 1)
	go func() { streamErr <- rdr.Start(ctx, start) }()

	// Snapshot phase: DBLog for tables with no committed position.
	for _, ref := range needsSnapshot {
		log.Info("snapshot", "table", ref.Source)
		r.emit(eventlog.KindSnapshotStarted, map[string]any{"table": ref.Source, "target": ref.Target})
		chunker, err := adapt.NewChunker(qdb, ref.Source, strings.Join(ref.PrimaryKey, ","), cfg.ChunkSize)
		if err != nil {
			rdr.Close()
			_ = qdb.Close()
			return nil, err
		}
		if err := dblog.SnapshotTable(ctx, chunker, rdr, router, ref.Target, dblog.SnapshotConfig{
			WindowTimeout: cfg.WindowTimeout,
			CaughtUpPoll:  cfg.CaughtUpPoll,
		}); err != nil {
			rdr.Close()
			_ = qdb.Close()
			return nil, fmt.Errorf("runner: snapshot %s: %w", ref.Source, err)
		}
		log.Info("snapshot done", "table", ref.Source)
		r.emit(eventlog.KindSnapshotDone, map[string]any{"table": ref.Source, "target": ref.Target})
	}

	r.streamErr = streamErr
	r.workerErr = workerErr
	r.routerDone = routerDone

	return r, nil
}

// Run blocks until ctx is cancelled or a terminal error surfaces, then
// releases the pipeline resources and seals the audit trail.
func (r *Runner) Run(ctx context.Context) error {
	err := r.run(ctx)
	if r.ev != nil {
		reason := "error"
		if errors.Is(err, context.Canceled) {
			reason = "cancelled"
		}
		_ = r.ev.Emit(context.Background(), eventlog.KindJobStopped,
			map[string]any{"reason": reason})
		r.ev.Close()
	}
	r.rdr.Close()
	r.closeQuery()
	return err
}

func (r *Runner) run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-r.streamErr:
			return fmt.Errorf("runner: stream: %w", err)
		case err := <-r.workerErr:
			return fmt.Errorf("runner: worker: %w", err)
		case err := <-r.routerDone:
			return fmt.Errorf("runner: router: %w", err)
		}
	}
}

// DroppedByWindow returns the number of rows dropped by the window for a
// target table (proof of caught-up state).
func (r *Runner) DroppedByWindow(target string) int64 {
	return r.w.DroppedByWindow(target)
}

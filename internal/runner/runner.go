// Package runner wires the collapsed process: one binary runs the source
// reader, the DBLog snapshot, and the worker in a single process (local
// mode). The gRPC/Flight split arrives with multi-worker support.
package runner

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	_ "github.com/go-sql-driver/mysql"

	"github.com/maltzsama/urutau/internal/change"
	"github.com/maltzsama/urutau/internal/position"
	foziceberg "github.com/maltzsama/urutau/internal/sink/iceberg"
	"github.com/maltzsama/urutau/internal/source/mysql"
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
	Logger        *slog.Logger
}

// Run executes the collapsed pipeline for a validated spec until ctx is
// cancelled or a terminal error occurs.
func Run(ctx context.Context, s *spec.Spec, cfg Config) error {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	log := cfg.Logger

	conn, err := parseMySQLURI(s.Source.URI)
	if err != nil {
		return err
	}

	// One query connection for chunk SELECTs, schema introspection, and the
	// master position (caught-up proof).
	qdb, err := sql.Open("mysql", conn.queryDSN())
	if err != nil {
		return fmt.Errorf("runner: open query db: %w", err)
	}
	defer func() { _ = qdb.Close() }()

	// Resolve source tables and their Iceberg schemas.
	refs, schemas, err := resolveTables(ctx, qdb, s)
	if err != nil {
		return err
	}

	// Catalog + writers, ensuring tables exist.
	cat, err := foziceberg.NewCatalog(ctx, catalogConfig(s))
	if err != nil {
		return fmt.Errorf("runner: catalog: %w", err)
	}
	if err := foziceberg.EnsureNamespace(ctx, cat, table.Identifier{s.Sink.Namespace}); err != nil {
		return err
	}

	writers := make(map[string]*foziceberg.TableWriter, len(refs))
	for _, ref := range refs {
		ident := targetIdent(s, ref.Target)
		if err := foziceberg.EnsureTable(ctx, cat, ident, schemas[ref.Source]); err != nil {
			return fmt.Errorf("runner: ensure %s: %w", ref.Target, err)
		}
		wr, err := foziceberg.NewTableWriter(ctx, cat, ident, ref.PrimaryKey)
		if err != nil {
			return fmt.Errorf("runner: writer %s: %w", ref.Target, err)
		}
		writers[ref.Target] = wr
	}

	// Worker + ingest channel.
	ingest := make(chan change.Change, 1024)
	w := worker.New(worker.Config{MaxRows: cfg.MaxRows, MaxInterval: cfg.MaxInterval})
	for target, wr := range writers {
		w.Register(target, wr)
	}
	w.OnCommit(func(b change.Batch, rows int) {
		log.Info("commit", "table", b.Table, "rows", rows,
			"upserts", len(b.Upserts), "deletes", len(b.Deletes), "position", b.Position)
	})
	workerErr := make(chan error, 1)
	go func() { workerErr <- w.Run(ctx, ingest) }()

	// Resume positions: read cdc.position from the Iceberg table property.
	resume, needsSnapshot, err := resumeFrom(ctx, cat, s, refs)
	if err != nil {
		return err
	}
	log.Info("resume", "from", resumeOrNone(resume), "snapshot_tables", len(needsSnapshot))

	// Reader (one replication connection) → relay → ingest.
	out := make(chan change.Change, 1024)
	rdr, err := mysql.New(ctx, mysql.Config{
		Addr:      conn.addr(),
		User:      conn.user,
		Password:  conn.password,
		ServerID:  cfg.ServerID,
		Heartbeat: cfg.Heartbeat,
		Tables:    refs,
		Logger:    log,
	}, out)
	if err != nil {
		return err
	}
	defer rdr.Close()

	router := &relay{ingest: ingest, window: w, buffers: map[string]*[]change.Change{}}
	routerDone := make(chan error, 1)
	go func() { routerDone <- router.run(ctx, out) }()

	start := resume
	if start == nil {
		if m, err := rdr.Master(); err != nil {
			return fmt.Errorf("runner: master position: %w", err)
		} else {
			start = m
		}
	}

	streamErr := make(chan error, 1)
	go func() { streamErr <- rdr.StartFromGTID(ctx, start) }()

	// Snapshot phase: DBLog for tables with no committed position.
	for _, ref := range needsSnapshot {
		log.Info("snapshot", "table", ref.Source)
		chunker, err := mysql.NewChunker(qdb, ref.Source, strings.Join(ref.PrimaryKey, ","), cfg.ChunkSize)
		if err != nil {
			return err
		}
		if err := mysql.SnapshotTable(ctx, chunker, rdr, router, ref.Target, mysql.SnapshotConfig{
			WindowTimeout: cfg.WindowTimeout,
			CaughtUpPoll:  cfg.CaughtUpPoll,
		}); err != nil {
			return fmt.Errorf("runner: snapshot %s: %w", ref.Source, err)
		}
		log.Info("snapshot done", "table", ref.Source)
	}

	// Stream: block until cancelled or a terminal error surfaces.
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-streamErr:
			return fmt.Errorf("runner: stream: %w", err)
		case err := <-workerErr:
			return fmt.Errorf("runner: worker: %w", err)
		case err := <-routerDone:
			return fmt.Errorf("runner: router: %w", err)
		}
	}
}

// relay implements mysql.Relay: buffers a table's events during a snapshot
// window, forwards window rows to the worker's window, and releases the
// buffered events tagged InWindow plus the Closes marker after caught-up.
type relay struct {
	ingest  chan<- change.Change
	window  *worker.Worker
	mu      sync.Mutex
	buffers map[string]*[]change.Change
}

func (r *relay) Ingest() chan<- change.Change { return r.ingest }

func (r *relay) Buffer(table string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	buf := make([]change.Change, 0)
	r.buffers[table] = &buf
}

func (r *relay) Release(table string, chunkID uint32, at *position.GTID) {
	r.mu.Lock()
	buf := r.buffers[table]
	delete(r.buffers, table)
	r.mu.Unlock()

	for i := range *buf {
		(*buf)[i].Window = &change.Window{ChunkID: chunkID, InWindow: true}
	}
	for _, c := range *buf {
		r.ingest <- c
	}
	r.ingest <- change.Change{
		Table:    table,
		Position: at.String(),
		Window:   &change.Window{ChunkID: chunkID, Closes: true},
	}
}

func (r *relay) AddWindowRows(target string, chunkID uint32, rows []change.Change) error {
	return r.window.AddWindowRows(target, chunkID, rows)
}

// run routes reader events, buffering tables that are mid-snapshot.
func (r *relay) run(ctx context.Context, out <-chan change.Change) error {
	for {
		select {
		case c, ok := <-out:
			if !ok {
				return nil
			}
			r.mu.Lock()
			buf, buffering := r.buffers[c.Table]
			r.mu.Unlock()
			if buffering {
				*buf = append(*buf, c)
				continue
			}
			select {
			case r.ingest <- c:
			case <-ctx.Done():
				return ctx.Err()
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func resumeFrom(ctx context.Context, cat *rest.Catalog, s *spec.Spec, refs []mysql.TableRef) (*position.GTID, []mysql.TableRef, error) {
	var positions []position.Position
	var needsSnapshot []mysql.TableRef
	for _, ref := range refs {
		tbl, err := cat.LoadTable(ctx, targetIdent(s, ref.Target))
		if err != nil {
			return nil, nil, fmt.Errorf("runner: load %s: %w", ref.Target, err)
		}
		if pos := tbl.Properties()["cdc.position"]; pos != "" {
			g, err := position.ParseGTID(pos)
			if err != nil {
				return nil, nil, fmt.Errorf("runner: %s cdc.position %q: %w", ref.Target, pos, err)
			}
			positions = append(positions, g)
		} else {
			needsSnapshot = append(needsSnapshot, ref)
		}
	}
	if len(positions) == 0 {
		return nil, needsSnapshot, nil
	}
	return position.Min(positions).(*position.GTID), needsSnapshot, nil
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

// resolveTables introspects each source table and derives its Iceberg
// schema, so the pipeline knows the PK (equality key) and the target shape
// before writing anything.
func resolveTables(ctx context.Context, qdb *sql.DB, s *spec.Spec) ([]mysql.TableRef, map[string]*iceberg.Schema, error) {
	refs := make([]mysql.TableRef, 0, len(s.Tables))
	schemas := make(map[string]*iceberg.Schema, len(s.Tables))
	for _, t := range s.Tables {
		schemaName, tableName, ok := strings.Cut(t.Source, ".")
		if !ok {
			return nil, nil, fmt.Errorf("runner: source %q must be db.table", t.Source)
		}
		st, err := mysql.QueryTable(ctx, qdb, schemaName, tableName)
		if err != nil {
			return nil, nil, fmt.Errorf("runner: introspect %s: %w", t.Source, err)
		}
		pk := t.PrimaryKey
		if len(pk) == 0 && len(st.PKColumns) > 0 {
			for _, idx := range st.PKColumns {
				pk = append(pk, st.Columns[idx].Name)
			}
		}
		is, err := mysql.IcebergSchema(st)
		if err != nil {
			return nil, nil, fmt.Errorf("runner: schema %s: %w", t.Source, err)
		}
		refs = append(refs, mysql.TableRef{Source: t.Source, Target: t.Target, PrimaryKey: pk})
		schemas[t.Source] = is
	}
	return refs, schemas, nil
}

// mysqlConn is the parsed source URI.
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

func resumeOrNone(p *position.GTID) string {
	if p == nil {
		return "none"
	}
	return p.String()
}

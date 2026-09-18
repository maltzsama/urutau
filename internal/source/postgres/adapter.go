package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/stdlib"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/driver"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/sourcepull"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// Source adapts the Postgres pgoutput reader to the source contract. It is
// the self-contained driver entry point; init() registers the kind.
type Source struct {
	spec    *spec.Spec
	rt      source.Runtime
	db      *sql.DB     // the source's query connection (chunk SELECTs, introspection)
	connCfg *ConnConfig // resolved connection config (nil when using URI)
}

func capabilities() source.Capabilities {
	return source.Capabilities{
		Snapshot:            true,
		ChunkQuery:          true,
		Stream:              true,
		MaxConnections:      10,
		Modes:               []source.Mode{source.ModeCDC},
		BeforeImage:         true,  // old tuple carries the deleted row (PK-only unless REPLICA IDENTITY FULL)
		MonotonicSequence:   false, // commit LSNs are monotonic but not per-message coordinates
		RecoverablePosition: true,  // LSN allows exact resume
	}
}

func init() {
	factory := func(s *spec.Spec, rt source.Runtime) (source.Source, error) {
		var cc *ConnConfig
		var err error
		if s.Source.Postgres != nil {
			cc, err = BuildConnConfigFromPostgres(s.Source.Postgres)
			if err != nil {
				return nil, err
			}
		} else {
			cc, err = BuildConnConfig(s.Source.URI)
			if err != nil {
				return nil, err
			}
		}
		// Open the query connection through the SAME ConnConfig as the
		// replication reader, so TLS and the SSH tunnel apply to both.
		// sql.Open("pgx", dsn) would drop the DialFunc (no SSH) and rebuild
		// TLS from the DSN alone; stdlib.GetConnector keeps the native config.
		db := sql.OpenDB(stdlib.GetConnector(*cc.ConnConfig))
		if cc.MaxOpenConns > 0 {
			db.SetMaxOpenConns(cc.MaxOpenConns)
		}
		return Source{spec: s, rt: rt, db: db, connCfg: cc}, nil
	}
	if err := driver.RegisterSource("postgres", capabilities(), factory); err != nil {
		panic(err)
	}
}

var _ source.Source = Source{}
var _ source.QuerySource = Source{}

// Introspect resolves one spec table into its ref and canonical schema.
func (a Source) Introspect(ctx context.Context, t spec.Table) (core.TableRef, core.Schema, []core.Warning, error) {
	schemaName, tableName, ok := strings.Cut(t.Source, ".")
	if !ok {
		return source.TableRef{}, core.Schema{}, nil, fmt.Errorf("postgres: source %q must be db.table", t.Source)
	}
	st, err := QueryTable(ctx, a.db, schemaName, tableName)
	if err != nil {
		return source.TableRef{}, core.Schema{}, nil, fmt.Errorf("postgres: introspect %s: %w", t.Source, err)
	}
	pk := t.PrimaryKey
	if len(pk) == 0 && len(st.PKColumns) > 0 {
		for _, idx := range st.PKColumns {
			pk = append(pk, st.Columns[idx].Name)
		}
	}
	cs, err := CanonicalSchema(st)
	if err != nil {
		return core.TableRef{}, core.Schema{}, nil, fmt.Errorf("postgres: schema %s: %w", t.Source, err)
	}
	return core.TableRef{Source: t.Source, Target: t.Target, PrimaryKey: pk}, cs, nil, nil
}

// NewChunker builds the chunk SELECT source for one table. The concurrent
// normalization pool (#161) and the snapshot-query retry budget (#166) come
// from the resolved connection config (maxThreads / retryCount).
func (a Source) NewChunker(source, pk string, chunkSize int) (source.ChunkSource, error) {
	opts := []ChunkerOption{}
	if a.connCfg != nil {
		opts = append(opts, WithWorkers(a.connCfg.MaxOpenConns), WithRetries(a.connCfg.RetryCount))
	}
	return NewChunker(a.db, source, pk, chunkSize, opts...)
}

// CloseQuery releases the query connection and tears down the SSH tunnel.
func (a Source) CloseQuery() error {
	var err error
	if a.db != nil {
		err = a.db.Close()
	}
	if a.connCfg != nil {
		if cerr := a.connCfg.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}

// Open builds the replication reader over the pipeline's tables.
func (a Source) Open(ctx context.Context, refs []source.TableRef) (source.Reader, error) {
	slot := a.spec.Source.SlotName
	if slot == "" {
		return nil, fmt.Errorf("postgres: source requires slotName")
	}

	// Determine the replication URI and retry budget.
	uri := a.spec.Source.URI
	maxRetries := 0
	if a.connCfg != nil {
		uri = a.connCfg.QueryURI
		maxRetries = a.connCfg.RetryCount
	}

	// One channel for the whole reader life: New writes into it and the
	// returned stream pulls from it. Creating it per attempt would orphan
	// the reader's output on the attempt that succeeds.
	out := make(chan rowchange.Change, 1024)
	var rdr *Reader
	var err error
	for attempt := 0; ; attempt++ {
		rdr, err = New(ctx, Config{
			URI:        uri,
			ConnCfg:    a.connCfg,
			DB:         a.db,
			SlotName:   slot,
			Tables:     refs,
			Logger:     a.rt.Logger,
			RetryCount: maxRetries,
		}, out)
		if err == nil {
			break
		}
		// Retry only transient failures: a permanent error (bad password,
		// missing database) is not going to heal with backoff.
		if !isTransient(err) || attempt >= maxRetries {
			return nil, fmt.Errorf("postgres: open (after %d retries): %w", attempt, err)
		}
		backoff := time.Duration(1<<uint(attempt)) * time.Second
		if backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
		a.rt.Logger.Warn("postgres: connection failed, retrying",
			"attempt", attempt+1, "backoff", backoff, "err", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
	}

	puller := sourcepull.New(out)
	// Introspect each table so live batches encode against the canonical
	// schema — a stable shape per table, never a per-drain inference.
	if a.db != nil {
		schemas := make(map[string]core.Schema, len(refs))
		for _, ref := range refs {
			schemaName, tableName, ok := strings.Cut(ref.Source, ".")
			if !ok {
				continue
			}
			st, qerr := QueryTable(ctx, a.db, schemaName, tableName)
			if qerr != nil {
				return nil, fmt.Errorf("postgres: introspect %s: %w", ref.Source, qerr)
			}
			cs, serr := CanonicalSchema(st)
			if serr != nil {
				return nil, fmt.Errorf("postgres: schema %s: %w", ref.Source, serr)
			}
			schemas[ref.Target] = cs
		}
		puller.SetSchemas(schemas)
	}
	return stream{Reader: rdr, out: out, Puller: puller}, nil
}

// InitialPosition returns the slot's confirmed LSN: a first boot starts
// streaming from the slot's consistency point — everything after it flows
// through the stream, everything before it belongs to the snapshot.
func (a Source) InitialPosition(ctx context.Context) (position.Position, error) {
	return ConfirmedLSN(ctx, a.db, a.spec.Source.SlotName)
}

// ParsePosition decodes a stored cdc.position as an LSN.
func (a Source) ParsePosition(s string) (position.Position, error) {
	return position.ParseLSN(s)
}

// stream adapts the concrete pgoutput reader to the Reader contract.
type stream struct {
	*Reader
	out chan rowchange.Change
	*sourcepull.Puller
}

// Start begins the stream at the given LSN.
func (s stream) Start(ctx context.Context, from position.Position) error {
	l, ok := from.(*position.LSN)
	if !ok {
		return fmt.Errorf("postgres: start position must be an LSN, got %T", from)
	}
	errCh := make(chan error, 1)
	s.SetErr(errCh)
	go func() { errCh <- s.StartFromLSN(ctx, l) }()
	return nil
}

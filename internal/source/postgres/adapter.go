package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
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
	// A column projection must name real columns and keep every key column:
	// the sink resolves the key and sort order by column name, and the
	// snapshot SELECT uses the projection verbatim. Declared keys are checked
	// at spec validation; this catches a typo and a key the source
	// introspects.
	if err := checkColumnFilterExists(st, t.ColumnFilter); err != nil {
		return source.TableRef{}, core.Schema{}, nil, fmt.Errorf("postgres: %s: %w", t.Source, err)
	}
	if err := checkColumnFilterCoversPK(t.ColumnFilter, pk); err != nil {
		return source.TableRef{}, core.Schema{}, nil, fmt.Errorf("postgres: %s: %w", t.Source, err)
	}
	cs, err := CanonicalSchema(st)
	if err != nil {
		return source.TableRef{}, core.Schema{}, nil, fmt.Errorf("postgres: schema %s: %w", t.Source, err)
	}
	cs = filterSchemaColumns(cs, t.ColumnFilter)
	return core.TableRef{Source: t.Source, Target: t.Target, PrimaryKey: pk}, cs, nil, nil
}

// checkColumnFilterExists reports an error when a projected column is not in
// the source table. A typo would otherwise silently narrow the target schema
// (filterSchemaColumns drops unknown names) and then fail the snapshot with an
// unknown-column SQL error — or, with a snapshot-skipping bootstrap, leave the
// requested column silently absent.
func checkColumnFilterExists(st *TableState, columnFilter []string) error {
	for _, c := range columnFilter {
		if st.FindColumn(c) < 0 {
			return fmt.Errorf("columnFilter %q not found in the source table", c)
		}
	}
	return nil
}

// checkColumnFilterCoversPK reports an error when a primary-key column is not
// in the projection.
func checkColumnFilterCoversPK(columnFilter, pk []string) error {
	if len(columnFilter) == 0 || len(pk) == 0 {
		return nil
	}
	keep := make(map[string]bool, len(columnFilter))
	for _, c := range columnFilter {
		keep[c] = true
	}
	for _, k := range pk {
		if !keep[k] {
			return fmt.Errorf("columnFilter must include primary key column %q", k)
		}
	}
	return nil
}

// filterSchemaColumns keeps only the named columns, preserving the source
// order. An empty filter keeps every column.
func filterSchemaColumns(cs core.Schema, columns []string) core.Schema {
	if len(columns) == 0 {
		return cs
	}
	keep := make(map[string]bool, len(columns))
	for _, c := range columns {
		keep[c] = true
	}
	out := core.Schema{PrimaryKey: cs.PrimaryKey}
	for _, c := range cs.Columns {
		if keep[c.Name] {
			out.Columns = append(out.Columns, c)
		}
	}
	return out
}

// tableFor returns the spec table matching a source identifier.
func (a Source) tableFor(source string) (spec.Table, bool) {
	for _, t := range a.spec.Tables {
		if t.Source == source {
			return t, true
		}
	}
	return spec.Table{}, false
}

// projectionFor resolves a table's read projection (#162/#163): the selected
// columns and the compiled filter. Both are applied at the source boundary,
// before the Arrow hot-path.
func (a Source) projectionFor(source string) (columns []string, filter sq.Sqlizer, err error) {
	t, ok := a.tableFor(source)
	if !ok {
		return nil, nil, nil
	}
	if len(t.ColumnFilter) > 0 {
		columns = t.ColumnFilter
	}
	filter, err = filterToSquirrel(t.Filter)
	if err != nil {
		return nil, nil, err
	}
	return columns, filter, nil
}

// NewChunker builds the chunk SELECT source for one table. The concurrent
// normalization pool (#161) and the snapshot-query retry budget (#166) come
// from the resolved connection config (maxThreads / retryCount); the column
// projection (#162) and the filter (#163) come from the spec table.
func (a Source) NewChunker(source, pk string, chunkSize int) (source.ChunkSource, error) {
	opts := []ChunkerOption{}
	if a.connCfg != nil {
		opts = append(opts, WithWorkers(a.connCfg.MaxOpenConns), WithRetries(a.connCfg.RetryCount))
	}
	cols, filter, err := a.projectionFor(source)
	if err != nil {
		return nil, err
	}
	if len(cols) > 0 {
		opts = append(opts, WithColumns(cols))
	}
	if filter != nil {
		opts = append(opts, WithFilter(filter))
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
	filters, columns := a.filtersColumnsFor(refs)
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
			Filters:    filters,
			Columns:    columns,
		}, out)
		if err == nil {
			break
		}
		// Retry only transient failures: a permanent error (bad password,
		// missing database) is not going to heal with backoff.
		if !isTransient(err) || attempt >= maxRetries {
			return nil, fmt.Errorf("postgres: open (after %d retries): %w", attempt, err)
		}
		a.rt.Logger.Warn("postgres: connection failed, retrying",
			"attempt", attempt+1, "backoff", retryBackoff(attempt), "err", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(retryBackoff(attempt)):
		}
	}

	puller := sourcepull.New(out)
	// Introspect each table so live batches encode against the canonical
	// schema — a stable shape per table, never a per-drain inference. The
	// schema is narrowed to the column projection so the live Arrow batches
	// match the target table.
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
			if t, ok := a.tableFor(ref.Source); ok {
				if cerr := checkColumnFilterExists(st, t.ColumnFilter); cerr != nil {
					return nil, fmt.Errorf("postgres: %s: %w", ref.Source, cerr)
				}
				cs = filterSchemaColumns(cs, t.ColumnFilter)
			}
			schemas[ref.Target] = cs
		}
		puller.SetSchemas(schemas)
	}
	return stream{Reader: rdr, out: out, Puller: puller}, nil
}

// filtersColumnsFor builds the per-source filter and column projection maps
// (#162/#163) for the tables the reader streams. The reader compiles the
// filter with the introspected column types.
func (a Source) filtersColumnsFor(refs []source.TableRef) (map[string]*spec.Filter, map[string][]string) {
	var filters map[string]*spec.Filter
	var columns map[string][]string
	for _, ref := range refs {
		t, ok := a.tableFor(ref.Source)
		if !ok {
			continue
		}
		if len(t.ColumnFilter) > 0 {
			if columns == nil {
				columns = make(map[string][]string, len(refs))
			}
			columns[ref.Source] = t.ColumnFilter
		}
		if t.Filter != nil {
			if filters == nil {
				filters = make(map[string]*spec.Filter, len(refs))
			}
			filters[ref.Source] = t.Filter
		}
	}
	return filters, columns
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

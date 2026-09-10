// Package enrich — the SQL reference loader.
//
// THE TWO ROWS.
//
//	Row 1 — rows.Scan. Imposed by database/sql: the driver writes into
//	        []any targets, one per column, one call per source row.
//	Row 2 — the value→builder switch in appendTyped, same loop iteration.
//	        Also imposed by database/sql: Scan hands back the driver's
//	        natural Go type per cell and someone has to route it to a
//	        typed Arrow builder.
//
// Both are co-located in Load's loop, here, and nowhere else in the
// system. Both die together when an Arrow-native driver (ADBC) lands —
// then Load becomes "read the reference as Arrow, done." Tracked; not this
// wave.
//
// Every loop and every scalar temporary in this file carries a
// //allow:rowloop marker with its reason. No //allow:rowloop marker exists
// anywhere else in internal/enrich except columnar.go's index/overlay pass.
package enrich

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	// The reference read is a plain SQL query against a second database —
	// the same engines as the sources. Both are wired through
	// driver.Connector (never a re-serialized DSN string).
	"github.com/go-sql-driver/mysql"
	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// NewSQLLoader opens the reference connection. The URI scheme picks the
// driver (mysql:// or postgres://); onRef is the reference-side join column
// name — the loader appends ORDER BY onRef when the query has no ORDER BY,
// so buildImage can detect duplicate keys by adjacent diff.
func NewSQLLoader(uri, query, onRef string) (Loader, error) {
	var connector driver.Connector
	switch {
	case strings.HasPrefix(uri, "mysql://"):
		cfg, err := mysqlConfig(uri)
		if err != nil {
			return nil, fmt.Errorf("enrich: reference uri: %w", err)
		}
		c, err := mysql.NewConnector(cfg)
		if err != nil {
			return nil, fmt.Errorf("enrich: mysql connector: %w", err)
		}
		connector = c
	case strings.HasPrefix(uri, "postgres://") || strings.HasPrefix(uri, "postgresql://"):
		dsn, err := pgx.ParseConfig(uri)
		if err != nil {
			return nil, fmt.Errorf("enrich: reference uri: %w", err)
		}
		connector = stdlib.GetConnector(*dsn)
	default:
		return nil, fmt.Errorf("enrich: reference uri %q: unsupported scheme (mysql:// | postgres://)", uri)
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1) // one sequential re-read per refresh; no pool theater
	db.SetConnMaxLifetime(5 * time.Minute)
	return &sqlLoader{db: db, query: query, onRef: onRef, alloc: memory.DefaultAllocator}, nil
}

// mysqlConfig parses a mysql:// URI into the driver config — WITHOUT ever
// re-serializing the credentials as a DSN string (RV-06): a password
// decoded from the URI may contain ':', '@', '/' or '?' — any of them
// corrupts a DSN the driver parses positionally.
func mysqlConfig(raw string) (*mysql.Config, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "mysql" {
		return nil, fmt.Errorf("mysql: uri scheme %q, want mysql", u.Scheme)
	}
	user := u.User.Username()
	pass, _ := u.User.Password()
	host := u.Hostname()
	if host == "" {
		return nil, fmt.Errorf("mysql: uri %q lacks host", raw)
	}
	port := u.Port()
	if port == "" {
		port = "3306"
	}
	db := strings.TrimPrefix(u.Path, "/")
	if db == "" {
		return nil, fmt.Errorf("mysql: uri %q lacks /db", raw)
	}
	cfg := mysql.NewConfig()
	cfg.User = user
	cfg.Passwd = pass
	cfg.Net = "tcp"
	cfg.Addr = net.JoinHostPort(host, port)
	cfg.DBName = db
	cfg.ParseTime = true // DATETIME decodes as time.Time
	if q := u.Query(); len(q) > 0 {
		cfg.Params = make(map[string]string, len(q))
		for k, vs := range q {
			if len(vs) > 0 {
				cfg.Params[k] = vs[0]
			}
		}
	}
	return cfg, nil
}

type sqlLoader struct {
	db    *sql.DB
	query string
	onRef string
	alloc memory.Allocator
}

var orderByRe = regexp.MustCompile(`(?is)\border\s+by\b[^)]*$`)

// hasOrderBy reports whether q already ends with an ORDER BY clause (after
// trimming a trailing ';' and whitespace). A conservative suffix check: a
// query with ORDER BY inside a subquery but not at the top level reads as
// false and gets wrapped.
func hasOrderBy(q string) bool {
	return orderByRe.MatchString(strings.TrimRight(strings.TrimSpace(q), "; \t\n"))
}

// appendOrderBy makes q deterministically ordered by onRef so buildImage's
// adjacent-diff duplicate check is valid. A bare query gets a suffix; a
// query with LIMIT/GROUP BY/UNION near the end (where a suffix would bind
// wrong) is wrapped in a subquery.
func appendOrderBy(q, onRef string) string {
	trimmed := strings.TrimRight(strings.TrimSpace(q), "; \t\n")
	tail := strings.ToLower(trimmed)
	if strings.Contains(tail, " limit ") || strings.Contains(tail, " union ") ||
		strings.Contains(tail, " group by ") || strings.HasSuffix(tail, ")") {
		return fmt.Sprintf("SELECT * FROM (%s) _enrich_ref ORDER BY %s", trimmed, quoteIdent(onRef))
	}
	return trimmed + " ORDER BY " + quoteIdent(onRef)
}

func quoteIdent(s string) string {
	// onRef comes from the spec, validated as an identifier; the quotes are
	// belt-and-suspenders and portable enough for both engines.
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// Load runs the reference query and returns every column as a typed Arrow
// array in one record. See THE TWO ROWS at the top of this file.
func (l *sqlLoader) Load(ctx context.Context) (arrow.RecordBatch, error) {
	q := l.query
	if !hasOrderBy(q) {
		q = appendOrderBy(q, l.onRef)
	}
	rows, err := l.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("enrich: reference query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("enrich: reference columns: %w", err)
	}

	fields := make([]arrow.Field, len(colTypes)) // []Field: config
	builders := make([]array.Builder, len(colTypes))
	for i, ct := range colTypes { // range over config
		dt := builderType(ct)
		fields[i] = arrow.Field{Name: ct.Name(), Type: dt, Nullable: true}
		builders[i] = array.NewBuilder(l.alloc, dt)
	}
	defer func() {
		for _, b := range builders { // range over config
			b.Release()
		}
	}()

	//allow:rowloop Row 1 — the driver writes into scanTargets; size is len(colTypes), not a data count.
	scanTargets := make([]any, len(colTypes))
	for i := range scanTargets { // range over the config-sized target slice
		scanTargets[i] = new(any)
	}

	//allow:rowloop Row 1 — THE ONLY ROW LOOP IN THE SYSTEM (database/sql imposes it).
	for rows.Next() {
		if err := rows.Scan(scanTargets...); err != nil {
			return nil, fmt.Errorf("enrich: reference scan: %w", err)
		}
		for i, b := range builders { // range over config
			if err := appendTyped(b, *(scanTargets[i].(*any))); err != nil {
				return nil, fmt.Errorf("enrich: reference column %q: %w", colTypes[i].Name(), err)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("enrich: reference rows: %w", err)
	}

	arrs := make([]arrow.Array, len(builders)) // []Array: config
	for i, b := range builders {               // range over config
		arrs[i] = b.NewArray()
	}
	defer func() {
		for _, a := range arrs { // range over config
			a.Release()
		}
	}()
	n := int64(0)
	if len(arrs) > 0 {
		n = int64(arrs[0].Len())
	}
	return array.NewRecordBatch(arrow.NewSchema(fields, nil), arrs, n), nil
}

func (l *sqlLoader) Close() error { return l.db.Close() }

// builderType picks the Arrow type for a SQL column. The reference value
// space is deliberately small (string / int64 / float64 / bool / []byte /
// time.Time) — the same family the CDC sources decode into.
func builderType(ct *sql.ColumnType) arrow.DataType {
	return builderTypeByName(ct.DatabaseTypeName())
}

// builderTypeByName maps a database type name to an Arrow type. An unknown
// name falls back to String; the driver hands back []byte or string there
// and appendTyped stringifies.
func builderTypeByName(dbType string) arrow.DataType {
	switch strings.ToUpper(dbType) {
	case "BOOL", "BOOLEAN", "TINYINT(1)":
		return arrow.FixedWidthTypes.Boolean
	case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT",
		"INT2", "INT4", "INT8", "SERIAL", "BIGSERIAL":
		return arrow.PrimitiveTypes.Int64
	case "FLOAT", "DOUBLE", "REAL", "NUMERIC", "DECIMAL", "FLOAT4", "FLOAT8":
		return arrow.PrimitiveTypes.Float64
	case "DATETIME", "TIMESTAMP", "TIMESTAMPTZ", "DATE", "TIME":
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	case "BLOB", "BYTEA", "BINARY", "VARBINARY", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB":
		return arrow.BinaryTypes.Binary
	default:
		// CHAR/VARCHAR/TEXT/UUID/JSON/enum/unknown → string.
		return arrow.BinaryTypes.String
	}
}

//allow:rowloop Row 2 — THE ONLY value-conversion switch in the system (database/sql imposes it).
func appendTyped(b array.Builder, v any) error {
	if v == nil {
		b.AppendNull()
		return nil
	}
	switch bl := b.(type) {
	case *array.BooleanBuilder:
		switch t := v.(type) {
		case bool:
			bl.Append(t)
		case int64:
			bl.Append(t != 0)
		case []byte:
			bl.Append(len(t) == 1 && t[0] != '0')
		default:
			return fmt.Errorf("value %T is not boolean-compatible", v)
		}
	case *array.Int64Builder:
		switch t := v.(type) {
		case int64:
			bl.Append(t)
		case int32:
			bl.Append(int64(t))
		case int:
			bl.Append(int64(t))
		case []byte:
			n, perr := parseInt(t)
			if perr != nil {
				return perr
			}
			bl.Append(n)
		default:
			return fmt.Errorf("value %T is not int64-compatible", v)
		}
	case *array.Float64Builder:
		switch t := v.(type) {
		case float64:
			bl.Append(t)
		case float32:
			bl.Append(float64(t))
		case int64:
			bl.Append(float64(t))
		case []byte:
			f, perr := parseFloat(t)
			if perr != nil {
				return perr
			}
			bl.Append(f)
		default:
			return fmt.Errorf("value %T is not float64-compatible", v)
		}
	case *array.TimestampBuilder:
		switch t := v.(type) {
		case time.Time:
			bl.Append(arrow.Timestamp(t.UTC().UnixMicro()))
		case []byte:
			tm, perr := time.Parse("2006-01-02 15:04:05", string(t))
			if perr != nil {
				return fmt.Errorf("timestamp %q: %w", t, perr)
			}
			bl.Append(arrow.Timestamp(tm.UTC().UnixMicro()))
		default:
			return fmt.Errorf("value %T is not timestamp-compatible", v)
		}
	case *array.BinaryBuilder:
		switch t := v.(type) {
		case []byte:
			bl.Append(t)
		case string:
			bl.Append([]byte(t))
		default:
			return fmt.Errorf("value %T is not binary-compatible", v)
		}
	case *array.StringBuilder:
		switch t := v.(type) {
		case string:
			bl.Append(t)
		case []byte:
			bl.Append(string(t))
		default:
			bl.Append(fmt.Sprintf("%v", v))
		}
	default:
		return fmt.Errorf("enrich: no appendTyped case for builder %T", b)
	}
	return nil
}

func parseInt(b []byte) (int64, error) {
	var n int64
	if _, err := fmt.Sscan(string(b), &n); err != nil {
		return 0, fmt.Errorf("int %q: %w", b, err)
	}
	return n, nil
}

func parseFloat(b []byte) (float64, error) {
	var f float64
	if _, err := fmt.Sscan(string(b), &f); err != nil {
		return 0, fmt.Errorf("float %q: %w", b, err)
	}
	return f, nil
}

package mysql

import (
	"fmt"
	"strings"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/maltzsama/urutau/source"
)

// OnDDL surfaces destructive DDL (TRUNCATE/DROP/ALTER/RENAME) seen on the
// stream. The query is the authoritative statement. The engine does not
// propagate it, so the sink diverges from the source: it is reported as a
// metric and a run event, and under onTruncate: fail the stream ends here
// (#671). Benign DDL (CREATE INDEX, CREATE TABLE, …) is left to the worker's
// schema-drift check and is not reported.
func (r *Reader) OnDDL(_ *replication.EventHeader, _ gomysql.Position, q *replication.QueryEvent) error {
	stmt := string(q.Query)
	kind, table, destructive := destructiveDDL(stmt)
	if !destructive {
		return nil
	}
	if r.cfg.OnDestructiveDDL != nil {
		r.cfg.OnDestructiveDDL(source.DestructiveDDL{
			Source: "mysql", Kind: kind, Table: table, Detail: stmt,
		})
	}
	if r.cfg.OnTruncate == "fail" {
		return fmt.Errorf("mysql: destructive DDL on replicated table %q (onTruncate: fail): %s", table, stmt)
	}
	r.cfg.Logger.Warn("mysql: destructive DDL detected; not propagated to the sink",
		"kind", kind, "table", table, "query", stmt)
	return nil
}

// destructiveDDL classifies a statement as destructive DDL and extracts the
// affected table name (best-effort). It returns ok=false for anything that is
// not TRUNCATE/DROP/ALTER/RENAME.
func destructiveDDL(stmt string) (kind, table string, ok bool) {
	fields := strings.Fields(strings.TrimSpace(stmt))
	if len(fields) == 0 {
		return "", "", false
	}
	kw := strings.ToUpper(fields[0])
	switch kw {
	case "TRUNCATE":
		kind = "truncate"
	case "ALTER", "RENAME", "DROP":
		kind = "ddl"
	default:
		return "", "", false
	}
	// Skip an optional object keyword (TABLE/VIEW/…) and IF EXISTS to land on
	// the table identifier.
	rest := fields[1:]
	for len(rest) > 0 {
		switch strings.ToUpper(rest[0]) {
		case "IF", "EXISTS", "TABLE", "VIEW", "DATABASE", "SCHEMA":
			rest = rest[1:]
			continue
		}
		break
	}
	if len(rest) > 0 {
		table = strings.NewReplacer("`", "", `"`, "").Replace(rest[0])
	}
	return kind, table, true
}

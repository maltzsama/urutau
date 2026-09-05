package clickhouse

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/maltzsama/urutau/change"
	"github.com/maltzsama/urutau/core"
)

// tableIdent names one table: database + name, both ClickHouse identifiers.
type tableIdent struct {
	db, table string
}

func (t tableIdent) quoted() string {
	return quoteIdent(t.db) + "." + quoteIdent(t.table)
}

func quoteIdent(id string) string {
	return "`" + strings.ReplaceAll(id, "`", "\\`") + "`"
}

// reserved names the columns the sink owns. `position` carries the commit
// coordinate the resume reads via argMax, `seq` orders it, `is_deleted`
// marks tombstones. A spec column under one of these names would silently
// change what resume reads — EnsureTable rejects it instead. A readable
// position column for humans is the metadata column's job (declare
// metadata { from: position, as: <name> }); the reserved column is the
// mechanism's.
var reserved = map[string]bool{"position": true, "seq": true, "is_deleted": true}

// buildDDL renders the CREATE TABLE for one target table. The write mode
// picks the engine, because in ClickHouse that decision IS the DDL:
// upsert needs versioning (ReplacingMergeTree over seq, with tombstones);
// append is a plain log (MergeTree, no is_deleted — but seq stays, as pure
// ordering for the resume's argMax, not versioning).
//
// Default is NO partition: every insert lands in the single partition and is
// atomic by the engine's own guarantee. Declaring partitionBy (opt-in, raw
// ClickHouse expressions) trades that away for pruning — a batch crossing a
// partition boundary commits as multiple parts and is no longer all-or-
// nothing. That property must stay documented, never implied.
func buildDDL(ident tableIdent, ref core.TableRef, schema core.Schema, partitionBy []string, mode change.WriteMode) (string, error) {
	upsert := mode != change.AppendMode

	byName := make(map[string]core.Column, len(schema.Columns))
	for _, c := range schema.Columns {
		if reserved[c.Name] {
			return "", fmt.Errorf("column %q is reserved by the sink (position, seq, is_deleted are managed); declare readable copies via metadata as", c.Name)
		}
		byName[c.Name] = c
	}

	var order string
	if upsert {
		if len(ref.PrimaryKey) == 0 {
			return "", fmt.Errorf("upsert table %s requires a primary key", ref.Target)
		}
		keys := make([]string, 0, len(ref.PrimaryKey))
		for _, k := range ref.PrimaryKey {
			if _, ok := byName[k]; !ok {
				return "", fmt.Errorf("primary key column %q not in schema", k)
			}
			keys = append(keys, quoteIdent(k))
		}
		order = "(" + strings.Join(keys, ", ") + ")"
	} else {
		order = "tuple()"
	}

	cols := make([]string, 0, len(schema.Columns)+3)
	for _, c := range schema.Columns {
		// Sorting-key columns cannot be Nullable in ClickHouse.
		pkMember := upsert && slices.Contains(ref.PrimaryKey, c.Name)
		nullable := c.Type.Nullable && !pkMember
		t, err := chType(c.Type, nullable)
		if err != nil {
			return "", fmt.Errorf("column %q: %w", c.Name, err)
		}
		cols = append(cols, quoteIdent(c.Name)+" "+t)
	}
	cols = append(cols, quoteIdent("position")+" String", quoteIdent("seq")+" UInt64")
	if upsert {
		cols = append(cols, quoteIdent("is_deleted")+" UInt8 DEFAULT 0")
	}
	// Physical tombstone cleanup is the operator's maintenance (the engine
	// hides deleted rows from FINAL reads; removing them physically takes
	// OPTIMIZE ... FINAL CLEANUP, gated behind an experimental flag). The
	// sink marks tombstones correctly — it does not own vacuuming.

	engine := "MergeTree"
	if upsert {
		engine = "ReplacingMergeTree(seq, is_deleted)"
	}

	ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = %s ORDER BY %s",
		ident.quoted(), strings.Join(cols, ", "), engine, order)
	if len(partitionBy) > 0 {
		ddl += " PARTITION BY (" + strings.Join(partitionBy, ", ") + ")"
	}
	return ddl, nil
}

var errUnqualified = errors.New("target is unqualified and the sink declares no namespace")

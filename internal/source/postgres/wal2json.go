package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	pglogrepl "github.com/jackc/pglogrepl"

	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/position"
)

// wal2jsonMessage is one wal2json logical message (format-version 2). The
// plugin emits one JSON object per message: begin/commit boundaries and one
// object per row change.
type wal2jsonMessage struct {
	Action       string          `json:"action"` // B, C, I, U, D, T
	Schema       string          `json:"schema"`
	Table        string          `json:"table"`
	ColumnNames  []string        `json:"columnnames"`
	ColumnTypes  []string        `json:"columntypes"`
	ColumnValues []any           `json:"columnvalues"`
	OldKeys      *wal2jsonOldKey `json:"oldkeys"`
	NextLSN      string          `json:"nextlsn"`
	Timestamp    string          `json:"timestamp"`
}

// wal2jsonOldKey carries a delete's (or an update's old) row: with REPLICA
// IDENTITY FULL, keynames/keyvalues hold every column.
type wal2jsonOldKey struct {
	KeyNames  []string `json:"keynames"`
	KeyTypes  []string `json:"keytypes"`
	KeyValues []any    `json:"keyvalues"`
}

// handleWal2json decodes one wal2json message into the transaction buffer and
// flushes it at the commit boundary — the same enqueue/handleCommit path the
// pgoutput decoder uses, so windows, projections and filters apply unchanged.
func (r *Reader) handleWal2json(ctx context.Context, payload []byte) error {
	var msg wal2jsonMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		return fmt.Errorf("postgres: wal2json: %w", err)
	}
	switch msg.Action {
	case "B":
		r.txn = r.txn[:0]
	case "C":
		lsn, err := position.ParseLSN(msg.NextLSN)
		if err != nil {
			return fmt.Errorf("postgres: wal2json: commit lsn %q: %w", msg.NextLSN, err)
		}
		r.curCommitTS = parseWal2jsonTime(msg.Timestamp)
		return r.handleCommit(ctx, pglogrepl.LSN(*lsn))
	case "I", "U", "D":
		return r.handleWal2jsonChange(msg)
	case "T":
		r.cfg.Logger.Warn("postgres: wal2json truncate received; ignored (not part of the scalar milestone)")
	}
	return nil
}

// handleWal2jsonChange maps one row change onto a rowchange.Change. The table
// is identified by name (wal2json has no Relation message), so the ref,
// introspected state and projection are looked up by "schema.table".
func (r *Reader) handleWal2jsonChange(msg wal2jsonMessage) error {
	src := msg.Schema + "." + msg.Table
	ref, ok := r.bySrc[src]
	if !ok {
		return nil // not ours
	}
	st, ok := r.states[src]
	if !ok {
		return nil
	}
	entry := relEntry{state: st, ref: ref, proj: r.projections[src]}

	after, err := wal2jsonRow(st, msg.ColumnNames, msg.ColumnValues)
	if err != nil {
		return err
	}
	var before map[string]any
	if msg.OldKeys != nil {
		if before, err = wal2jsonRow(st, msg.OldKeys.KeyNames, msg.OldKeys.KeyValues); err != nil {
			return err
		}
	}

	switch msg.Action {
	case "I":
		keep, err := entry.proj.keep(after)
		if err != nil {
			return err
		}
		if !keep {
			return nil
		}
		r.enqueue(entry, rowchange.OpInsert, entry.proj.project(after), nil)
	case "U":
		if entry.proj.hasFilter() {
			afterMatch, err := entry.proj.keep(after)
			if err != nil {
				return err
			}
			beforeMatch := false
			if before != nil {
				if beforeMatch, err = entry.proj.keep(before); err != nil {
					return err
				}
			}
			if !beforeMatch && !afterMatch {
				return nil
			}
			if beforeMatch && !afterMatch {
				r.enqueue(entry, rowchange.OpDelete, nil, entry.proj.project(before))
				return nil
			}
		}
		r.enqueue(entry, rowchange.OpUpdate, entry.proj.project(after), entry.proj.project(before))
	case "D":
		if before == nil {
			return fmt.Errorf("postgres: wal2json: delete %s: no old row", src)
		}
		keep, err := entry.proj.keep(before)
		if err != nil {
			return err
		}
		if !keep {
			return nil
		}
		r.enqueue(entry, rowchange.OpDelete, nil, entry.proj.project(before))
	}
	return nil
}

// wal2jsonRow keys column names to coerced values. A column absent from the
// message (a delete carries only the identity) stays absent.
func wal2jsonRow(st *TableState, names []string, values []any) (map[string]any, error) {
	row := make(map[string]any, len(names))
	for i, name := range names {
		var v any
		if i < len(values) {
			v = values[i]
		}
		if col := st.FindColumn(name); col >= 0 {
			cv, err := coerceWal2json(v, st.Columns[col].DataType)
			if err != nil {
				return nil, fmt.Errorf("postgres: wal2json: column %s: %w", name, err)
			}
			v = cv
		}
		row[name] = v
	}
	return row, nil
}

// coerceWal2json maps a JSON value onto the Go type urutau's Arrow encoder
// expects for the column: JSON numbers arrive as float64, so integer columns
// are narrowed back to int64. Everything else passes through (string, bool,
// nil), which is already the decoded shape.
func coerceWal2json(v any, dataType string) (any, error) {
	if v == nil {
		return nil, nil
	}
	switch strings.ToLower(dataType) {
	case "smallint", "integer", "bigint":
		switch t := v.(type) {
		case float64:
			return int64(t), nil
		case string:
			return strconv.ParseInt(t, 10, 64)
		}
	case "real", "double precision":
		switch t := v.(type) {
		case float64:
			return t, nil
		case string:
			return strconv.ParseFloat(t, 64)
		}
	}
	return v, nil
}

// parseWal2jsonTime parses the plugin's commit timestamp; a zero time on any
// unrecognized layout (the timestamp is advisory, not a resume coordinate).
func parseWal2jsonTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	for _, layout := range []string{
		"2006-01-02 15:04:05.999999-07",
		"2006-01-02 15:04:05.999999-07:00",
		"2006-01-02 15:04:05-07",
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

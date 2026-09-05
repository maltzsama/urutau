package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/maltzsama/urutau/core"
)

// urutau_progress is the ClickHouse stand-in for the Iceberg table
// properties that carry snapshot state (chunk bounds, pending list).
//
// DOCUMENTED CONTRACT: it is an optimization record, NEVER a source of data
// correctness. Losing it must only cost a re-snapshot (cold backfill), never
// a wrong or missing row. If you are about to read it to decide what data
// should exist, stop — the data's truth is in the data table, whose rows
// carry the position.
const progressTable = "urutau_progress"

// ensureProgressTable creates the progress side table if absent.
func (s *Sink) ensureProgressTable(ctx context.Context) error {
	q := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s "+
		"(`target` String, `k` String, `v` String, `ver` UInt64) "+
		"ENGINE = ReplacingMergeTree(ver) ORDER BY (target, k)",
		quoteIdent(progressTable))
	if err := s.conn.Exec(ctx, q); err != nil {
		return fmt.Errorf("create %s: %w", progressTable, err)
	}
	return nil
}

// SetProperties records snapshot progress for one target. SnapshotState and
// its pending chunk list travel atomically under one ver, so a crash mid-
// write leaves the previous state as the winner.
func (s *Sink) SetProperties(ctx context.Context, ref core.TableRef, props map[string]string) error {
	if len(props) == 0 {
		return nil
	}
	target, err := s.targetKey(ref)
	if err != nil {
		return err
	}
	ver := uint64(time.Now().UnixNano())
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO "+quoteIdent(progressTable))
	if err != nil {
		return fmt.Errorf("progress insert: %w", err)
	}
	for k, v := range props {
		if err := batch.Append(target, k, v, ver); err != nil {
			_ = batch.Abort()
			return fmt.Errorf("progress append %q: %w", k, err)
		}
	}
	if err := batch.Send(); err != nil {
		_ = batch.Abort()
		return fmt.Errorf("progress send: %w", err)
	}
	return nil
}

// Properties reads the latest recorded progress for one target. A missing
// record yields an empty map with no error (treated as not started).
func (s *Sink) Properties(ctx context.Context, ref core.TableRef) (map[string]string, error) {
	target, err := s.targetKey(ref)
	if err != nil {
		return nil, err
	}
	rows, err := s.conn.Query(ctx,
		fmt.Sprintf("SELECT k, argMax(v, ver) FROM %s WHERE target = ? GROUP BY k", quoteIdent(progressTable)),
		target)
	if err != nil {
		return nil, fmt.Errorf("progress read: %w", err)
	}
	defer func() { _ = rows.Close() }()
	props := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		props[k] = v
	}
	return props, rows.Err()
}

// targetKey is the progress table's row key: the target name as the spec
// declares it (not the quoted identifier), stable across boots.
func (s *Sink) targetKey(ref core.TableRef) (string, error) {
	if ref.Target == "" {
		return "", fmt.Errorf("progress: target is required")
	}
	return ref.Target, nil
}

package postgres

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	pglogrepl "github.com/jackc/pglogrepl"

	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/source"
)

// microsecFromUnixEpochToY2K mirrors pglogrepl's epoch offset.
const microsecFromUnixEpochToY2K = 946684800 * 1000000

// beginPayload builds the pgoutput Begin message payload (type byte + body):
// FinalLSN (8), CommitTime (8, µs since 2000-01-01), Xid (4).
func beginPayload(finalLSN uint64, commitTS time.Time, xid uint32) []byte {
	micros := commitTS.Unix()*1000000 + int64(commitTS.Nanosecond())/1000 - microsecFromUnixEpochToY2K
	buf := make([]byte, 21)
	buf[0] = msgBegin
	binary.BigEndian.PutUint64(buf[1:9], finalLSN)
	binary.BigEndian.PutUint64(buf[9:17], uint64(micros))
	binary.BigEndian.PutUint32(buf[17:21], xid)
	return buf
}

// TestCommitTSCapturedFromBegin covers #160: the transaction commit time from
// the pgoutput Begin message is stamped on every row of the transaction.
func TestCommitTSCapturedFromBegin(t *testing.T) {
	r := &Reader{}
	want := time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC)
	if err := r.handleXLogData(context.Background(), pglogrepl.XLogData{WALData: beginPayload(0x40, want, 7)}); err != nil {
		t.Fatal(err)
	}
	if !r.curCommitTS.Equal(want) {
		t.Fatalf("curCommitTS = %v, want %v", r.curCommitTS, want)
	}
	r.enqueue(relEntry{ref: source.TableRef{Target: "raw.t"}}, rowchange.OpInsert, map[string]any{"id": int64(1)}, nil)
	if len(r.txn) != 1 {
		t.Fatalf("txn = %d rows, want 1", len(r.txn))
	}
	if !r.txn[0].CommitTS.Equal(want) {
		t.Fatalf("change CommitTS = %v, want %v", r.txn[0].CommitTS, want)
	}
	// Every row of the same transaction shares the commit time.
	r.enqueue(relEntry{ref: source.TableRef{Target: "raw.t"}}, rowchange.OpDelete, nil, map[string]any{"id": int64(2)})
	if !r.txn[1].CommitTS.Equal(want) {
		t.Fatalf("second change CommitTS = %v, want %v", r.txn[1].CommitTS, want)
	}
}

// TestInitialWaitExceeded covers the #154 timer decision.
func TestInitialWaitExceeded(t *testing.T) {
	started := time.Unix(1000, 0)
	wait := 300 * time.Second
	if initialWaitExceeded(false, started, wait, started.Add(299*time.Second)) {
		t.Fatal("before the deadline must not fire")
	}
	if !initialWaitExceeded(false, started, wait, started.Add(301*time.Second)) {
		t.Fatal("past the deadline must fire")
	}
	if initialWaitExceeded(true, started, wait, started.Add(time.Hour)) {
		t.Fatal("a primed stream must never fire, however long it is idle")
	}
}

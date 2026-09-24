package worker

// Issue #356: a deterministic, model-based check of the delivery contract.
//
// The contract is state-based — at-least-once delivery, idempotent sinks — so a
// scenario test ("this sequence passed") leaves a gap between it and "the
// implementation preserves state under arbitrary valid replay/restart
// sequences". This drives a seeded sequence of INSERT/UPDATE/DELETE through
// the worker into a state-applying sink and compares the result to a tiny
// reference model (key → value), with duplicates, replays, and varying batch
// boundaries. The final sink state must equal the model exactly.

import (
	"context"
	"fmt"
	"maps"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/sink"
)

// stateSink applies committed batches to an in-memory table, so a test can
// compare the engine's result to a reference model. It is the "sink" half of
// the state-based contract: upsert sets key → value, delete removes the key.
type stateSink struct {
	mu    sync.Mutex
	state map[int64]string
	pos   string
}

func (s *stateSink) Close() error { return nil }

func (s *stateSink) Commit(_ context.Context, b *dataplane.Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if b.Record != nil && b.Record.NumRows() > 0 {
		rows, err := transport.DecodeBatch(b.Record, b.Table, []string{"id"})
		if err != nil {
			return err
		}
		for _, c := range rows {
			id, ok := c.Key[0].(int64)
			if !ok {
				continue
			}
			switch c.Op {
			case rowchange.OpDelete:
				delete(s.state, id)
			default:
				if v, ok := c.After["v"].(string); ok {
					s.state[id] = v
				}
			}
		}
	}
	if len(b.Watermark) > 0 {
		s.pos = string(b.Watermark)
	}
	return nil
}

// generateOps returns a seeded sequence of changes and the reference model
// they produce (last write wins; a delete removes the key). The key space is
// small so the sequence exercises many updates and deletes per key.
func generateOps(rng *rand.Rand, n int) ([]rowchange.Change, map[int64]string) {
	model := map[int64]string{}
	changes := make([]rowchange.Change, 0, n)
	for i := 0; i < n; i++ {
		id := int64(rng.Intn(64))
		pos := fmt.Sprintf("pos-%d", i)
		switch rng.Intn(3) {
		case 0, 1: // insert or update — both upsert to the new value
			op := rowchange.OpInsert
			if rng.Intn(2) == 0 {
				op = rowchange.OpUpdate
			}
			v := fmt.Sprintf("v%d", rng.Intn(1000))
			changes = append(changes, chg("t", op, id, v, pos))
			model[id] = v
		case 2:
			changes = append(changes, rowchange.Change{
				Op: rowchange.OpDelete, Table: "t", Key: []any{id}, Position: pos,
			})
			delete(model, id)
		}
	}
	return changes, model
}

// duplicateSome re-delivers a change immediately (same op, same value) for
// roughly one in five — an at-least-once duplicate, which an idempotent sink
// must absorb without changing the final state.
func duplicateSome(rng *rand.Rand, in []rowchange.Change) []rowchange.Change {
	out := make([]rowchange.Change, 0, len(in)*2)
	for _, c := range in {
		out = append(out, c)
		if rng.Intn(5) == 0 {
			out = append(out, c)
		}
	}
	return out
}

func runStateSink(t *testing.T, changes []rowchange.Change, maxRows int) *stateSink {
	t.Helper()
	s := &stateSink{state: map[int64]string{}}
	if err := runWorker(t, Config{MaxRows: maxRows, MaxInterval: time.Hour},
		[]string{"t"}, map[string]sink.TableWriter{"t": s}, changes); err != nil {
		t.Fatalf("worker: %v", err)
	}
	return s
}

// A seeded sequence of upserts/deletes, with duplicates and at every batch
// boundary, must land exactly as the reference model — and the committed
// position must never advance past the last delivered change.
func TestStateMachineReplayAndDuplicates(t *testing.T) {
	for seed := int64(0); seed < 40; seed++ {
		rng := rand.New(rand.NewSource(seed))
		base, model := generateOps(rng, 250)
		delivered := duplicateSome(rng, base)
		wantPos := delivered[len(delivered)-1].Position

		for _, maxRows := range []int{1, 8, 1000} {
			s := runStateSink(t, delivered, maxRows)
			if !maps.Equal(model, s.state) {
				t.Fatalf("seed %d maxRows %d: sink state != model\n got=%v\nwant=%v", seed, maxRows, s.state, model)
			}
			if s.pos != wantPos {
				t.Fatalf("seed %d maxRows %d: committed position = %q, want %q (never past durable data)", seed, maxRows, s.pos, wantPos)
			}
		}
	}
}

// A restart replays the tail of the log (at-least-once), so the second run
// re-delivers an overlap. The combined state must still equal the model, and
// the batch boundaries of each run must not matter.
func TestStateMachineRestartReplay(t *testing.T) {
	for seed := int64(0); seed < 50; seed++ {
		rng := rand.New(rand.NewSource(seed))
		base, model := generateOps(rng, 200)
		cut := 1 + rng.Intn(len(base)-1)
		overlap := 5
		start := cut - overlap
		if start < 0 {
			start = 0
		}

		s := &stateSink{state: map[int64]string{}}
		for _, part := range [][]rowchange.Change{base[:cut], base[start:]} {
			if err := runWorker(t, Config{MaxRows: 16, MaxInterval: time.Hour},
				[]string{"t"}, map[string]sink.TableWriter{"t": s}, part); err != nil {
				t.Fatalf("seed %d: worker: %v", seed, err)
			}
		}
		if !maps.Equal(model, s.state) {
			t.Fatalf("seed %d: after restart sink != model\n got=%v\nwant=%v", seed, s.state, model)
		}
	}
}

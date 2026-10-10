package remote

import (
	"context"
	"sync"
	"testing"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/worker"
	"github.com/maltzsama/urutau/sink"
)

// fakeCommitter is the minimal sink.TableWriter the chunk tests register; it
// records nothing the chunk path needs beyond satisfying the contract.
type fakeCommitter struct {
	mu      sync.Mutex
	batches []*dataplane.Batch
}

func (f *fakeCommitter) Close() error { return nil }

func (f *fakeCommitter) Commit(_ context.Context, b *dataplane.Batch) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batches = append(f.batches, b)
	return nil
}

// testSchema is the id/v schema the remote tests register tables with.
func testSchema() core.Schema {
	return core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "v", Type: core.ColumnType{Kind: core.KindString}},
		},
		PrimaryKey: []string{"id"},
	}
}

// regTable registers a table with testSchema via the exported worker API.
func regTable(t *testing.T, w *worker.Worker, target string, c sink.TableWriter, mode dataplane.WriteMode) {
	t.Helper()
	w.RegisterCommitter(target, c, mode)
	w.SetKnownSchema(target, testSchema())
}

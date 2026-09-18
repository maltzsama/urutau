package postgres

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// sliceProducer returns a produce func over a fixed row set.
func sliceProducer(rows [][]any) func() ([]any, bool, error) {
	i := 0
	return func() ([]any, bool, error) {
		if i >= len(rows) {
			return nil, false, nil
		}
		r := rows[i]
		i++
		return r, true, nil
	}
}

func testRows(n int) [][]any {
	rows := make([][]any, n)
	for i := range rows {
		rows[i] = []any{int64(i), fmt.Sprintf("v%d", i)}
	}
	return rows
}

func TestScanPipelineSequential(t *testing.T) {
	cols := []string{"id", "v"}
	rows := testRows(100)
	var got []map[string]any
	err := scanPipeline(context.Background(), cols, 1, sliceProducer(rows), func(m map[string]any) error {
		got = append(got, m)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(rows) {
		t.Fatalf("got %d rows, want %d", len(got), len(rows))
	}
	// Sequential preserves order.
	if got[0]["id"] != int64(0) || got[99]["id"] != int64(99) {
		t.Fatalf("sequential order broken: first=%v last=%v", got[0]["id"], got[99]["id"])
	}
}

func TestScanPipelineConcurrent(t *testing.T) {
	cols := []string{"id", "v"}
	rows := testRows(5000)

	var mu sync.Mutex
	seen := make(map[int64]bool, len(rows))
	err := scanPipeline(context.Background(), cols, 8, sliceProducer(rows), func(m map[string]any) error {
		mu.Lock()
		seen[m["id"].(int64)] = true
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(seen) != len(rows) {
		t.Fatalf("got %d distinct rows, want %d", len(seen), len(rows))
	}
}

func TestScanPipelineFnErrorStops(t *testing.T) {
	cols := []string{"id", "v"}
	rows := testRows(100000)
	want := fmt.Errorf("boom")
	calls := 0
	var mu sync.Mutex

	done := make(chan error, 1)
	go func() {
		done <- scanPipeline(context.Background(), cols, 8, sliceProducer(rows), func(m map[string]any) error {
			mu.Lock()
			calls++
			mu.Unlock()
			return want
		})
	}()

	select {
	case err := <-done:
		if err != want {
			t.Fatalf("err = %v, want %v", err, want)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("scanPipeline did not return after fn error (goroutine leak/deadlock)")
	}
	mu.Lock()
	if calls != 1 {
		t.Fatalf("fn called %d times after error, want 1", calls)
	}
	mu.Unlock()
}

func TestScanPipelineProducerError(t *testing.T) {
	cols := []string{"id", "v"}
	want := fmt.Errorf("producer failed")
	i := 0
	produce := func() ([]any, bool, error) {
		if i++; i == 10 {
			return nil, false, want
		}
		return []any{int64(i), "v"}, true, nil
	}
	err := scanPipeline(context.Background(), cols, 4, produce, func(map[string]any) error { return nil })
	if err == nil {
		t.Fatal("want producer error")
	}
}

func BenchmarkScanPipeline(b *testing.B) {
	cols := []string{"id", "v", "amount", "active"}
	const n = 20000
	rows := make([][]any, n)
	for i := range rows {
		rows[i] = []any{int64(i), fmt.Sprintf("value-%d", i), float64(i) / 7.0, i%2 == 0}
	}
	for _, workers := range []int{1, 4, 8} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := scanPipeline(context.Background(), cols, workers, sliceProducer(rows),
					func(map[string]any) error { return nil }); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

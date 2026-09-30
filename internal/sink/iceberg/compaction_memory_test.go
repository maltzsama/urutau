package iceberg

// Issue #457: the maintenance Pod is OOM-killed at 5Gi in compaction on
// tables of 150–580 MB of Parquet with ~1.8k equality-delete files. This
// measures the compaction's peak live heap on a CDC-shaped table — one data
// file and one equality-delete file per commit — at a growing commit count,
// to show what the heap grows with. Opt-in: URUTAU_COMPACTION_MEM=1.

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/spec"
)

func TestCompactionPeakHeapOnACDCTable(t *testing.T) {
	if os.Getenv("URUTAU_COMPACTION_MEM") == "" {
		t.Skip("set URUTAU_COMPACTION_MEM=1 to measure")
	}
	rows := 2000
	if v, _ := strconv.Atoi(os.Getenv("URUTAU_COMPACTION_ROWS")); v > 0 {
		rows = v
	}
	commitCounts := []int{50, 100, 200, 400}
	if v, _ := strconv.Atoi(os.Getenv("URUTAU_COMPACTION_COMMITS")); v > 0 {
		commitCounts = []int{v}
	}
	for _, commits := range commitCounts {
		t.Run(fmt.Sprintf("commits=%d", commits), func(t *testing.T) {
			ctx := context.Background()
			s := hadoopSink(t)
			ref := createOrders(t, s)
			w, err := s.Writer(ctx, ref, core.CastPolicy{}, nil)
			if err != nil {
				t.Fatal(err)
			}
			pad := string(make([]byte, 200))
			next := int64(0)
			for c := 0; c < commits; c++ {
				// Half new keys, half updates of earlier ones: each commit
				// carries a data file and an equality-delete file.
				var rs [][3]any
				for i := 0; i < rows; i++ {
					if i%2 == 0 || next == 0 {
						rs = append(rs, [3]any{next, pad, rowchange.OpInsert})
						next++
					} else {
						rs = append(rs, [3]any{int64((c*7919 + i) % int(next)), pad, rowchange.OpUpdate})
					}
				}
				b := wireBatch(t, rs...)
				if err := w.Commit(ctx, b); err != nil {
					t.Fatalf("commit %d: %v", c, err)
				}
				b.Release()
			}
			cfg := fullMaintenance()
			cfg.Compaction = &spec.CompactionConfig{MinInputFiles: 2}
			m := NewMaintainer(s.cat, s.ident("orders"), cfg, nil, nil, nil)

			runtime.GC()
			var base runtime.MemStats
			runtime.ReadMemStats(&base)
			var peak atomic.Uint64
			stop := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				var ms runtime.MemStats
				for {
					select {
					case <-stop:
						return
					case <-time.After(5 * time.Millisecond):
					}
					runtime.ReadMemStats(&ms)
					if ms.HeapInuse > peak.Load() {
						peak.Store(ms.HeapInuse)
					}
				}
			}()
			start := time.Now()
			err = m.RunOnce(ctx, []sink.MaintenanceOp{sink.MaintenanceCompaction})
			close(stop)
			wg.Wait()
			if err != nil {
				t.Fatalf("compaction: %v", err)
			}
			var dataBytes int64
			tbl, _ := s.cat.LoadTable(ctx, s.ident("orders"))
			if snap := tbl.CurrentSnapshot(); snap != nil {
				dataBytes, _ = strconv.ParseInt(snap.Summary.Properties["total-files-size"], 10, 64)
			}
			t.Logf("commits=%d rows/commit=%d: compacted table %d MiB on disk; peak heap above baseline %d MiB in %s",
				commits, rows, dataBytes>>20, (peak.Load()-base.HeapInuse)>>20, time.Since(start).Round(time.Millisecond))
		})
	}
}

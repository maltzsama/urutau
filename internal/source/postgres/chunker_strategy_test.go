package postgres

import (
	"math"
	"testing"
)

func TestCtidRanges(t *testing.T) {
	// 250 pages, 100 per chunk → starts at 0, 100, 200.
	got := ctidRanges(250, 100)
	want := []string{"(0,0)", "(100,0)", "(200,0)"}
	if len(got) != len(want) {
		t.Fatalf("ctidRanges = %v, want %v", got, want)
	}
	for i := range want {
		if got[i][0] != want[i] {
			t.Fatalf("ctidRanges[%d] = %v, want %s", i, got[i][0], want[i])
		}
	}
	// A table smaller than one chunk still yields one start.
	if got := ctidRanges(10, 100); len(got) != 1 || got[0][0] != "(0,0)" {
		t.Fatalf("small table = %v, want [(0,0)]", got)
	}
	// An empty relation still yields one chunk.
	if got := ctidRanges(0, 100); len(got) != 1 {
		t.Fatalf("empty table = %v, want one chunk", got)
	}
}

func TestCtidPartitionedBounds(t *testing.T) {
	// Two leaves: 1000 pages and 200 pages; max 1000. A 100-page budget
	// spreads proportionally, so every page lands in exactly one chunk and
	// the starts are strictly increasing.
	got := ctidPartitionedBoundsPages([]int64{1000, 200}, 1000, 100)
	if len(got) < 2 {
		t.Fatalf("want multiple chunks, got %v", got)
	}
	for i := 1; i < len(got); i++ {
		if got[i][0] == got[i-1][0] {
			t.Fatalf("duplicate start at %d: %v", i, got)
		}
	}
	if got[0][0] != "(0,0)" {
		t.Fatalf("first start = %v, want (0,0)", got[0][0])
	}
}

func TestPartitionNumeric(t *testing.T) {
	c := &Chunker{pk: []string{"id"}}
	got, err := c.partitionNumeric(int64(0), int64(99), 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Fatalf("got %d ranges, want 4", len(got))
	}
	if got[0].Low != nil {
		t.Fatalf("first range low = %v, want nil", got[0].Low)
	}
	if got[3].High != nil {
		t.Fatalf("last range high = %v, want nil (open)", got[3].High)
	}
	// Contiguous: each range's High equals the next's Low.
	for i := 0; i < len(got)-1; i++ {
		if got[i].High == nil || got[i+1].Low == nil || got[i].High[0] != got[i+1].Low[0] {
			t.Fatalf("ranges not contiguous at %d: %+v", i, got)
		}
	}
}

func TestPartitionNumericSmallDomain(t *testing.T) {
	// A domain narrower than the worker count: only the FINAL range may be
	// open-ended; the extra middle ranges must be empty [max,max), never
	// open-ended (an open middle range would swallow every key above max and
	// starve the later workers).
	c := &Chunker{pk: []string{"id"}}
	got, err := c.partitionNumeric(int64(0), int64(2), 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 5 {
		t.Fatalf("got %d ranges, want 5", len(got))
	}
	for i, ch := range got {
		if i < len(got)-1 && ch.High == nil {
			t.Fatalf("range %d is open-ended; only the last may be: %+v", i, got)
		}
	}
	if got[len(got)-1].High != nil {
		t.Fatalf("last range high = %v, want nil (open)", got[len(got)-1].High)
	}
}

func TestPartitionNumericFullDomain(t *testing.T) {
	// The full int64 domain overflows min/step arithmetic; it must not panic
	// or emit more than one open-ended range.
	c := &Chunker{pk: []string{"id"}}
	got, err := c.partitionNumeric(math.MinInt64, math.MaxInt64, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Fatalf("got %d ranges, want 4", len(got))
	}
	open := 0
	for _, ch := range got {
		if ch.High == nil {
			open++
		}
	}
	if open != 1 {
		t.Fatalf("got %d open-ended ranges, want exactly 1: %+v", open, got)
	}
}

func TestTypeDomainRanges(t *testing.T) {
	for _, tc := range []struct {
		name     string
		dataType string
	}{
		{"int", "bigint"},
		{"float", "double precision"},
		{"text", "text"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := typeDomainRanges(tc.dataType, 4)
			if len(got) != 4 {
				t.Fatalf("got %d ranges, want 4", len(got))
			}
			open := 0
			for i, ch := range got {
				if ch.High == nil {
					open++
				}
				if i > 0 && (ch.Low == nil) {
					t.Fatalf("range %d low = nil; only the first may be open: %+v", i, got)
				}
				if i < len(got)-1 && ch.High == nil {
					t.Fatalf("range %d open-ended; only the last may be: %+v", i, got)
				}
			}
			if open != 1 {
				t.Fatalf("got %d open-ended ranges, want exactly 1: %+v", open, got)
			}
		})
	}
}

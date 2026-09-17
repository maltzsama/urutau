package iceberg

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/maltzsama/urutau/core"
)

// stubCatalog is a catalog.Catalog whose LoadTable always fails with
// loadTableErr. Embedding the interface satisfies the remaining methods.
type stubCatalog struct {
	catalog.Catalog
	loadTableErr error
}

func (s *stubCatalog) LoadTable(ctx context.Context, ident table.Identifier) (*table.Table, error) {
	return nil, s.loadTableErr
}

// Properties must distinguish "table does not exist" (treated as not started:
// empty properties, no error) from any other catalog failure (propagated —
// swallowing it silently discards snapshot progress and restarts the backfill
// from scratch, duplicating append-table rows).
func TestPropertiesDistinguishesMissingTableFromErrors(t *testing.T) {
	s := &Sink{}
	ref := core.TableRef{Target: "raw.orders"}

	s.cat = &stubCatalog{loadTableErr: fmt.Errorf("iceberg: load: %w", catalog.ErrNoSuchTable)}
	props, err := s.Properties(context.Background(), ref)
	if err != nil {
		t.Fatalf("Properties(not found) = %v, want empty map and nil error", err)
	}
	if len(props) != 0 {
		t.Fatalf("Properties(not found) = %v, want empty", props)
	}

	boom := errors.New("polaris 503")
	s.cat = &stubCatalog{loadTableErr: boom}
	if _, err = s.Properties(context.Background(), ref); !errors.Is(err, boom) {
		t.Fatalf("Properties(boom) = %v, want wrapped boom", err)
	}
}

// targetFileSizeFrom is the bridge from the spec's byte-size spelling
// ("128Mi") to the int64 bytes WithTargetFileSize wants. A regression here is
// silent — strconv.ParseInt("128Mi") errors and the override is dropped, so
// sink.defaults.targetFileSize would validate and then do nothing — so this
// pins the grammar.
func TestTargetFileSizeFrom(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"", 0}, // unset: no override
		{"128Mi", 128 * 1024 * 1024},
		{"512Mi", 512 * 1024 * 1024},
		{"1Gi", 1024 * 1024 * 1024},
		{"64Ki", 64 * 1024},
		{"4096", 4096}, // bare integer = plain bytes
		{"0", 0},
		{"not-a-size", 0}, // defense: Validate rejects this first
	}
	for _, c := range cases {
		if got := targetFileSizeFrom(c.in); got != c.want {
			t.Errorf("targetFileSizeFrom(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

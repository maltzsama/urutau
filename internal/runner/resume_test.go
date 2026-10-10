package runner

// Coverage for the runner's snapshot-progress helper. The resume resolution
// itself now lives in the shared internal/resume package, tested there; this
// keeps only readSnapshotProgress, which stays in the runner.

import (
	"context"
	"errors"
	"testing"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/sink"
)

type resumeSink struct {
	sink.Sink
	positions map[string]string
	posErr    error
	propsErr  error
}

func (s resumeSink) Position(_ context.Context, ref core.TableRef) (string, error) {
	if s.posErr != nil {
		return "", s.posErr
	}
	return s.positions[ref.Target], nil
}

func (s resumeSink) Properties(context.Context, core.TableRef) (map[string]string, error) {
	if s.propsErr != nil {
		return nil, s.propsErr
	}
	return map[string]string{}, nil
}

func TestReadSnapshotProgress(t *testing.T) {
	ctx := context.Background()
	sp, err := readSnapshotProgress(ctx, &resumeSink{}, core.TableRef{Target: "a"})
	if err != nil {
		t.Fatalf("readSnapshotProgress: %v", err)
	}
	if sp == nil {
		t.Fatal("progress must not be nil")
	}

	if _, err := readSnapshotProgress(ctx, &resumeSink{propsErr: errors.New("boom")}, core.TableRef{Target: "a"}); err == nil {
		t.Fatal("a Properties error must propagate")
	}
}

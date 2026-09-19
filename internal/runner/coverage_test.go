package runner

// Coverage for the runner's resume/progress helpers: resumeFrom folds each
// target's committed position into a MinSafe resume point plus the set of
// tables still needing a snapshot, and readSnapshotProgress reads the
// snapshot state off the sink's table properties.

import (
	"context"
	"errors"
	"testing"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/source"
)

type resumeSource struct{ source.Source }

func (resumeSource) ParsePosition(s string) (position.Position, error) {
	return position.MustLSN(s), nil
}

type badResumeSource struct{ source.Source }

func (badResumeSource) ParsePosition(string) (position.Position, error) {
	return nil, errors.New("bad lsn")
}

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

func TestResumeFrom(t *testing.T) {
	ctx := context.Background()
	refs := []core.TableRef{{Target: "a"}, {Target: "b"}}

	// All committed: MinSafe of the two, nothing to snapshot. Stream b is
	// ahead of the resume point, so it is the crash-recovery replay set.
	snk := &resumeSink{positions: map[string]string{"a": "0/10", "b": "0/20"}}
	pos, needs, recovery, err := resumeFrom(ctx, resumeSource{}, snk, refs)
	if err != nil {
		t.Fatalf("resumeFrom: %v", err)
	}
	if len(needs) != 0 {
		t.Fatalf("needsSnapshot = %v, want none", needs)
	}
	if pos == nil || pos.String() != "0/10" {
		t.Fatalf("resume = %v, want 0/10", pos)
	}
	if len(recovery) != 1 || recovery[0] != "b" {
		t.Fatalf("recovery = %v, want [b]", recovery)
	}

	// One uncommitted: it needs a snapshot, the other sets the resume.
	snk = &resumeSink{positions: map[string]string{"a": "0/10"}}
	pos, needs, _, err = resumeFrom(ctx, resumeSource{}, snk, refs)
	if err != nil {
		t.Fatalf("resumeFrom(partial): %v", err)
	}
	if len(needs) != 1 || needs[0].Target != "b" {
		t.Fatalf("needsSnapshot = %v, want [b]", needs)
	}
	if pos == nil || pos.String() != "0/10" {
		t.Fatalf("resume = %v, want 0/10", pos)
	}

	// Nothing committed: no resume, everything needs a snapshot.
	snk = &resumeSink{positions: map[string]string{}}
	pos, needs, _, err = resumeFrom(ctx, resumeSource{}, snk, refs)
	if err != nil {
		t.Fatalf("resumeFrom(fresh): %v", err)
	}
	if pos != nil || len(needs) != 2 {
		t.Fatalf("resume/needs = %v, %v; want nil, both", pos, needs)
	}

	// A sink read error propagates.
	snk = &resumeSink{posErr: errors.New("catalog down")}
	if _, _, _, err := resumeFrom(ctx, resumeSource{}, snk, refs); err == nil {
		t.Fatal("a Position error must propagate")
	}

	// A bad stored position propagates.
	snk = &resumeSink{positions: map[string]string{"a": "0/10", "b": "0/20"}}
	if _, _, _, err := resumeFrom(ctx, badResumeSource{}, snk, refs); err == nil {
		t.Fatal("a ParsePosition error must propagate")
	}
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

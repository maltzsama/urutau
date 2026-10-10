package snapshot

import (
	"context"
	"errors"
	"testing"

	"github.com/maltzsama/urutau/core"
)

// fakeProps is a Properties/PropsWriter double for the shared prelude tests.
type fakeProps struct {
	props    map[string]string
	propsErr error
	written  map[string]string
	writeErr error
	setCalls int
}

func (f *fakeProps) Properties(context.Context, core.TableRef) (map[string]string, error) {
	if f.propsErr != nil {
		return nil, f.propsErr
	}
	return f.props, nil
}

func (f *fakeProps) SetProperties(_ context.Context, _ core.TableRef, props map[string]string) error {
	if f.writeErr != nil {
		return f.writeErr
	}
	f.setCalls++
	f.written = props
	return nil
}

func TestReadProgress(t *testing.T) {
	ctx := context.Background()
	ref := core.TableRef{Target: "a"}

	sp, err := ReadProgress(ctx, &fakeProps{}, ref)
	if err != nil {
		t.Fatalf("ReadProgress: %v", err)
	}
	if sp == nil || sp.State != StateComplete {
		t.Fatalf("empty properties must read as complete, got %+v", sp)
	}

	if _, err := ReadProgress(ctx, &fakeProps{propsErr: errors.New("boom")}, ref); err == nil {
		t.Fatal("a Properties error must propagate")
	}

	props, err := EncodeSnapshotProgress(&SnapshotProgress{State: StateInProgress, Bounds: [][]any{{int64(1)}}, Pending: []uint32{0}})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	sp, err = ReadProgress(ctx, &fakeProps{props: props}, ref)
	if err != nil {
		t.Fatalf("ReadProgress: %v", err)
	}
	if sp.State != StateInProgress || len(sp.Pending) != 1 {
		t.Fatalf("got %+v", sp)
	}
}

func TestResumable(t *testing.T) {
	if Resumable(nil) {
		t.Fatal("nil progress is not resumable")
	}
	if Resumable(&SnapshotProgress{State: StateInProgress}) {
		t.Fatal("in_progress without bounds is not resumable")
	}
	if Resumable(&SnapshotProgress{State: StateComplete, Bounds: [][]any{{int64(1)}}}) {
		t.Fatal("complete is not resumable")
	}
	if !Resumable(&SnapshotProgress{State: StateInProgress, Bounds: [][]any{{int64(1)}}}) {
		t.Fatal("in_progress with bounds is resumable")
	}
}

func TestMarkComplete(t *testing.T) {
	ctx := context.Background()
	fake := &fakeProps{}
	if err := MarkComplete(ctx, fake, core.TableRef{Target: "a"}); err != nil {
		t.Fatalf("MarkComplete: %v", err)
	}
	if fake.setCalls != 1 || fake.written[PropSnapshotState] != string(StateComplete) {
		t.Fatalf("wrote %v", fake.written)
	}
	if err := MarkComplete(ctx, &fakeProps{writeErr: errors.New("boom")}, core.TableRef{Target: "a"}); err == nil {
		t.Fatal("a SetProperties error must propagate")
	}
}

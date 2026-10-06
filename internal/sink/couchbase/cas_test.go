package couchbase

import (
	"context"
	"testing"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/snapshot"
)

// casKV is fakeKV with optimistic concurrency: it stamps a CAS per document
// and can inject a concurrent write between a caller's read and its replace,
// which is exactly the race the fast-mode control document must survive
// (issue #566).
type casKV struct {
	*fakeKV
	cas          map[string]uint64
	onFirstWrite func()
	firstDone    bool
}

func newCASKV() *casKV {
	return &casKV{fakeKV: newFakeKV(), cas: map[string]uint64{}}
}

func (c *casKV) getCAS(ctx context.Context, id string, out any) (bool, uint64, error) {
	ok, err := c.fakeKV.get(ctx, id, out)
	if err != nil || !ok {
		return ok, 0, err
	}
	return true, c.cas[id], nil
}

func (c *casKV) replaceCAS(ctx context.Context, id string, doc any, cas uint64) error {
	if !c.firstDone && c.onFirstWrite != nil {
		c.firstDone = true
		c.onFirstWrite()
	}
	if c.cas[id] != cas {
		return errCASMismatch
	}
	if err := c.fakeKV.upsert(ctx, id, doc); err != nil {
		return err
	}
	c.cas[id]++
	return nil
}

func concurrentControlWrite(kv *casKV, props map[string]string) {
	_ = kv.fakeKV.upsert(context.Background(), controlKey, &controlDoc{Properties: props})
	kv.cas[controlKey]++
}

// A commit that races a coordinator property write must retry and keep both:
// the position it commits and the property the coordinator recorded.
func TestCommitFastCASKeepsConcurrentProperties(t *testing.T) {
	kv := newCASKV()
	kv.onFirstWrite = func() {
		concurrentControlWrite(kv, map[string]string{snapshot.PropSnapshotState: "in_progress"})
	}
	w := newTableWriter(kv, nil, plan(metaIngest()), "", nil)

	if err := w.Commit(context.Background(), upsertBatch("g1:42", 1)); err != nil {
		t.Fatalf("commit: %v", err)
	}
	props, err := propertiesOf(context.Background(), kv)
	if err != nil {
		t.Fatal(err)
	}
	if props[snapshot.PropSnapshotState] != "in_progress" {
		t.Fatalf("the commit discarded the concurrent snapshot property: %v", props)
	}
	var ctrl controlDoc
	if found, err := kv.get(context.Background(), controlKey, &ctrl); err != nil || !found {
		t.Fatalf("control doc: found=%v err=%v", found, err)
	}
	if ctrl.Position != "g1:42" {
		t.Fatalf("control position = %q, want g1:42", ctrl.Position)
	}
}

// The mirror: a property write that races a commit must keep the commit's
// position.
func TestSetPropertiesCASKeepsConcurrentPosition(t *testing.T) {
	kv := newCASKV()
	kv.onFirstWrite = func() {
		concurrentControlWrite(kv, nil) // a bare concurrent write bumps the CAS
		_ = kv.fakeKV.upsert(context.Background(), controlKey,
			&controlDoc{Position: "g1:99"})
	}
	if err := setProperties(context.Background(), kv, core.TableRef{Target: "raw.t"},
		map[string]string{snapshot.PropSnapshotState: "complete"}, time.Now); err != nil {
		t.Fatalf("setProperties: %v", err)
	}
	var ctrl controlDoc
	if found, err := kv.get(context.Background(), controlKey, &ctrl); err != nil || !found {
		t.Fatalf("control doc: found=%v err=%v", found, err)
	}
	if ctrl.Position != "g1:99" {
		t.Fatalf("position lost: %q, want g1:99", ctrl.Position)
	}
	if ctrl.Properties[snapshot.PropSnapshotState] != "complete" {
		t.Fatalf("property not merged: %v", ctrl.Properties)
	}
}

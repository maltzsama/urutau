package coordk8s

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/maltzsama/urutau/spec"
)

// fakeHost is a coordinator with one owner per table that records the scales
// asked of it.
type fakeHost struct {
	tables []spec.Table
	owners map[string]int
	scaled map[string]int
	fail   error
}

func (h *fakeHost) Pipeline() (string, bool) { return "p", true }
func (h *fakeHost) Tables() []spec.Table     { return h.tables }
func (h *fakeHost) Owners(target string) (int, bool) {
	n, ok := h.owners[target]
	return n, ok
}

func (h *fakeHost) ScaleTable(_ context.Context, target string, n int) error {
	if h.fail != nil {
		return h.fail
	}
	h.scaled[target] = n
	h.owners[target] = n
	return nil
}

func autoscalerFor(t *testing.T, h *fakeHost, replicas int32) *Autoscaler {
	t.Helper()
	sts := WorkerStatefulSet(spec.WorkerGroupPrefix("p", "raw.orders"), "ns", metav1.OwnerReference{}, sampleTemplate(), replicas)
	var cluster Cluster
	cluster.Use(fake.NewSimpleClientset(sts), "ns")
	return NewAutoscaler(&cluster, h, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func ordersHost() *fakeHost {
	return &fakeHost{
		tables: []spec.Table{{Target: "raw.orders"}},
		owners: map[string]int{"raw.orders": 1},
		scaled: map[string]int{},
	}
}

// The autoscaler asks for a re-slice when the StatefulSet's replica count and
// the table's owner count diverge, and for nothing once they agree.
func TestAutoscalerFollowsTheReplicaCount(t *testing.T) {
	h := ordersHost()
	a := autoscalerFor(t, h, 3)

	a.Reconcile(context.Background())
	if h.scaled["raw.orders"] != 3 {
		t.Fatalf("scaled to %d, want 3", h.scaled["raw.orders"])
	}

	delete(h.scaled, "raw.orders")
	a.Reconcile(context.Background())
	if _, again := h.scaled["raw.orders"]; again {
		t.Fatal("a table already at its replica count was scaled again")
	}
}

// A zero-replica StatefulSet keeps one owner: the range stays owned and its
// queue buffers until the pod returns.
func TestAutoscalerNeverScalesBelowOneOwner(t *testing.T) {
	h := ordersHost()
	h.owners["raw.orders"] = 2
	a := autoscalerFor(t, h, 0)

	a.Reconcile(context.Background())
	if h.scaled["raw.orders"] != 1 {
		t.Fatalf("scaled to %d, want 1", h.scaled["raw.orders"])
	}
}

// A failed scale holds the table for the cooldown, and the next pass does
// not try it again.
func TestAutoscalerHoldsATableAfterAFailedScale(t *testing.T) {
	h := ordersHost()
	h.fail = errors.New("drain timed out")
	a := autoscalerFor(t, h, 3)

	before := time.Now()
	a.Reconcile(context.Background())
	until, held := a.HeldUntil("raw.orders")
	if !held || until.Before(before.Add(scaleFailureCooldown)) {
		t.Fatalf("held = %v until %v, want a hold of %v", held, until, scaleFailureCooldown)
	}

	h.fail = nil
	a.Reconcile(context.Background())
	if _, scaled := h.scaled["raw.orders"]; scaled {
		t.Fatal("a held table was scaled before its cooldown elapsed")
	}
}

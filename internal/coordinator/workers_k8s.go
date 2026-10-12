package coordinator

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/maltzsama/urutau/internal/coordk8s"
	"github.com/maltzsama/urutau/spec"
)

// The coordinator's Kubernetes side lives in internal/coordk8s. This file is
// the wiring: the coordinator's cluster handle, and the view of itself the
// autoscaler works through.

// provisionWorkers ensures the worker StatefulSets for the given worker
// names (see coordk8s.Cluster.Provision).
func (c *Coordinator) provisionWorkers(ctx context.Context, workerTarget map[string]string) error {
	if !coordk8s.WorkerPodTemplateAvailable(workerTarget) {
		return nil // provisioning is off: no API call, and no spec needed
	}
	return c.k8s.Provision(ctx, c.cfg.Spec.Pipeline, workerTarget, c.log)
}

// workerClientset returns the coordinator's cached in-cluster Kubernetes
// client, its namespace, and the ownerReference new worker workloads carry.
func (c *Coordinator) workerClientset(ctx context.Context) (kubernetes.Interface, string, metav1.OwnerReference, error) {
	return c.k8s.Client(ctx)
}

// scaleReconcileLoop keeps the coordinator's routing in step with the worker
// StatefulSets' replica counts. It runs only when the operator rendered
// worker pod templates (the same switch that turns provisioning on); a
// pipeline without Kubernetes workers makes zero API calls.
func (c *Coordinator) scaleReconcileLoop(ctx context.Context) {
	if !c.workerK8s {
		return
	}
	c.autoscaler().Run(ctx)
}

// reconcileReplicas runs one pass of the replica reconcile.
func (c *Coordinator) reconcileReplicas(ctx context.Context) {
	c.autoscaler().Reconcile(ctx)
}

// autoscaler returns the coordinator's autoscaler, built on first use.
func (c *Coordinator) autoscaler() *coordk8s.Autoscaler {
	c.scaleOnce.Do(func() {
		c.scale = coordk8s.NewAutoscaler(&c.k8s, scaleHost{c}, c.log)
	})
	return c.scale
}

// scaleHost is the coordinator as the autoscaler sees it.
type scaleHost struct{ c *Coordinator }

func (h scaleHost) Pipeline() (string, bool) {
	if h.c.cfg.Spec == nil {
		return "", false
	}
	return h.c.cfg.Spec.Pipeline, true
}

func (h scaleHost) Tables() []spec.Table { return h.c.tablesOrSpec() }

func (h scaleHost) Owners(target string) (int, bool) {
	owners, ok := h.c.loadRouting().ownersOf(target)
	return len(owners), ok
}

func (h scaleHost) ScaleTable(ctx context.Context, target string, n int) error {
	return h.c.ScaleTable(ctx, target, n)
}

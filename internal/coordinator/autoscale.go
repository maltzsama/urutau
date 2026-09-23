package coordinator

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/maltzsama/urutau/spec"
)

// scaleReconcileInterval is how often the coordinator re-reads each table's
// worker StatefulSet replica count and re-slices routing to match. KEDA (via
// the operator's ScaledObject) owns that count; the coordinator only follows
// it (issue #298).
const scaleReconcileInterval = 5 * time.Second

// scaleFailureCooldown is how long the reconcile loop waits after a failed
// scale before trying again. A scale pauses the table's input for the whole
// drain, so retrying every tick would hold the input paused far more than it
// runs and the backlog the drain waits on would never clear.
const scaleFailureCooldown = time.Minute

// scaleReconcileLoop keeps the coordinator's routing in step with the worker
// StatefulSets' replica counts. It runs only when the operator rendered
// worker pod templates (the same switch that turns provisioning on); a
// pipeline without Kubernetes workers makes zero API calls. The loop is
// serial: a re-slice can take up to ScaleDrainTimeout, and the next tick
// waits for it rather than piling up.
func (c *Coordinator) scaleReconcileLoop(ctx context.Context) {
	if !c.workerK8s {
		return
	}
	// Converge once at start: a coordinator restart after a KEDA scale must
	// not wait a full interval before routing matches the running pods.
	c.reconcileReplicas(ctx)
	t := time.NewTicker(scaleReconcileInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.reconcileReplicas(ctx)
		}
	}
}

// reconcileReplicas reads each table's worker StatefulSet and re-slices the
// table to its replica count when the two diverge. A table with no
// StatefulSet (Kubernetes provisioning off for it, or not yet created) is
// left alone. The replica count is the single source of truth: the
// coordinator never writes it, so it cannot fight KEDA or an operator.
func (c *Coordinator) reconcileReplicas(ctx context.Context) {
	if c.cfg.Spec == nil {
		return
	}
	if c.scaleRetryAfter.After(time.Now()) {
		return // a recent scale failed; let the table drain before retrying
	}
	cs, ns, _, err := c.workerClientset(ctx)
	if err != nil {
		c.log.Debug("coordinator: scale reconcile skipped", "err", err)
		return
	}
	for _, t := range c.cfg.Spec.Tables {
		name := spec.WorkerGroupPrefix(c.cfg.Spec.Pipeline, t.Target)
		sts, err := cs.AppsV1().StatefulSets(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			continue // not provisioned for this table
		}
		if sts.Spec.Replicas == nil {
			continue
		}
		want := int(*sts.Spec.Replicas)
		// Routing always has at least one owner: a zero-replica StatefulSet
		// (KEDA scaled to zero) keeps the range owned, and its queue buffers
		// until the pod returns.
		if want < 1 {
			want = 1
		}
		owners, ok := c.loadRouting().ownersOf(t.Target)
		if !ok || len(owners) == want {
			continue
		}
		if err := c.ScaleTable(ctx, t.Target, want); err != nil {
			c.log.Warn("coordinator: scale reconcile", "table", t.Target, "replicas", want, "err", err)
			c.scaleRetryAfter = time.Now().Add(scaleFailureCooldown)
			return
		}
	}
}

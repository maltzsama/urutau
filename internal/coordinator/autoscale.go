package coordinator

import (
	"context"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	// The cooldown map belongs to this loop alone; it is made here, where
	// that ownership starts, not checked for on every pass.
	if c.scaleRetryAfter == nil {
		c.scaleRetryAfter = map[string]time.Time{}
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
	cs, ns, _, err := c.workerClientset(ctx)
	if err != nil {
		c.log.Debug("coordinator: scale reconcile skipped", "err", err)
		return
	}
	tables := c.tablesOrSpec()
	c.pruneScaleRetry(tables)
	for _, t := range tables {
		if c.scaleRetryAfter[t.Target].After(time.Now()) {
			continue // this table's recent scale failed; let it drain first
		}
		name := spec.WorkerGroupPrefix(c.cfg.Spec.Pipeline, t.Target)
		sts, err := cs.AppsV1().StatefulSets(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			// Not found is the quiet case: nothing provisioned for this
			// table. Anything else (forbidden, timeout, the API server
			// down) is the reconcile going blind: the StatefulSet may be
			// scaled while routing never follows. One table's error does
			// not stop the others.
			if !apierrors.IsNotFound(err) {
				c.log.Warn("coordinator: scale reconcile: cannot read the worker statefulset",
					"table", t.Target, "statefulset", name, "err", err)
			}
			continue
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
			if c.scaleRetryAfter == nil {
				c.scaleRetryAfter = map[string]time.Time{} // a pass run outside the loop (tests)
			}
			c.scaleRetryAfter[t.Target] = time.Now().Add(scaleFailureCooldown)
		}
	}
}

// pruneScaleRetry drops the cooldown of every table that is no longer in the
// pipeline. A cooldown is written when a scale fails and nothing else ever
// removed it, so a table that left the spec kept its entry for the life of
// the process.
func (c *Coordinator) pruneScaleRetry(tables []spec.Table) {
	if len(c.scaleRetryAfter) == 0 {
		return
	}
	live := make(map[string]struct{}, len(tables))
	for _, t := range tables {
		live[t.Target] = struct{}{}
	}
	for target := range c.scaleRetryAfter {
		if _, ok := live[target]; !ok {
			delete(c.scaleRetryAfter, target)
		}
	}
}

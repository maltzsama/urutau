package coordk8s

import (
	"context"
	"log/slog"
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

// ScaleHost is what the autoscaler needs from the coordinator: the pipeline
// and its tables, how many owners a table has now, and the re-slice itself.
type ScaleHost interface {
	// Pipeline is the pipeline's name; false when no spec is loaded.
	Pipeline() (string, bool)
	Tables() []spec.Table
	// Owners is the table's current owner count; false when the table has
	// no routing entry.
	Owners(target string) (int, bool)
	ScaleTable(ctx context.Context, target string, n int) error
}

// Autoscaler keeps the coordinator's routing in step with the worker
// StatefulSets' replica counts.
type Autoscaler struct {
	cluster *Cluster
	host    ScaleHost
	log     *slog.Logger
	// retryAfter suppresses one table's replica reconcile until the given
	// time, after a scale of it failed. It belongs to the reconcile loop
	// alone, which is serial.
	retryAfter map[string]time.Time
}

// NewAutoscaler builds an autoscaler over the cluster for the host.
func NewAutoscaler(cluster *Cluster, host ScaleHost, log *slog.Logger) *Autoscaler {
	return &Autoscaler{cluster: cluster, host: host, log: log, retryAfter: map[string]time.Time{}}
}

// Run reconciles once and then on every interval until ctx ends. The loop is
// serial: a re-slice can take up to ScaleDrainTimeout, and the next tick
// waits for it rather than piling up.
func (a *Autoscaler) Run(ctx context.Context) {
	// Converge once at start: a coordinator restart after a KEDA scale must
	// not wait a full interval before routing matches the running pods.
	a.Reconcile(ctx)
	t := time.NewTicker(scaleReconcileInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			a.Reconcile(ctx)
		}
	}
}

// Reconcile reads each table's worker StatefulSet and re-slices the table to
// its replica count when the two diverge. A table with no StatefulSet
// (Kubernetes provisioning off for it, or not yet created) is left alone.
// The replica count is the single source of truth: the coordinator never
// writes it, so it cannot fight KEDA or an operator.
func (a *Autoscaler) Reconcile(ctx context.Context) {
	pipeline, ok := a.host.Pipeline()
	if !ok {
		return
	}
	cs, ns, _, err := a.cluster.Client(ctx)
	if err != nil {
		a.log.Debug("coordinator: scale reconcile skipped", "err", err)
		return
	}
	tables := a.host.Tables()
	a.prune(tables)
	for _, t := range tables {
		if a.retryAfter[t.Target].After(time.Now()) {
			continue // this table's recent scale failed; let it drain first
		}
		name := spec.WorkerGroupPrefix(pipeline, t.Target)
		sts, err := cs.AppsV1().StatefulSets(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			// Not found is the quiet case: nothing provisioned for this
			// table. Anything else (forbidden, timeout, the API server
			// down) is the reconcile going blind: the StatefulSet may be
			// scaled while routing never follows. One table's error does
			// not stop the others.
			if !apierrors.IsNotFound(err) {
				a.log.Warn("coordinator: scale reconcile: cannot read the worker statefulset",
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
		owners, ok := a.host.Owners(t.Target)
		if !ok || owners == want {
			continue
		}
		if err := a.host.ScaleTable(ctx, t.Target, want); err != nil {
			a.log.Warn("coordinator: scale reconcile", "table", t.Target, "replicas", want, "err", err)
			a.retryAfter[t.Target] = time.Now().Add(scaleFailureCooldown)
		}
	}
}

// Hold suppresses a table's reconcile until the given time, as a failed
// scale does.
func (a *Autoscaler) Hold(target string, until time.Time) { a.retryAfter[target] = until }

// HeldUntil reports the time a table's reconcile is suppressed until, if it
// is.
func (a *Autoscaler) HeldUntil(target string) (time.Time, bool) {
	t, ok := a.retryAfter[target]
	return t, ok
}

// prune drops the cooldown of every table that is no longer in the pipeline.
// A cooldown is written when a scale fails and nothing else ever removed it,
// so a table that left the spec kept its entry for the life of the process.
func (a *Autoscaler) prune(tables []spec.Table) {
	if len(a.retryAfter) == 0 {
		return
	}
	live := make(map[string]struct{}, len(tables))
	for _, t := range tables {
		live[t.Target] = struct{}{}
	}
	for target := range a.retryAfter {
		if _, ok := live[target]; !ok {
			delete(a.retryAfter, target)
		}
	}
}

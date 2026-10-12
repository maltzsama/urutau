// Package coordk8s is the coordinator's Kubernetes side: provisioning the
// worker StatefulSets, following their replica counts, and the ephemeral
// maintenance worker Pods.
//
// The coordinator, not the operator, creates the worker StatefulSets it
// needs — the same driver→executor relationship Spark uses (the driver
// clones the executor pod template and creates the Pod; it never assembles
// a PodSpec field by field). The operator (see internal/operator) renders
// one worker Pod template per table into the coordinator's own ConfigMap;
// this package only ever reads that template, clones it, and stamps a
// derived --name onto it. It never builds credentials, resources, or an
// image itself.
//
// It depends on the spec and on client-go. What it needs from the
// coordinator it takes through ScaleHost.
package coordk8s

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/maltzsama/urutau/spec"
)

// Cluster is the coordinator's cached in-cluster Kubernetes client, with its
// namespace and the ownerReference new worker workloads carry. The zero
// value is ready: the client is built on first use.
type Cluster struct {
	mu     sync.Mutex
	client kubernetes.Interface
	ns     string
	owner  metav1.OwnerReference
}

// Client returns the client, its namespace, and the ownerReference. It is
// built once and reused: the scale reconcile loop calls it on every tick,
// and rebuilding the client each time would re-read the service account
// files and re-GET the coordinator Pod. A failure is not cached, so a
// transient boot error does not permanently disable provisioning.
func (c *Cluster) Client(ctx context.Context) (kubernetes.Interface, string, metav1.OwnerReference, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.client != nil {
		return c.client, c.ns, c.owner, nil
	}
	cs, ns, owner, err := InClusterClient(ctx)
	if err != nil {
		return nil, "", metav1.OwnerReference{}, err
	}
	c.client, c.ns, c.owner = cs, ns, owner
	return cs, ns, owner, nil
}

// Use installs a client and namespace instead of the in-cluster ones. Tests
// use it to inject a fake clientset.
func (c *Cluster) Use(cs kubernetes.Interface, ns string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.client, c.ns = cs, ns
}

// Provision ensures one StatefulSet exists per table, with replicas equal to
// that table's partition count, using that table's own Pod template rendered
// by the operator into this coordinator's ConfigMap
// ("worker-pod-template.<target>.yaml", next to pipeline.yaml). The
// StatefulSet is named "<pipeline>-<target>", so its pods are named
// "<pipeline>-<target>-<index>" — exactly spec.Table.WorkerGroupNames — and
// KEDA has a single replica count to scale for the whole table (issue #298).
// If no such template file exists for ANY of the given names, Kubernetes
// worker provisioning is off entirely — this is the normal case for every
// pipeline that doesn't set spec.image, and the coordinator makes zero
// Kubernetes API calls.
func (c *Cluster) Provision(ctx context.Context, pipeline string, workerTarget map[string]string, log *slog.Logger) error {
	if len(workerTarget) == 0 {
		return nil
	}
	if !WorkerPodTemplateAvailable(workerTarget) {
		return nil
	}

	clientset, ns, owner, err := c.Client(ctx)
	if err != nil {
		return fmt.Errorf("k8s worker provisioning: %w", err)
	}

	// One StatefulSet per table: its replica count is the number of worker
	// group names the boot path derived for that target.
	replicas := map[string]int{}
	for _, target := range workerTarget {
		replicas[target]++
	}
	targets := make([]string, 0, len(replicas))
	for target := range replicas {
		targets = append(targets, target)
	}
	sort.Strings(targets) // stable order, so boot provisioning is deterministic

	templates := map[string]corev1.PodTemplateSpec{}
	for _, target := range targets {
		tmpl, ok := templates[target]
		if !ok {
			tmpl, err = LoadWorkerPodTemplate(target)
			if err != nil {
				return fmt.Errorf("k8s worker provisioning: %s: %w", target, err)
			}
			templates[target] = tmpl
		}
		name := spec.WorkerGroupPrefix(pipeline, target)
		if err := ensureService(ctx, clientset, ns, workerHeadlessService(name, ns, owner)); err != nil {
			return fmt.Errorf("k8s worker provisioning: %s: %w", name, err)
		}
		sts := WorkerStatefulSet(name, ns, owner, tmpl, int32(replicas[target]))
		if err := ensureStatefulSet(ctx, clientset, ns, sts); err != nil {
			return fmt.Errorf("k8s worker provisioning: %s: %w", name, err)
		}
		log.Info("coordinator: worker statefulset ensured", "table", target, "replicas", replicas[target])
	}
	return nil
}

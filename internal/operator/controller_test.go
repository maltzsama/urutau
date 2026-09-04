package operator

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	urutauv1alpha1 "github.com/maltzsama/urutau/api/v1alpha1"
)

var (
	testEnv *envtest.Environment
	testCtx context.Context
	cli     client.Client
)

func TestMain(m *testing.M) {
	assets := os.Getenv("KUBEBUILDER_ASSETS")
	if assets == "" {
		assets = envtestAssetsPath()
	}
	if assets != "" {
		_ = os.Setenv("KUBEBUILDER_ASSETS", assets)
		testEnv = &envtest.Environment{
			CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd")},
			ErrorIfCRDPathMissing: false,
		}
		cfg, err := testEnv.Start()
		if err == nil {
			testCtx = context.Background()
			sch := runtime.NewScheme()
			if err := urutauv1alpha1.AddToScheme(sch); err == nil {
				if err := clientgoscheme.AddToScheme(sch); err == nil {
					var mgr ctrl.Manager
					mgr, err = ctrl.NewManager(cfg, ctrl.Options{
						Scheme:  sch,
						Metrics: metricsserver.Options{BindAddress: "0"},
					})
					if err == nil {
						if err := (&CoordinatorReconciler{Client: mgr.GetClient(), Image: "urutau:dev"}).SetupWithManager(mgr); err == nil {
							cli = mgr.GetClient()
							go func() { _ = mgr.Start(testCtx) }()
						}
					}
				}
			}
		}
		if testEnv != nil {
			defer func() { _ = testEnv.Stop() }()
		}
	}

	os.Exit(m.Run())
}

// requireEnvtest skips tests that need the live control plane.
func requireEnvtest(t *testing.T) {
	t.Helper()
	if cli == nil {
		t.Skip("envtest control plane unavailable")
	}
}

// envtestAssetsPath resolves the setup-envtest install location without
// shelling out: the default cache dir layout is
// ~/.local/share/kubebuilder-envtest/k8s/<version>-<os>-<arch>. Empty when
// nothing is installed.
func envtestAssetsPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	dir := filepath.Join(home, ".local", "share", "kubebuilder-envtest", "k8s")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return ""
	}
	for _, e := range entries {
		if e.IsDir() {
			return filepath.Join(dir, e.Name())
		}
	}
	return ""
}

func pipelineCR(name, ns string) *urutauv1alpha1.CDCPipeline {
	return &urutauv1alpha1.CDCPipeline{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: urutauv1alpha1.CDCPipelineSpec{
			Definition: urutauv1alpha1.Definition{
				Inline: map[string]any{
					"pipeline": "e2e",
					"source":   map[string]any{"kind": "mysql", "uri": "mysql://repl@mysql:3306/shop"},
					"sink": map[string]any{
						"uri": "http://polaris:8181/api/catalog", "namespace": "raw",
						"warehouse": "quickstart_catalog", "clientId": "root", "clientSecret": "s3cr3t",
					},
					"tables": []any{
						map[string]any{"source": "shop.orders", "target": "raw.orders", "primaryKey": []any{"id"}},
					},
				},
			},
		},
	}
}

func TestReconcilerCreatesCoordinator(t *testing.T) {
	requireEnvtest(t)
	nsName := "test-ops-" + fmt.Sprint(time.Now().UnixNano()%100000)
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: nsName}}
	_ = cli.Create(testCtx, ns)

	cr := pipelineCR("orders", nsName)
	cr.Spec.Coordinator.Snapshot.ChunkSize = 500
	cr.Spec.Coordinator.Supervision.AckTimeout = "30s"
	cr.Spec.Coordinator.MetricsAddr = ":9090"
	cr.Spec.Secrets = urutauv1alpha1.Secrets{Source: "mysql-creds", Catalog: "polaris-creds"}
	if err := cli.Create(testCtx, cr); err != nil {
		t.Fatalf("create CR: %v", err)
	}

	// The reconciler creates the coordinator StatefulSet + ConfigMap.
	sts := &appsv1.StatefulSet{}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		err := cli.Get(testCtx, types.NamespacedName{Name: "orders-coordinator", Namespace: nsName}, sts)
		if err == nil {
			break
		}
		if !apierrors.IsNotFound(err) {
			t.Fatalf("get statefulset: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if sts.Name == "" {
		t.Fatal("coordinator StatefulSet was not created")
	}
	if sts.Spec.Replicas == nil || *sts.Spec.Replicas != 1 {
		t.Fatalf("replicas = %v, want 1", sts.Spec.Replicas)
	}
	t.Logf("reconciler ok: %s created", sts.Name)

	// The ConfigMap carries the full inline spec (parsable, valid), not a
	// bare table list the coordinator could never read.
	cm := &corev1.ConfigMap{}
	if err := cli.Get(testCtx, types.NamespacedName{Name: "orders-coordinator", Namespace: nsName}, cm); err != nil {
		t.Fatalf("get configmap: %v", err)
	}
	if _, ok := cm.Data["pipeline.yaml"]; !ok {
		t.Fatalf("configmap data = %v, want pipeline.yaml", cm.Data)
	}

	// The coordinator command points at the YAML and carries the rendered
	// CoordinatorSpec knobs.
	cmd := strings.Join(sts.Spec.Template.Spec.Containers[0].Command, " ")
	if !strings.Contains(cmd, "--file /etc/urutau/pipeline.yaml") ||
		!strings.Contains(cmd, "--chunk-size 500") ||
		!strings.Contains(cmd, "--ack-timeout 30s") ||
		!strings.Contains(cmd, "--metrics-addr :9090") {
		t.Fatalf("coordinator command = %q, want file + rendered flags", cmd)
	}

	// Referenced secrets are mounted as env with the documented key
	// convention; nothing rides in the ConfigMap.
	envByName := map[string]corev1.EnvVar{}
	for _, e := range sts.Spec.Template.Spec.Containers[0].Env {
		envByName[e.Name] = e
	}
	src := envByName["URUTAU_SOURCE_URI"]
	if src.ValueFrom == nil || src.ValueFrom.SecretKeyRef.Name != "mysql-creds" || src.ValueFrom.SecretKeyRef.Key != "uri" {
		t.Fatalf("URUTAU_SOURCE_URI env = %+v, want secretKeyRef mysql-creds/uri", src)
	}
	sec := envByName["URUTAU_SINK_CLIENT_SECRET"]
	if sec.ValueFrom == nil || sec.ValueFrom.SecretKeyRef.Name != "polaris-creds" || sec.ValueFrom.SecretKeyRef.Key != "clientSecret" {
		t.Fatalf("URUTAU_SINK_CLIENT_SECRET env = %+v, want secretKeyRef polaris-creds/clientSecret", sec)
	}
}

func TestReconcilerStopsAtTerminated(t *testing.T) {
	requireEnvtest(t)
	nsName := "test-term-" + fmt.Sprint(time.Now().UnixNano()%100000)
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: nsName}}
	_ = cli.Create(testCtx, ns)

	cr := pipelineCR("dead", nsName)
	if err := cli.Create(testCtx, cr); err != nil {
		t.Fatalf("create CR: %v", err)
	}
	// Wait for the coordinator to exist first: the initial reconcile must
	// have completed before we flip the status, or a slow CI lets the
	// reconcile read a pre-termination snapshot and create the workload
	// after the delete below — permanently.
	sts := &appsv1.StatefulSet{}
	for i := 0; i < 50; i++ {
		err := cli.Get(testCtx, types.NamespacedName{Name: "dead-coordinator", Namespace: nsName}, sts)
		if err == nil {
			break
		}
		if !apierrors.IsNotFound(err) {
			t.Fatalf("get statefulset: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if sts.Name == "" {
		t.Fatal("coordinator StatefulSet was not created")
	}
	// Status is a subresource: set it via the status endpoint.
	fresh := &urutauv1alpha1.CDCPipeline{}
	if err := cli.Get(testCtx, types.NamespacedName{Name: "dead", Namespace: nsName}, fresh); err != nil {
		t.Fatalf("get CR: %v", err)
	}
	fresh.Status.Terminated = &urutauv1alpha1.Terminated{Reason: "crashloop", At: "now"}
	for i := 0; i < 10; i++ {
		if err := cli.Status().Update(testCtx, fresh); err == nil {
			break
		} else if !apierrors.IsConflict(err) {
			t.Fatalf("set status: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
		if err := cli.Get(testCtx, types.NamespacedName{Name: "dead", Namespace: nsName}, fresh); err != nil {
			t.Fatalf("reget CR: %v", err)
		}
		fresh.Status.Terminated = &urutauv1alpha1.Terminated{Reason: "crashloop", At: "now"}
	}
	// The reconciler reads the manager's cache: wait until the termination
	// is visible there, or the delete-event reconcile below can still see
	// a pre-termination CR and recreate the workload.
	for i := 0; i < 50; i++ {
		if err := cli.Get(testCtx, types.NamespacedName{Name: "dead", Namespace: nsName}, fresh); err != nil {
			t.Fatalf("reget CR: %v", err)
		}
		if fresh.Status.Terminated != nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if fresh.Status.Terminated == nil {
		t.Fatal("terminated status never reached the manager cache")
	}
	if err := cli.Delete(testCtx, sts); err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("delete statefulset: %v", err)
	}

	// No coordinator workload may reappear for a terminated pipeline.
	// Poll: the deletion itself is asynchronous through the cache.
	deadline := time.Now().Add(10 * time.Second)
	for {
		err := cli.Get(testCtx, types.NamespacedName{Name: "dead-coordinator", Namespace: nsName}, sts)
		if apierrors.IsNotFound(err) {
			break
		}
		if err != nil {
			t.Fatalf("get statefulset: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("terminated pipeline got a coordinator: statefulset still present after 10s")
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Log("terminated ok: no coordinator recreated")
}

func TestWebhookRejectsEmptyOrAmbiguousDefinition(t *testing.T) {
	v := &pipelineValidator{}

	// Zero definitions: rejected.
	cr := pipelineCR("empty", "test-ops-unique")
	cr.Spec.Definition.Inline = nil
	if _, err := v.ValidateCreate(testCtx, cr); err == nil {
		t.Fatal("webhook accepted a pipeline with no definition")
	}

	// Two definitions: rejected.
	cr2 := pipelineCR("both", "test-ops-unique")
	cr2.Spec.Definition.Image = "urutau-runtime:dev"
	if _, err := v.ValidateCreate(testCtx, cr2); err == nil {
		t.Fatal("webhook accepted image AND inline")
	}

	// A valid inline definition with an invalid spec is rejected by the
	// same validation the coordinator runs.
	cr3 := pipelineCR("bad", "test-ops-unique")
	cr3.Spec.Definition.Inline = map[string]any{
		"pipeline": "bad",
		"source":   map[string]any{"kind": "mysql", "uri": "mysql://u@m:3306/shop"},
		"sink":     map[string]any{"uri": "http://polaris:8181/api/catalog"},
		"tables":   []any{map[string]any{"source": "shop.orders", "target": "raw.orders"}},
	}
	if _, err := v.ValidateCreate(testCtx, cr3); err == nil {
		t.Fatal("webhook accepted an inline spec without primaryKey on an upsert table")
	}

	// The well-formed CR passes.
	ok := pipelineCR("ok", "test-ops-unique")
	if _, err := v.ValidateCreate(testCtx, ok); err != nil {
		t.Fatalf("webhook rejected a valid pipeline: %v", err)
	}
}

var _ = client.IgnoreNotFound

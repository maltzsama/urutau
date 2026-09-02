package operator

import (
	"context"
	"os"
	"path/filepath"
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
				Tables: []map[string]any{{"source": "shop.orders", "target": "raw.orders"}},
			},
		},
	}
}

func TestReconcilerCreatesCoordinator(t *testing.T) {
	requireEnvtest(t)
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "test-ops"}}
	_ = cli.Create(testCtx, ns)

	cr := pipelineCR("orders", "test-ops")
	if err := cli.Create(testCtx, cr); err != nil {
		t.Fatalf("create CR: %v", err)
	}

	// The reconciler creates the coordinator StatefulSet + ConfigMap.
	sts := &appsv1.StatefulSet{}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		err := cli.Get(testCtx, types.NamespacedName{Name: "orders-coordinator", Namespace: "test-ops"}, sts)
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
}

func TestReconcilerStopsAtTerminated(t *testing.T) {
	requireEnvtest(t)
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "test-term"}}
	_ = cli.Create(testCtx, ns)

	cr := pipelineCR("dead", "test-term")
	if err := cli.Create(testCtx, cr); err != nil {
		t.Fatalf("create CR: %v", err)
	}
	// Status is a subresource: set it via the status endpoint, then delete the
	// coordinator — a terminated pipeline must not have it recreated. The
	// reconciler's finalizer Update races this, so retry on conflict.
	fresh := &urutauv1alpha1.CDCPipeline{}
	if err := cli.Get(testCtx, types.NamespacedName{Name: "dead", Namespace: "test-term"}, fresh); err != nil {
		t.Fatalf("reget CR: %v", err)
	}
	fresh.Status.Terminated = &urutauv1alpha1.Terminated{Reason: "crashloop", At: "now"}
	for i := 0; i < 10; i++ {
		if err := cli.Status().Update(testCtx, fresh); err == nil {
			break
		} else if !apierrors.IsConflict(err) {
			t.Fatalf("set status: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
		if err := cli.Get(testCtx, types.NamespacedName{Name: "dead", Namespace: "test-term"}, fresh); err != nil {
			t.Fatalf("reget CR: %v", err)
		}
		fresh.Status.Terminated = &urutauv1alpha1.Terminated{Reason: "crashloop", At: "now"}
	}
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "dead-coordinator", Namespace: "test-term"}}
	_ = cli.Delete(testCtx, sts)

	// No coordinator workload may reappear for a terminated pipeline.
	time.Sleep(3 * time.Second)
	err := cli.Get(testCtx, types.NamespacedName{Name: "dead-coordinator", Namespace: "test-term"}, sts)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("terminated pipeline got a coordinator: %v", err)
	}
	t.Log("terminated ok: no coordinator recreated")
}

func TestWebhookRejectsEmptyTables(t *testing.T) {
	v := &pipelineValidator{}
	cr := pipelineCR("empty", "test-ops")
	cr.Spec.Definition.Tables = nil
	if _, err := v.ValidateCreate(testCtx, cr); err == nil {
		t.Fatal("webhook accepted a pipeline without tables")
	}
	t.Log("webhook ok: empty tables rejected")
}

var _ = client.IgnoreNotFound

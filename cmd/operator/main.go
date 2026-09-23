package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	urutauv1alpha1 "github.com/maltzsama/urutau/api/v1alpha1"
	_ "github.com/maltzsama/urutau/internal/builtin" // register built-in drivers via init()
	"github.com/maltzsama/urutau/internal/operator"
)

var scheme = runtime.NewScheme()

// leaderElectionID names the coordination.k8s.io Lease the manager elects
// through. Stable across releases: changing it makes every replica fight for
// a fresh lease.
const leaderElectionID = "urutau-operator-lock"

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(urutauv1alpha1.AddToScheme(scheme))
}

func main() {
	if err := run(); err != nil {
		ctrl.Log.WithName("setup").Error(err, "fatal")
		os.Exit(1)
	}
}

func run() error {
	var (
		metricsAddr     string
		probeAddr       string
		image           string
		fieldManager    string
		enableWebhook   bool
		watchNamespaces string
		kedaPromAddr    string
		kedaThreshold   string
	)
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "metrics endpoint")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "health probe endpoint")
	flag.StringVar(&image, "coordinator-image", "", "coordinator container image (required)")
	flag.StringVar(&fieldManager, "field-manager", operator.DefaultFieldManager, "Server-Side Apply field manager name (must be unique per controller managing the same objects)")
	flag.BoolVar(&enableWebhook, "enable-webhook", true, "enable the admission webhook")
	flag.StringVar(&watchNamespaces, "watch-namespaces", "", "comma-separated namespaces to watch (empty = all namespaces; each watched namespace needs its own RoleBinding, see config/multi-tenant)")
	flag.StringVar(&kedaPromAddr, "keda-prometheus-address", "", "Prometheus server address for KEDA ScaledObjects (empty disables worker autoscaling)")
	flag.StringVar(&kedaThreshold, "keda-threshold", "30", "per-replica backlog target (outstanding batches) for KEDA ScaledObjects")
	opts := zap.Options{Development: false}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	// SetLogger must precede WithName: ctrl.Log.WithName captures the logger
	// in effect at call time, so naming first would bind the pre-zap default
	// and silently drop every setupLog record.
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	setupLog := ctrl.Log.WithName("setup")

	if image == "" {
		return fmt.Errorf("coordinator-image must not be empty")
	}

	cfg, err := ctrl.GetConfig()
	if err != nil {
		return fmt.Errorf("unable to load kubeconfig: %w", err)
	}
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		// Only the elected leader reconciles; the other replicas stay ready
		// as hot standbys. Without this, two replicas would both reconcile —
		// a second leader also needs the coordination.k8s.io/leases RBAC.
		LeaderElection:   true,
		LeaderElectionID: leaderElectionID,
		Cache:            cache.Options{DefaultNamespaces: namespaceConfig(watchNamespaces)},
	})
	if err != nil {
		return fmt.Errorf("unable to start manager: %w", err)
	}

	// One reconciler instance serves both the controller and the webhook so
	// any future state (caches, limits) is shared, not duplicated.
	r := &operator.CoordinatorReconciler{
		Client:                mgr.GetClient(),
		Image:                 image,
		FieldManager:          fieldManager,
		KEDAPrometheusAddress: kedaPromAddr,
		KEDAThreshold:         kedaThreshold,
	}
	if err := r.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("unable to create controller: %w", err)
	}
	if enableWebhook {
		if err := r.SetupWebhookWithManager(mgr); err != nil {
			return fmt.Errorf("unable to create webhook: %w", err)
		}
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return fmt.Errorf("unable to set up health check: %w", err)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return fmt.Errorf("unable to set up ready check: %w", err)
	}

	setupLog.Info("starting manager",
		"coordinatorImage", image,
		"webhookEnabled", enableWebhook,
		"metricsAddr", metricsAddr,
		"probeAddr", probeAddr,
		"watchNamespaces", watchNamespaces,
		"kedaPrometheusAddress", kedaPromAddr,
	)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		return fmt.Errorf("problem running manager: %w", err)
	}
	return nil
}

// namespaceConfig turns a comma-separated namespace list into the manager
// cache's DefaultNamespaces map, scoping every watch to the managed
// namespaces so the operator needs RBAC only there (the per-tenant
// RoleBinding model, config/multi-tenant) instead of cluster-wide. An empty
// list returns nil, which leaves the cache watching all namespaces — the
// cluster-wide single-tenant default.
func namespaceConfig(csv string) map[string]cache.Config {
	var out map[string]cache.Config
	for _, ns := range strings.Split(csv, ",") {
		ns = strings.TrimSpace(ns)
		if ns == "" {
			continue
		}
		if out == nil {
			out = map[string]cache.Config{}
		}
		out[ns] = cache.Config{}
	}
	return out
}

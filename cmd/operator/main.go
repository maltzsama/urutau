package main

import (
	"flag"
	"fmt"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	urutauv1alpha1 "github.com/maltzsama/urutau/api/v1alpha1"
	_ "github.com/maltzsama/urutau/internal/builtin" // register built-in drivers via init()
	"github.com/maltzsama/urutau/internal/operator"
)

var scheme = runtime.NewScheme()

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
		metricsAddr   string
		probeAddr     string
		image         string
		enableWebhook bool
	)
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "metrics endpoint")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "health probe endpoint")
	flag.StringVar(&image, "coordinator-image", "", "coordinator container image (required)")
	flag.BoolVar(&enableWebhook, "enable-webhook", true, "enable the admission webhook")
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
	})
	if err != nil {
		return fmt.Errorf("unable to start manager: %w", err)
	}

	// One reconciler instance serves both the controller and the webhook so
	// any future state (caches, limits) is shared, not duplicated.
	r := &operator.CoordinatorReconciler{
		Client: mgr.GetClient(),
		Image:  image,
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
	)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		return fmt.Errorf("problem running manager: %w", err)
	}
	return nil
}

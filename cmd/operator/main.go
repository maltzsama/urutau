package main

import (
	"flag"
	"os"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	urutauv1alpha1 "github.com/maltzsama/urutau/api/v1alpha1"
	"github.com/maltzsama/urutau/internal/operator"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(urutauv1alpha1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
}

func main() {
	var (
		metricsAddr   string
		probeAddr     string
		image         string
		enableWebhook bool
	)
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "metrics endpoint")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "health probe endpoint")
	flag.StringVar(&image, "coordinator-image", "urutau:latest", "coordinator container image")
	flag.BoolVar(&enableWebhook, "enable-webhook", false, "enable the admission webhook")
	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
	})
	if err != nil {
		os.Exit(1)
	}

	if err := (&operator.CoordinatorReconciler{
		Client: mgr.GetClient(),
		Image:  image,
	}).SetupWithManager(mgr); err != nil {
		setupLog := ctrl.Log.WithName("setup")
		setupLog.Error(err, "unable to create controller")
		os.Exit(1)
	}

	if enableWebhook {
		if err := (&operator.CoordinatorReconciler{
			Client: mgr.GetClient(),
			Image:  image,
		}).SetupWebhookWithManager(mgr); err != nil {
			ctrl.Log.WithName("setup").Error(err, "unable to create webhook")
			os.Exit(1)
		}
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		os.Exit(1)
	}

	ctrl.Log.WithName("setup").Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		os.Exit(1)
	}
}

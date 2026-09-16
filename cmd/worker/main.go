package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/maltzsama/urutau/driver"
	_ "github.com/maltzsama/urutau/internal/builtin" // register built-in drivers via init()
	"github.com/maltzsama/urutau/internal/grpctls"
	"github.com/maltzsama/urutau/internal/logging"
	"github.com/maltzsama/urutau/internal/plugin/flightwrap"
	"github.com/maltzsama/urutau/internal/worker"
	"github.com/maltzsama/urutau/sink"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "urutau-worker:", err)
		os.Exit(1)
	}
}

func run() error {
	root := &cobra.Command{
		Use:           "urutau-worker",
		Short:         "Urutau worker — Iceberg writer fed by the coordinator's Flight stream",
		SilenceUsage:  true,
		SilenceErrors: true, // main prints the single prefixed error line
	}
	root.AddCommand(runCmd())

	// SIGINT/SIGTERM cancel the command context: the remote session shuts
	// down gracefully — buffered rows drain, the coordinator is told —
	// instead of the process dying at SIGKILL.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return root.ExecuteContext(ctx)
}

// workerFlags are the raw CLI values. config() turns them into a validated
// worker.RemoteConfig, keeping the RunE a thin wrapper.
type workerFlags struct {
	coordinator  string
	name         string
	catalogURI   string
	warehouse    string
	clientID     string
	clientSecret string
	scope        string
	namespace    string
	maxRows      int
	maxInterval  time.Duration
	metricsAddr  string
	pluginPaths  []string
	tlsCert      string
	tlsKey       string
	tlsCA        string
	logLevel     string
	logFormat    string
	// maintenance mode (the coordinator launches the worker as an ephemeral
	// per-table maintenance worker).
	maintenance bool
}

func runCmd() *cobra.Command {
	f := &workerFlags{}
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Connect to a coordinator and write its stream to Iceberg",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			// Load dynamic plugins before anything else.
			for _, p := range f.pluginPaths {
				if err := driver.LoadPlugin(p, flightwrap.Wrap{}); err != nil {
					return err
				}
			}
			// Maintenance mode: the coordinator launched this process as an
			// ephemeral maintenance worker. It connects, waits for the
			// coordinator to push a maintenance assignment, runs the pass,
			// and exits.
			if f.maintenance {
				cfg, err := f.config()
				if err != nil {
					return err
				}
				return worker.RunMaintenance(cmd.Context(), cfg)
			}
			cfg, err := f.config()
			if err != nil {
				return err
			}
			cfg.Logger.Info("starting worker",
				"coordinator", cfg.Coordinator,
				"name", cfg.Name,
				"namespace", cfg.Namespace,
				"catalog", cfg.Sink.URI,
				"metrics", cfg.MetricsAddr,
			)
			return worker.RunRemote(cmd.Context(), cfg)
		},
	}
	fl := cmd.Flags()
	fl.StringVar(&f.coordinator, "coordinator", "127.0.0.1:50051", "coordinator address (host:port)")
	fl.StringVar(&f.name, "name", os.Getenv("HOSTNAME"), "worker name (Hello); defaults to $HOSTNAME")
	// The catalog settings fall back to the URUTAU_SINK_* environment the
	// operator mounts from the CDCPipeline's Secrets (see
	// internal/operator.coordinatorEnv): in-cluster the worker never sees a
	// flag, only env. A flag always wins over the environment.
	fl.StringVar(&f.catalogURI, "catalog-uri", envOr("URUTAU_SINK_URI", "http://localhost:8181/api/catalog"), "Iceberg REST catalog URI")
	fl.StringVar(&f.warehouse, "warehouse", envOr("URUTAU_SINK_WAREHOUSE", "quickstart_catalog"), "catalog warehouse name")
	fl.StringVar(&f.clientID, "client-id", os.Getenv("URUTAU_SINK_CLIENT_ID"), "catalog OAuth2 client id")
	fl.StringVar(&f.clientSecret, "client-secret", os.Getenv("URUTAU_SINK_CLIENT_SECRET"), "catalog OAuth2 client secret")
	fl.StringVar(&f.scope, "scope", envOr("URUTAU_SINK_SCOPE", "PRINCIPAL_ROLE:ALL"), "catalog OAuth2 scope")
	fl.StringVar(&f.namespace, "namespace", "raw", "fallback namespace for bare targets")
	fl.IntVar(&f.maxRows, "max-rows", 1000, "flush the batch once this many rows are buffered")
	fl.DurationVar(&f.maxInterval, "max-interval", 2*time.Second, "flush cadence")
	fl.StringVar(&f.metricsAddr, "metrics-addr", "", "serve /metrics on this address (optional)")
	fl.StringSliceVar(&f.pluginPaths, "plugin", nil, "path to a Go plugin (.so); can be repeated for multiple plugins")
	fl.StringVar(&f.tlsCert, "tls-cert", "", "client certificate for the control plane (mTLS; all three TLS flags required)")
	fl.StringVar(&f.tlsKey, "tls-key", "", "client private key for the control plane (mTLS)")
	fl.StringVar(&f.tlsCA, "tls-ca", "", "CA that signs the coordinator's server cert (mTLS)")
	fl.StringVar(&f.logLevel, "log-level", "info", "log level: debug|info|warn|error")
	fl.StringVar(&f.logFormat, "log-format", "text", "log format: text|json")
	fl.BoolVar(&f.maintenance, "maintenance", false, "run as an ephemeral maintenance worker: connect, run the coordinator's maintenance assignment once, and exit")
	return cmd
}

// envOr returns the environment value, or fallback when it is unset or empty.
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func (f *workerFlags) config() (worker.RemoteConfig, error) {
	tlsCfg := grpctls.Config{CertFile: f.tlsCert, KeyFile: f.tlsKey, ClientCAFile: f.tlsCA}
	if err := tlsCfg.Validate(); err != nil {
		return worker.RemoteConfig{}, err
	}
	if f.coordinator == "" {
		return worker.RemoteConfig{}, fmt.Errorf("--coordinator must not be empty")
	}
	if f.name == "" {
		return worker.RemoteConfig{}, fmt.Errorf("--name must not be empty (set $HOSTNAME or pass --name)")
	}
	if f.catalogURI == "" {
		return worker.RemoteConfig{}, fmt.Errorf("--catalog-uri must not be empty")
	}
	logger, err := logging.New(f.logLevel, f.logFormat)
	if err != nil {
		return worker.RemoteConfig{}, err
	}
	slog.SetDefault(logger)
	return worker.RemoteConfig{
		Coordinator: f.coordinator,
		Name:        f.name,
		Namespace:   f.namespace,
		Sink: sink.Config{
			URI: f.catalogURI,
			Options: map[string]string{
				"warehouse":     f.warehouse,
				"client_id":     f.clientID,
				"client_secret": f.clientSecret,
				"scope":         f.scope,
			},
		},
		MaxRows:     f.maxRows,
		MaxInterval: f.maxInterval,
		Logger:      logger,
		MetricsAddr: f.metricsAddr,
		TLS:         tlsCfg,
	}, nil
}

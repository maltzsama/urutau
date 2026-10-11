package main

import (
	"context"
	"errors"
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
	"github.com/maltzsama/urutau/internal/memlimit"
	"github.com/maltzsama/urutau/internal/plugin/flightwrap"
	remote "github.com/maltzsama/urutau/internal/worker/remote"
	"github.com/maltzsama/urutau/sink"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "urutau-worker:", err)
		writeTerminationMessage(err)
		os.Exit(1)
	}
}

// terminationLog is where Kubernetes reads a container's termination message.
const terminationLog = "/dev/termination-log"

// writeTerminationMessage records why the worker exited in its Pod's
// termination message, which the coordinator reads when the worker comes
// back to tell a crash from a lost network (issue #461). Best effort: outside
// Kubernetes the file does not exist.
func writeTerminationMessage(err error) {
	_ = os.WriteFile(terminationLog, []byte(terminationMessage(err)), 0o644)
}

// terminationMessage is err, marked "network: " when the worker lost its
// coordinator (see remote.ErrCoordinatorLost).
func terminationMessage(err error) string {
	if errors.Is(err, remote.ErrCoordinatorLost) {
		return "network: " + err.Error()
	}
	return err.Error()
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
// remote.RemoteConfig, keeping the RunE a thin wrapper.
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
	maxBytesMi   int64
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
				return remote.RunMaintenance(cmd.Context(), cfg)
			}
			cfg, err := f.config()
			if err != nil {
				return err
			}
			cfg.Logger.Info("starting worker",
				"coordinator", cfg.Coordinator,
				"name", cfg.Name,
				"namespace", cfg.Namespace,
				"catalog", logging.RedactURI(cfg.Sink.URI),
				"metrics", cfg.MetricsAddr,
			)
			return remote.RunRemote(cmd.Context(), cfg)
		},
	}
	fl := cmd.Flags()
	fl.StringVar(&f.coordinator, "coordinator", "127.0.0.1:50051", "coordinator address (host:port)")
	fl.StringVar(&f.name, "name", os.Getenv("HOSTNAME"), "worker name (Hello); defaults to $HOSTNAME")
	// The catalog settings come from the URUTAU_SINK_* environment the
	// operator mounts from the CDCPipeline's Secrets (see
	// internal/operator.coordinatorEnv): in-cluster the worker never sees a
	// flag, only env. A flag always wins over the environment. There is NO
	// insecure default (a localhost catalog with full scope): a worker without
	// configuration fails instead (issue #604).
	fl.StringVar(&f.catalogURI, "catalog-uri", os.Getenv("URUTAU_SINK_URI"), "Iceberg REST catalog URI")
	fl.StringVar(&f.warehouse, "warehouse", os.Getenv("URUTAU_SINK_WAREHOUSE"), "catalog warehouse name")
	fl.StringVar(&f.clientID, "client-id", os.Getenv("URUTAU_SINK_CLIENT_ID"), "catalog OAuth2 client id")
	fl.StringVar(&f.clientSecret, "client-secret", "", "catalog OAuth2 client secret (default: $URUTAU_SINK_CLIENT_SECRET)")
	fl.StringVar(&f.scope, "scope", os.Getenv("URUTAU_SINK_SCOPE"), "catalog OAuth2 scope")
	fl.StringVar(&f.namespace, "namespace", "raw", "fallback namespace for bare targets")
	fl.IntVar(&f.maxRows, "max-rows", 10000, "flush the batch once this many rows are buffered (one Iceberg commit)")
	fl.Int64Var(&f.maxBytesMi, "max-bytes-mi", 32, "flush the batch once its buffered rows hold this many MiB")
	fl.DurationVar(&f.maxInterval, "max-interval", 2*time.Second, "flush cadence")
	fl.StringVar(&f.metricsAddr, "metrics-addr", "", "serve /metrics on this address (optional)")
	fl.StringSliceVar(&f.pluginPaths, "plugin", nil, "path to a Go plugin (.so, CGO build only); can be repeated for multiple plugins")
	fl.StringVar(&f.tlsCert, "tls-cert", "", "client certificate for the control plane (mTLS; all three TLS flags required)")
	fl.StringVar(&f.tlsKey, "tls-key", "", "client private key for the control plane (mTLS)")
	fl.StringVar(&f.tlsCA, "tls-ca", "", "CA that signs the coordinator's server cert (mTLS)")
	fl.StringVar(&f.logLevel, "log-level", "info", "log level: debug|info|warn|error")
	fl.StringVar(&f.logFormat, "log-format", "text", "log format: text|json")
	fl.BoolVar(&f.maintenance, "maintenance", false, "run as an ephemeral maintenance worker: connect, run the coordinator's maintenance assignment once, and exit")
	return cmd
}

func (f *workerFlags) config() (remote.RemoteConfig, error) {
	tlsCfg := grpctls.Config{CertFile: f.tlsCert, KeyFile: f.tlsKey, ClientCAFile: f.tlsCA}
	if err := tlsCfg.Validate(); err != nil {
		return remote.RemoteConfig{}, err
	}
	if f.coordinator == "" {
		return remote.RemoteConfig{}, fmt.Errorf("--coordinator must not be empty")
	}
	if f.name == "" {
		return remote.RemoteConfig{}, fmt.Errorf("--name must not be empty (set $HOSTNAME or pass --name)")
	}
	if f.catalogURI == "" {
		return remote.RemoteConfig{}, fmt.Errorf("--catalog-uri must not be empty (set $URUTAU_SINK_URI or pass --catalog-uri)")
	}
	logger, logBuffer, err := logging.NewBuffered(f.logLevel, f.logFormat, 2000)
	if err != nil {
		return remote.RemoteConfig{}, err
	}
	slog.SetDefault(logger)
	// The Pod's memory limit, given to the garbage collector (#437).
	// Applied AFTER the logger so the first line honours --log-format.
	memlimit.Apply(logger)
	// The secret defaults to the environment, but only HERE — as a flag
	// default it would be printed by --help and by a mistyped flag's usage,
	// leaking the credential into logs (#597).
	clientSecret := f.clientSecret
	if clientSecret == "" {
		clientSecret = os.Getenv("URUTAU_SINK_CLIENT_SECRET")
	}
	return remote.RemoteConfig{
		Coordinator: f.coordinator,
		Name:        f.name,
		Namespace:   f.namespace,
		Sink: sink.Config{
			URI: f.catalogURI,
			Options: map[string]string{
				"warehouse":     f.warehouse,
				"client_id":     f.clientID,
				"client_secret": clientSecret,
				"scope":         f.scope,
			},
		},
		MaxRows:     f.maxRows,
		MaxBytes:    f.maxBytesMi << 20,
		MaxInterval: f.maxInterval,
		Logger:      logger,
		LogBuffer:   logBuffer,
		MetricsAddr: f.metricsAddr,
		TLS:         tlsCfg,
	}, nil
}

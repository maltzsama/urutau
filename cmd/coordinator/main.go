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
	"github.com/maltzsama/urutau/internal/coordinator"
	"github.com/maltzsama/urutau/internal/eventlog"
	"github.com/maltzsama/urutau/internal/grpctls"
	"github.com/maltzsama/urutau/internal/logging"
	"github.com/maltzsama/urutau/internal/plugin/flightwrap"
	"github.com/maltzsama/urutau/spec"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "urutau-coordinator:", err)
		os.Exit(1)
	}
}

func run() error {
	root := &cobra.Command{
		Use:           "urutau-coordinator",
		Short:         "Urutau coordinator — source reader, DBLog snapshot, worker sessions",
		SilenceUsage:  true,
		SilenceErrors: true, // main prints the single prefixed error line
	}
	root.AddCommand(runCmd())

	// SIGINT/SIGTERM cancel the command context: worker sessions close with
	// a graceful drain signal instead of the process dying at SIGKILL.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return root.ExecuteContext(ctx)
}

// coordinatorFlags are the raw CLI values. config() turns them into a
// validated coordinator.Config, keeping the RunE a thin wrapper.
type coordinatorFlags struct {
	file            string
	listen          string
	metricsAddr     string
	tlsCert         string
	tlsKey          string
	tlsCA           string
	serverID        uint32
	chunkSize       int
	maxParallel     int
	windowTimeout   time.Duration
	flowTotalBytes  int64
	flowPerWorkerMi int64
	waitWorker      time.Duration
	ackTimeout      time.Duration
	maxResets       int
	resetWindow     time.Duration
	eventlogURI     string
	checkpointURI   string
	checkpointSec   int
	pluginPaths     []string
	logLevel        string
	logFormat       string
}

func runCmd() *cobra.Command {
	f := &coordinatorFlags{}
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Serve the source pipeline to workers",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			// Load dynamic plugins before anything else.
			for _, p := range f.pluginPaths {
				if err := driver.LoadPlugin(p, flightwrap.Wrap{}); err != nil {
					return err
				}
			}
			logger, err := logging.New(f.logLevel, f.logFormat)
			if err != nil {
				return err
			}
			slog.SetDefault(logger)
			// Validate flags before touching the filesystem: a bad flag
			// fails fast, not after a spec read.
			if err := f.validate(); err != nil {
				return err
			}
			s, err := loadSpec(f.file)
			if err != nil {
				return err
			}
			cfg := f.config(s, logger)
			cfg.Logger.Info("starting coordinator",
				"listen", cfg.ListenAddr,
				"serverID", cfg.ServerID,
				"metrics", cfg.MetricsAddr,
				"eventlog", f.eventlogURI != "",
				"checkpoint", f.checkpointURI != "",
			)
			return coordinator.Run(cmd.Context(), cfg)
		},
	}
	fl := cmd.Flags()
	// input
	fl.StringVarP(&f.file, "file", "f", "pipeline.yaml", "pipeline spec (inline YAML)")
	// listener / control plane
	fl.StringVar(&f.listen, "listen", ":50051", "gRPC + Flight listen address")
	fl.StringVar(&f.metricsAddr, "metrics-addr", "", "serve /metrics and /statusz on this address (optional)")
	fl.StringVar(&f.tlsCert, "tls-cert", "", "server certificate for the control plane (mTLS; all three TLS flags required)")
	fl.StringVar(&f.tlsKey, "tls-key", "", "server private key for the control plane (mTLS)")
	fl.StringVar(&f.tlsCA, "tls-ca", "", "CA that signs worker client certs (mTLS)")
	// source
	fl.Uint32Var(&f.serverID, "server-id", 1101, "MySQL server id for this replicator")
	fl.IntVar(&f.chunkSize, "chunk-size", 10000, "DBLog snapshot chunk size (rows per chunk)")
	fl.IntVar(&f.maxParallel, "max-parallel-chunks", 0, "max concurrent chunk SELECTs during snapshot (0 = serial; must not exceed the source driver ceiling)")
	fl.DurationVar(&f.windowTimeout, "window-timeout", 5*time.Minute, "DBLog window timeout (pathology detector)")
	// flow control
	fl.Int64Var(&f.flowTotalBytes, "flow-total-bytes", 512<<20, "process-wide ceiling on unacked batch bytes in flight")
	fl.Int64Var(&f.flowPerWorkerMi, "flow-per-worker-min-mi", 16, "per-worker minimum share of the flow budget (MiB)")
	fl.DurationVar(&f.waitWorker, "wait-worker", 2*time.Minute, "how long to wait for every expected worker session")
	fl.DurationVar(&f.ackTimeout, "ack-timeout", 30*time.Second, "worker considered stale without an ack for this long")
	fl.IntVar(&f.maxResets, "max-resets", 5, "resets within the window before the job terminates")
	fl.DurationVar(&f.resetWindow, "reset-window", 15*time.Minute, "sliding window for the reset count")
	// audit / recovery
	fl.StringVar(&f.eventlogURI, "eventlog", "", "s3://<bucket>/<prefix> audit trail store (optional)")
	fl.StringVar(&f.checkpointURI, "checkpoint", "", "s3://<bucket>/<prefix> async position manifests (optional)")
	fl.IntVar(&f.checkpointSec, "checkpoint-interval", 10, "checkpoint write interval (seconds)")
	// process
	fl.StringSliceVar(&f.pluginPaths, "plugin", nil, "path to a Go plugin (.so); can be repeated for multiple plugins")
	fl.StringVar(&f.logLevel, "log-level", "info", "log level: debug|info|warn|error")
	fl.StringVar(&f.logFormat, "log-format", "text", "log format: text|json")
	return cmd
}

// validate rejects incoherent flag combinations before any I/O.
func (f *coordinatorFlags) validate() error {
	tlsCfg := grpctls.Config{CertFile: f.tlsCert, KeyFile: f.tlsKey, ClientCAFile: f.tlsCA}
	if err := tlsCfg.Validate(); err != nil {
		return err
	}
	if f.maxParallel < 0 {
		return fmt.Errorf("max-parallel-chunks must be >= 0 (got %d)", f.maxParallel)
	}
	if f.flowPerWorkerMi < 0 {
		return fmt.Errorf("flow-per-worker-min-mi must be >= 0 (got %d)", f.flowPerWorkerMi)
	}
	perWorker := f.flowPerWorkerMi << 20
	// A per-worker floor above the process ceiling makes the ceiling
	// meaningless: one worker alone could exceed it.
	if f.flowTotalBytes > 0 && perWorker > f.flowTotalBytes {
		return fmt.Errorf(
			"flow-per-worker-min-mi (%d bytes) exceeds flow-total-bytes (%d bytes)", perWorker, f.flowTotalBytes)
	}
	if f.checkpointURI != "" && f.checkpointSec <= 0 {
		return fmt.Errorf("checkpoint-interval must be > 0 when --checkpoint is set")
	}
	return nil
}

func (f *coordinatorFlags) config(s *spec.Spec, logger *slog.Logger) coordinator.Config {
	return coordinator.Config{
		Spec:              s,
		ListenAddr:        f.listen,
		ServerID:          f.serverID,
		Heartbeat:         5 * time.Second, // control-plane liveness cadence (protocol constant)
		ChunkSize:         f.chunkSize,
		MaxParallelChunks: f.maxParallel,
		WindowTimeout:     f.windowTimeout,
		CaughtUpPoll:      time.Second, // caught-up proof poll (protocol constant)
		WaitWorker:        f.waitWorker,
		FlowTotalBytes:    f.flowTotalBytes,
		FlowPerWorkerMin:  f.flowPerWorkerMi << 20,
		Eventlog:          eventlogConfig(f.eventlogURI),
		Checkpoint:        checkpointConfig(f.checkpointURI, f.checkpointSec),
		AckTimeout:        f.ackTimeout,
		MaxResets:         f.maxResets,
		ResetWindow:       f.resetWindow,
		MetricsAddr:       f.metricsAddr,
		TLS:               grpctls.Config{CertFile: f.tlsCert, KeyFile: f.tlsKey, ClientCAFile: f.tlsCA},
		Logger:            logger,
	}
}

// loadSpec opens, parses and validates the inline YAML pipeline spec.
func loadSpec(path string) (*spec.Spec, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	s, err := spec.LoadYAML(f)
	if err != nil {
		return nil, err
	}
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return s, nil
}

// checkpointConfig builds the S3 position-manifest config from the flags;
// nil when unset.
func checkpointConfig(uri string, intervalSec int) *coordinator.CheckpointConfig {
	if uri == "" {
		return nil
	}
	return &coordinator.CheckpointConfig{
		URI:      uri,
		Interval: time.Duration(intervalSec) * time.Second,
	}
}

func eventlogConfig(uri string) *eventlog.Config {
	if uri == "" {
		return nil
	}
	return &eventlog.Config{URI: uri}
}

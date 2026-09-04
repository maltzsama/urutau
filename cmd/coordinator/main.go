package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/maltzsama/urutau/internal/coordinator"
	"github.com/maltzsama/urutau/internal/eventlog"
	"github.com/maltzsama/urutau/internal/spec"
)

func main() {
	root := &cobra.Command{
		Use:          "urutau-coordinator",
		Short:        "Urutau coordinator — source reader, DBLog snapshot, worker sessions",
		SilenceUsage: true,
	}

	var (
		file            string
		listen          string
		serverID        uint32
		chunkSize       int
		maxParallel     int
		windowTimeout   time.Duration
		waitWorker      time.Duration
		flowTotalBytes  int64
		flowPerWorkerMi int64
		eventlogURI     string
		checkpointURI   string
		checkpointSec   int
		ackTimeout      time.Duration
		maxResets       int
		resetWindow     time.Duration
		metricsAddr     string
	)
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Serve the source pipeline to workers",
		RunE: func(cmd *cobra.Command, args []string) error {
			f, err := os.Open(file)
			if err != nil {
				return err
			}
			defer func() { _ = f.Close() }()
			s, err := spec.LoadYAML(f)
			if err != nil {
				return err
			}
			if err := s.Validate(); err != nil {
				return err
			}
			return coordinator.Run(cmd.Context(), coordinator.Config{
				Spec:              s,
				ListenAddr:        listen,
				ServerID:          serverID,
				Heartbeat:         5 * time.Second,
				ChunkSize:         chunkSize,
				MaxParallelChunks: maxParallel,
				WindowTimeout:     windowTimeout,
				CaughtUpPoll:      time.Second,
				WaitWorker:        waitWorker,
				FlowTotalBytes:    flowTotalBytes,
				FlowPerWorkerMin:  flowPerWorkerMi << 20,
				Eventlog:          eventlogConfig(eventlogURI),
				Checkpoint:        checkpointConfig(checkpointURI, checkpointSec),
				AckTimeout:        ackTimeout,
				MaxResets:         maxResets,
				ResetWindow:       resetWindow,
				MetricsAddr:       metricsAddr,
			})
		},
	}
	cmd.Flags().StringVarP(&file, "file", "f", "pipeline.yaml", "pipeline spec (inline YAML)")
	cmd.Flags().StringVar(&listen, "listen", ":50051", "gRPC + Flight listen address")
	cmd.Flags().Uint32Var(&serverID, "server-id", 1101, "MySQL server id for this replicator")
	cmd.Flags().IntVar(&chunkSize, "chunk-size", 10000, "DBLog snapshot chunk size (rows per chunk)")
	cmd.Flags().IntVar(&maxParallel, "max-parallel-chunks", 0, "Max concurrent chunk SELECTs during snapshot (0 = serial; must not exceed the source driver ceiling)")
	cmd.Flags().DurationVar(&windowTimeout, "window-timeout", 5*time.Minute, "DBLog window timeout (pathology detector)")
	cmd.Flags().DurationVar(&waitWorker, "wait-worker", 2*time.Minute, "how long to wait for every expected worker session")
	cmd.Flags().Int64Var(&flowTotalBytes, "flow-total-bytes", 512<<20, "process-wide ceiling on unacked batch bytes in flight")
	cmd.Flags().Int64Var(&flowPerWorkerMi, "flow-per-worker-min-mi", 16, "per-worker minimum share of the flow budget (MiB)")
	cmd.Flags().StringVar(&eventlogURI, "eventlog", "", "s3://<bucket>/<prefix> audit trail store (optional)")
	cmd.Flags().StringVar(&checkpointURI, "checkpoint", "", "s3://<bucket>/<prefix> async position manifests (optional)")
	cmd.Flags().IntVar(&checkpointSec, "checkpoint-interval", 10, "checkpoint write interval (seconds)")
	cmd.Flags().DurationVar(&ackTimeout, "ack-timeout", 30*time.Second, "worker considered stale without an ack for this long")
	cmd.Flags().IntVar(&maxResets, "max-resets", 5, "resets within the window before the job terminates")
	cmd.Flags().DurationVar(&resetWindow, "reset-window", 15*time.Minute, "sliding window for the reset count")
	cmd.Flags().StringVar(&metricsAddr, "metrics-addr", "", "serve /metrics and /statusz on this address (optional)")

	root.AddCommand(cmd)
	// SIGINT/SIGTERM cancel the command context: worker sessions close with
	// a graceful drain signal instead of the process dying at SIGKILL.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := root.ExecuteContext(ctx); err != nil {
		os.Exit(1)
	}
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

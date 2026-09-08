package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/maltzsama/urutau/driver"
	_ "github.com/maltzsama/urutau/internal/builtin"
	"github.com/maltzsama/urutau/internal/eventlog"
	"github.com/maltzsama/urutau/internal/pipeline"
	"github.com/maltzsama/urutau/internal/plugin"
	"github.com/maltzsama/urutau/internal/runner"
	"github.com/maltzsama/urutau/internal/supervisor"
	"github.com/maltzsama/urutau/internal/version"
	"github.com/maltzsama/urutau/spec"
)

func main() {
	root := &cobra.Command{
		Use:   "urutau",
		Short: "Urutau — CDC engine from MySQL/Postgres into Iceberg, reflecting state",
		Long: "Urutau is the CDC engine: a single replication connection per source\n" +
			"feeds N workers writing to Iceberg in parallel — upsert by PK,\n" +
			"first-class UPDATE/DELETE, no Kafka in the data path.",
		SilenceUsage: true,
	}

	root.AddCommand(versionCmd())
	root.AddCommand(runCmd())

	// SIGINT/SIGTERM cancel the command context: the pipeline drains in
	// flight commits and flushes buffered rows instead of dying at SIGKILL
	// (kubernetes grace period).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := root.ExecuteContext(ctx); err != nil {
		os.Exit(1)
	}
}

// runCmd wires the collapsed process from an inline YAML spec.
func runCmd() *cobra.Command {
	var (
		file              string
		serverID          uint32
		chunkSize         int
		maxParallelChunks int
		windowTimeout     time.Duration
		eventlogURI       string
		pluginPaths       []string
		sourcePlugin      string
		sinkPlugin        string
	)
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run the pipeline from a YAML spec",
		RunE: func(cmd *cobra.Command, args []string) error {
			// Load dynamic plugins before anything else.
			for _, p := range pluginPaths {
				if err := driver.LoadPlugin(p); err != nil {
					return err
				}
			}
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

			// External plugin mode: spawn subprocesses and run via
			// the supervisor + plugin adapters.
			if sourcePlugin != "" || sinkPlugin != "" {
				return runExternalPlugins(cmd.Context(), s, sourcePlugin, sinkPlugin)
			}

			// Built-in driver mode: run the collapsed pipeline.
			rc := runner.Config{
				ServerID:          serverID,
				Heartbeat:         5 * time.Second,
				ChunkSize:         chunkSize,
				MaxParallelChunks: maxParallelChunks,
				WindowTimeout:     windowTimeout,
				CaughtUpPoll:      time.Second,
				MaxRows:           1000,
				MaxInterval:       5 * time.Second,
			}
			if eventlogURI != "" {
				rc.Eventlog = &eventlog.Config{URI: eventlogURI}
			}
			r, err := runner.NewRunner(cmd.Context(), s, rc)
			if err != nil {
				return err
			}
			return r.Run(cmd.Context())
		},
	}
	cmd.Flags().StringVarP(&file, "file", "f", "pipeline.yaml", "pipeline spec (inline YAML)")
	cmd.Flags().Uint32Var(&serverID, "server-id", 1101, "MySQL server id for this replicator")
	cmd.Flags().IntVar(&chunkSize, "chunk-size", 10000, "DBLog snapshot chunk size (rows per chunk)")
	cmd.Flags().IntVar(&maxParallelChunks, "max-parallel-chunks", 0, "Max concurrent chunk SELECTs during snapshot (0 = serial; must not exceed the source driver ceiling)")
	cmd.Flags().DurationVar(&windowTimeout, "window-timeout", 5*time.Minute, "DBLog window timeout (pathology detector)")
	cmd.Flags().StringVar(&eventlogURI, "eventlog", "", "S3 URI for the run's JSONL audit trail (s3://bucket/prefix); AWS env supplies credentials/endpoint")
	cmd.Flags().StringSliceVar(&pluginPaths, "plugin", nil, "path to a Go plugin (.so); can be repeated for multiple plugins")
	cmd.Flags().StringVar(&sourcePlugin, "source-plugin", "", "path to an external source plugin binary (Arrow Flight)")
	cmd.Flags().StringVar(&sinkPlugin, "sink-plugin", "", "path to an external sink plugin binary (Arrow Flight)")
	return cmd
}

// runExternalPlugins spawns source and sink plugin subprocesses, connects
// Flight clients, and runs the pipeline using the plugin adapters.
func runExternalPlugins(ctx context.Context, s *spec.Spec, sourceBin, sinkBin string) error {
	logger := slog.Default()
	token := generateToken()

	var sourceCfg, sinkCfg pipeline.StageConfig
	workDir := "."

	if sourceBin != "" {
		sourceCfg = pipeline.StageConfig{
			Kind:    pipeline.StageSource,
			Bin:     sourceBin,
			Token:   token,
			WorkDir: workDir,
			Logger:  logger,
		}
	}
	if sinkBin != "" {
		sinkCfg = pipeline.StageConfig{
			Kind:    pipeline.StageSink,
			Bin:     sinkBin,
			Token:   token,
			WorkDir: workDir,
			Logger:  logger,
		}
	}

	pipeCfg := supervisor.PipelineConfig{
		Source: sourceCfg,
		Sink:   sinkCfg,
	}
	sup := supervisor.NewPipelineSupervisor(pipeCfg, logger)

	// Start all plugin subprocesses.
	go func() {
		if err := sup.Run(ctx); err != nil {
			logger.Error("supervisor stopped", "err", err)
		}
	}()

	// Wait for both plugins to be connected.
	if err := sup.WaitForReady(ctx); err != nil {
		return err
	}

	// Build adapters from connected clients.
	srcStage := sup.Source().Stage()
	snkStage := sup.Sink().Stage()

	srcAdapter := plugin.NewSourceAdapter(srcStage.Client, s.Source, logger)
	snkAdapter := plugin.NewSinkAdapter(snkStage.Client, logger)
	_ = snkAdapter // used by runner in full integration

	logger.Info("external plugins connected",
		"source", sourceBin,
		"sink", sinkBin,
		"pipeline", s.Pipeline,
	)

	// Build the collapsed runner with plugin adapters as the source.
	// The full integration wires srcAdapter as the source and snkAdapter
	// as the sink. For now, we use the existing runner path.
	// TODO: wire plugin adapters into the runner when the runner
	// accepts source.Source and sink.Sink interfaces directly.
	_ = srcAdapter

	// Block until shutdown.
	<-ctx.Done()
	logger.Info("shutting down")
	return nil
}

func generateToken() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic("crypto/rand: " + err.Error())
	}
	return base64.URLEncoding.EncodeToString(b)
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the binary version",
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.Println(version.String())
			return nil
		},
	}
}

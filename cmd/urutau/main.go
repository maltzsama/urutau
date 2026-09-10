package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
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
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/source"
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

			// The runner config is shared by both modes, so a built-in side
			// in the plugin path behaves identically to the collapsed run.
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

			// External plugin mode: spawn subprocesses and run via
			// the supervisor + plugin adapters.
			if sourcePlugin != "" || sinkPlugin != "" {
				return runExternalPlugins(cmd.Context(), s, rc, sourcePlugin, sinkPlugin)
			}

			// Built-in driver mode: run the collapsed pipeline.
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

// runExternalPlugins runs the pipeline over external plugin subprocesses.
// Each side (source, sink) that names a plugin binary is spawned via the
// supervisor and adapted to the public source.Source / sink.Sink contracts;
// a side left empty falls back to the built-in driver registry. The runner
// consumes both through the columnar seam (NewRunnerWithAdapters). rc carries
// the shared knobs (server id, chunk size, window timeout, eventlog) so a
// built-in side behaves identically to the collapsed runner.
func runExternalPlugins(ctx context.Context, s *spec.Spec, rc runner.Config, sourceBin, sinkBin string) error {
	logger := slog.Default()
	token, err := generateToken()
	if err != nil {
		return err
	}

	var (
		src source.Source
		snk sink.Sink
	)
	var closers []func()
	defer func() {
		for _, f := range closers {
			f()
		}
	}()

	// Source side: plugin adapter or registry driver.
	if sourceBin != "" {
		// A plugin source owns its chunking; the registry ceiling (which
		// bounds built-in chunk SELECTs) does not apply.
		rc.MaxParallelChunks = 0
		stage, err := spawnPluginStage(ctx, sourceBin, token, pipeline.StageSource, logger)
		if err != nil {
			return err
		}
		closers = append(closers, func() { _ = stage.Stop(context.Background()) })
		src = plugin.NewSourceAdapter(stage.Client, s.Source, logger)
	} else {
		if err := driver.ValidateParallelism(s.Source.Kind, rc.MaxParallelChunks); err != nil {
			return fmt.Errorf("runner: %w", err)
		}
		regSrc, err := driver.OpenSource(s, source.Runtime{
			ServerID:  rc.ServerID,
			Heartbeat: rc.Heartbeat,
			Logger:    logger,
		})
		if err != nil {
			return err
		}
		// The source owns its query connection; the runner releases it.
		src = regSrc
	}

	// Sink side: plugin adapter or registry driver.
	if sinkBin != "" {
		stage, err := spawnPluginStage(ctx, sinkBin, token, pipeline.StageSink, logger)
		if err != nil {
			return err
		}
		closers = append(closers, func() { _ = stage.Stop(context.Background()) })
		snk = plugin.NewSinkAdapter(stage.Client, logger)
	} else {
		regSnk, err := driver.OpenSink(ctx, s)
		if err != nil {
			return err
		}
		closers = append(closers, func() { _ = regSnk.Close() })
		snk = regSnk
	}

	logger.Info("external plugins connected",
		"source", sourceBin,
		"sink", sinkBin,
		"pipeline", s.Pipeline,
	)

	r, err := runner.NewRunnerWithAdapters(ctx, s, rc, src, snk)
	if err != nil {
		return err
	}
	return r.Run(ctx)
}

// spawnPluginStage runs one plugin stage via its supervisor and waits for
// the Flight client to be connected. The supervisor keeps the process alive
// (restart with backoff) until ctx ends.
func spawnPluginStage(ctx context.Context, bin, token string, kind pipeline.StageKind, logger *slog.Logger) (*pipeline.Stage, error) {
	sup := supervisor.NewStageSupervisor(pipeline.StageConfig{
		Kind:    kind,
		Bin:     bin,
		Token:   token,
		WorkDir: ".",
		Logger:  logger,
	}, logger)
	go func() {
		if err := sup.Run(ctx); err != nil {
			logger.Error("plugin stage stopped", "err", err)
		}
	}()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.Now().Add(30 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-sup.Dead():
			return nil, fmt.Errorf("plugin stage %s: died: %v", bin, sup.DeadErr())
		default:
		}
		st := sup.Stage()
		if st != nil && st.Client != nil {
			return st, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("plugin stage %s: not ready within 30s", bin)
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func generateToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate plugin token: %w", err)
	}
	return base64.URLEncoding.EncodeToString(b), nil
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

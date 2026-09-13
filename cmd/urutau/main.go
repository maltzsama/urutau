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
	_ "github.com/maltzsama/urutau/internal/builtin" // register built-in drivers via init()
	"github.com/maltzsama/urutau/internal/eventlog"
	"github.com/maltzsama/urutau/internal/logging"
	"github.com/maltzsama/urutau/internal/pipeline"
	"github.com/maltzsama/urutau/internal/plugin"
	"github.com/maltzsama/urutau/internal/plugin/flightwrap"
	"github.com/maltzsama/urutau/internal/runner"
	"github.com/maltzsama/urutau/internal/supervisor"
	"github.com/maltzsama/urutau/internal/version"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "urutau:", err)
		os.Exit(1)
	}
}

func run() error {
	root := &cobra.Command{
		Use:   "urutau",
		Short: "Urutau — CDC engine from MySQL/Postgres into Iceberg, reflecting state",
		Long: `Urutau is the CDC engine: a single replication connection per source
feeds N workers writing to Iceberg in parallel — upsert by PK,
first-class UPDATE/DELETE, no Kafka in the data path.`,
		SilenceUsage:  true,
		SilenceErrors: true, // main prints the single prefixed error line
	}
	root.AddCommand(versionCmd())
	root.AddCommand(runCmd())

	// SIGINT/SIGTERM cancel the command context: the pipeline drains in
	// flight commits and flushes buffered rows instead of dying at SIGKILL
	// (kubernetes grace period).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return root.ExecuteContext(ctx)
}

// pipelineFlags are the raw CLI values for `run`. config() turns them into a
// runner.Config; run() dispatches to the built-in or external-plugin path.
type pipelineFlags struct {
	file              string
	serverID          uint32
	chunkSize         int
	maxParallelChunks int
	windowTimeout     time.Duration
	eventlogURI       string
	pluginPaths       []string
	sourcePlugin      string
	sinkPlugin        string
	logLevel          string
	logFormat         string
}

// runCmd wires the pipeline from an inline YAML spec, either through the
// built-in drivers or external plugin subprocesses.
func runCmd() *cobra.Command {
	f := &pipelineFlags{}
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run the pipeline from a YAML spec",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return f.run(cmd.Context())
		},
	}
	fl := cmd.Flags()
	fl.StringVarP(&f.file, "file", "f", "pipeline.yaml", "pipeline spec (inline YAML)")
	fl.Uint32Var(&f.serverID, "server-id", 1101, "MySQL server id for this replicator")
	fl.IntVar(&f.chunkSize, "chunk-size", 10000, "DBLog snapshot chunk size (rows per chunk)")
	fl.IntVar(&f.maxParallelChunks, "max-parallel-chunks", 0, "max concurrent chunk SELECTs during snapshot (0 = serial; must not exceed the source driver ceiling)")
	fl.DurationVar(&f.windowTimeout, "window-timeout", 5*time.Minute, "DBLog window timeout (pathology detector)")
	fl.StringVar(&f.eventlogURI, "eventlog", "", "S3 URI for the run's JSONL audit trail (s3://bucket/prefix); AWS env supplies credentials/endpoint")
	fl.StringSliceVar(&f.pluginPaths, "plugin", nil, "path to a Go plugin (.so); can be repeated for multiple plugins")
	fl.StringVar(&f.sourcePlugin, "source-plugin", "", "path to an external source plugin binary (Arrow Flight)")
	fl.StringVar(&f.sinkPlugin, "sink-plugin", "", "path to an external sink plugin binary (Arrow Flight)")
	fl.StringVar(&f.logLevel, "log-level", "info", "log level: debug|info|warn|error")
	fl.StringVar(&f.logFormat, "log-format", "text", "log format: text|json")
	return cmd
}

func (f *pipelineFlags) run(ctx context.Context) error {
	// Load dynamic plugins before anything else.
	for _, p := range f.pluginPaths {
		if err := driver.LoadPlugin(p, flightwrap.Wrap{}); err != nil {
			return err
		}
	}
	s, err := loadSpec(f.file)
	if err != nil {
		return err
	}
	cfg, err := f.config()
	if err != nil {
		return err
	}

	// External plugin mode: spawn subprocesses and run via the supervisor
	// + plugin adapters. A side left empty falls back to the registry.
	if f.sourcePlugin != "" || f.sinkPlugin != "" {
		return runExternalPlugins(ctx, s, cfg, f.sourcePlugin, f.sinkPlugin)
	}

	// Built-in driver mode: run the collapsed pipeline.
	cfg.Logger.Info("starting pipeline",
		"mode", "builtin",
		"spec", f.file,
		"serverID", cfg.ServerID,
		"tables", len(s.Tables),
	)
	r, err := runner.NewRunner(ctx, s, cfg)
	if err != nil {
		return err
	}
	return r.Run(ctx)
}

func (f *pipelineFlags) config() (runner.Config, error) {
	logger, err := logging.New(f.logLevel, f.logFormat)
	if err != nil {
		return runner.Config{}, err
	}
	slog.SetDefault(logger)
	cfg := runner.Config{
		ServerID:          f.serverID,
		Heartbeat:         5 * time.Second, // control-plane liveness cadence (protocol constant)
		ChunkSize:         f.chunkSize,
		MaxParallelChunks: f.maxParallelChunks,
		WindowTimeout:     f.windowTimeout,
		CaughtUpPoll:      time.Second, // caught-up proof poll (protocol constant)
		MaxRows:           1000,        // local-run flush threshold
		MaxInterval:       5 * time.Second,
		Logger:            logger,
	}
	if f.eventlogURI != "" {
		cfg.Eventlog = &eventlog.Config{URI: f.eventlogURI}
	}
	return cfg, nil
}

// runExternalPlugins runs the pipeline over external plugin subprocesses.
// Each side (source, sink) that names a plugin binary is spawned via the
// supervisor and adapted to the public source.Source / sink.Sink contracts;
// a side left empty falls back to the built-in driver registry. The runner
// consumes both through the columnar seam (NewRunnerWithAdapters). cfg
// carries the shared knobs (server id, chunk size, window timeout, eventlog)
// so a built-in side behaves identically to the collapsed runner.
func runExternalPlugins(ctx context.Context, s *spec.Spec, cfg runner.Config, sourceBin, sinkBin string) error {
	logger := cfg.Logger
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
		cfg.MaxParallelChunks = 0
		stage, err := spawnPluginStage(ctx, sourceBin, token, pipeline.StageSource, logger)
		if err != nil {
			return err
		}
		closers = append(closers, func() { _ = stage.Stop(context.Background()) })
		src = plugin.NewSourceAdapter(stage.Client, s.Source, logger)
	} else {
		if err := driver.ValidateParallelism(s.Source.Kind, cfg.MaxParallelChunks); err != nil {
			return fmt.Errorf("runner: %w", err)
		}
		regSrc, err := driver.OpenSource(s, source.Runtime{
			ServerID:  cfg.ServerID,
			Heartbeat: cfg.Heartbeat,
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

	r, err := runner.NewRunnerWithAdapters(ctx, s, cfg, src, snk)
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
		WorkDir: ".", // the plugin inherits the parent process's working directory
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
		case <-ticker.C:
		}
		if st := sup.Stage(); st != nil && st.Client != nil {
			return st, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("plugin stage %s: not ready within 30s", bin)
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

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the binary version",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cmd.Println(version.String())
			return nil
		},
	}
}

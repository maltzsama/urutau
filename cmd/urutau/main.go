package main

import (
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/maltzsama/urutau/internal/runner"
	"github.com/maltzsama/urutau/internal/spec"
	"github.com/maltzsama/urutau/internal/version"
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

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

// runCmd wires the collapsed process from an inline YAML spec.
func runCmd() *cobra.Command {
	var (
		file          string
		serverID      uint32
		chunkSize     int
		windowTimeout time.Duration
	)
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run the pipeline from a YAML spec",
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
			return runner.Run(cmd.Context(), s, runner.Config{
				ServerID:      serverID,
				Heartbeat:     5 * time.Second,
				ChunkSize:     chunkSize,
				WindowTimeout: windowTimeout,
				CaughtUpPoll:  time.Second,
				MaxRows:       1000,
				MaxInterval:   5 * time.Second,
			})
		},
	}
	cmd.Flags().StringVarP(&file, "file", "f", "pipeline.yaml", "pipeline spec (inline YAML)")
	cmd.Flags().Bool("local", true, "run in collapsed (single process) mode")
	cmd.Flags().Uint32Var(&serverID, "server-id", 1101, "MySQL server id for this replicator")
	cmd.Flags().IntVar(&chunkSize, "chunk-size", 10000, "DBLog snapshot chunk size (rows per chunk)")
	cmd.Flags().DurationVar(&windowTimeout, "window-timeout", 5*time.Minute, "DBLog window timeout (pathology detector)")
	return cmd
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

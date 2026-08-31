package main

import (
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/maltzsama/urutau/internal/coordinator"
	"github.com/maltzsama/urutau/internal/spec"
)

func main() {
	root := &cobra.Command{
		Use:          "urutau-coordinator",
		Short:        "Urutau coordinator — source reader, DBLog snapshot, worker sessions",
		SilenceUsage: true,
	}

	var (
		file          string
		listen        string
		serverID      uint32
		chunkSize     int
		windowTimeout time.Duration
		waitWorker    time.Duration
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
				Spec:          s,
				ListenAddr:    listen,
				ServerID:      serverID,
				Heartbeat:     5 * time.Second,
				ChunkSize:     chunkSize,
				WindowTimeout: windowTimeout,
				CaughtUpPoll:  time.Second,
				WaitWorker:    waitWorker,
			})
		},
	}
	cmd.Flags().StringVarP(&file, "file", "f", "pipeline.yaml", "pipeline spec (inline YAML)")
	cmd.Flags().StringVar(&listen, "listen", ":50051", "gRPC + Flight listen address")
	cmd.Flags().Uint32Var(&serverID, "server-id", 1101, "MySQL server id for this replicator")
	cmd.Flags().IntVar(&chunkSize, "chunk-size", 10000, "DBLog snapshot chunk size (rows per chunk)")
	cmd.Flags().DurationVar(&windowTimeout, "window-timeout", 5*time.Minute, "DBLog window timeout (pathology detector)")
	cmd.Flags().DurationVar(&waitWorker, "wait-worker", 2*time.Minute, "how long to wait for the first worker session")

	root.AddCommand(cmd)
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

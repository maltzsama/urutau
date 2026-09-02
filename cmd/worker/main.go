package main

import (
	"os"
	"time"

	"github.com/spf13/cobra"

	icebergsink "github.com/maltzsama/urutau/internal/sink/iceberg"
	"github.com/maltzsama/urutau/internal/worker"
)

func main() {
	root := &cobra.Command{
		Use:          "urutau-worker",
		Short:        "Urutau worker — Iceberg writer fed by the coordinator's Flight stream",
		SilenceUsage: true,
	}

	var (
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
	)
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Connect to a coordinator and write its stream to Iceberg",
		RunE: func(cmd *cobra.Command, args []string) error {
			return worker.RunRemote(cmd.Context(), worker.RemoteConfig{
				Coordinator: coordinator,
				Name:        name,
				Namespace:   namespace,
				Sink: icebergsink.Config{
					URI:          catalogURI,
					Warehouse:    warehouse,
					ClientID:     clientID,
					ClientSecret: clientSecret,
					Scope:        scope,
				},
				MaxRows:     maxRows,
				MaxInterval: maxInterval,
			})
		},
	}
	cmd.Flags().StringVar(&coordinator, "coordinator", "127.0.0.1:50051", "coordinator address (host:port)")
	cmd.Flags().StringVar(&name, "name", "worker-1", "worker name (Hello)")
	cmd.Flags().StringVar(&catalogURI, "catalog-uri", "http://localhost:8181/api/catalog", "Iceberg REST catalog URI")
	cmd.Flags().StringVar(&warehouse, "warehouse", "quickstart_catalog", "catalog warehouse name")
	cmd.Flags().StringVar(&clientID, "client-id", "root", "catalog OAuth2 client id")
	cmd.Flags().StringVar(&clientSecret, "client-secret", "s3cr3t", "catalog OAuth2 client secret")
	cmd.Flags().StringVar(&scope, "scope", "PRINCIPAL_ROLE:ALL", "catalog OAuth2 scope")
	cmd.Flags().StringVar(&namespace, "namespace", "raw", "fallback namespace for bare targets")
	cmd.Flags().IntVar(&maxRows, "max-rows", 1000, "flush the batch once this many rows are buffered")
	cmd.Flags().DurationVar(&maxInterval, "max-interval", 2*time.Second, "flush cadence")

	root.AddCommand(cmd)
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/maltzsama/urutau/driver"
	_ "github.com/maltzsama/urutau/internal/builtin"
	"github.com/maltzsama/urutau/internal/grpctls"
	"github.com/maltzsama/urutau/internal/worker"
	"github.com/maltzsama/urutau/sink"
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
		metricsAddr  string
		pluginPaths  []string
		tlsCert      string
		tlsKey       string
		tlsCA        string
	)
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Connect to a coordinator and write its stream to Iceberg",
		RunE: func(cmd *cobra.Command, args []string) error {
			// Load dynamic plugins before anything else.
			for _, p := range pluginPaths {
				if err := driver.LoadPlugin(p); err != nil {
					return err
				}
			}
			if clientID == "" || clientSecret == "" {
				return fmt.Errorf("--client-id and --client-secret are required")
			}
			return worker.RunRemote(cmd.Context(), worker.RemoteConfig{
				Coordinator: coordinator,
				Name:        name,
				Namespace:   namespace,
				Sink: sink.Config{
					URI: catalogURI,
					Options: map[string]string{
						"warehouse":     warehouse,
						"client_id":     clientID,
						"client_secret": clientSecret,
						"scope":         scope,
					},
				},
				MaxRows:     maxRows,
				MaxInterval: maxInterval,
				MetricsAddr: metricsAddr,
				TLS:         grpctls.Config{CertFile: tlsCert, KeyFile: tlsKey, ClientCAFile: tlsCA},
			})
		},
	}
	cmd.Flags().StringVar(&coordinator, "coordinator", "127.0.0.1:50051", "coordinator address (host:port)")
	cmd.Flags().StringVar(&name, "name", "worker-1", "worker name (Hello)")
	cmd.Flags().StringVar(&catalogURI, "catalog-uri", "http://localhost:8181/api/catalog", "Iceberg REST catalog URI")
	cmd.Flags().StringVar(&warehouse, "warehouse", "quickstart_catalog", "catalog warehouse name")
	cmd.Flags().StringVar(&clientID, "client-id", "", "catalog OAuth2 client id (required)")
	cmd.Flags().StringVar(&clientSecret, "client-secret", "", "catalog OAuth2 client secret (required)")
	cmd.Flags().StringVar(&scope, "scope", "PRINCIPAL_ROLE:ALL", "catalog OAuth2 scope")
	cmd.Flags().StringVar(&namespace, "namespace", "raw", "fallback namespace for bare targets")
	cmd.Flags().IntVar(&maxRows, "max-rows", 1000, "flush the batch once this many rows are buffered")
	cmd.Flags().DurationVar(&maxInterval, "max-interval", 2*time.Second, "flush cadence")
	cmd.Flags().StringVar(&metricsAddr, "metrics-addr", "", "serve /metrics on this address (optional)")
	cmd.Flags().StringSliceVar(&pluginPaths, "plugin", nil, "path to a Go plugin (.so); can be repeated for multiple plugins")
	cmd.Flags().StringVar(&tlsCert, "tls-cert", "", "client certificate for the control plane (mTLS; all three TLS flags required)")
	cmd.Flags().StringVar(&tlsKey, "tls-key", "", "client private key for the control plane (mTLS)")
	cmd.Flags().StringVar(&tlsCA, "tls-ca", "", "CA that signs the coordinator's server cert (mTLS)")

	root.AddCommand(cmd)
	// SIGINT/SIGTERM cancel the command context: the remote session shuts
	// down gracefully — buffered rows drain, the coordinator is told —
	// instead of the process dying at SIGKILL.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := root.ExecuteContext(ctx); err != nil {
		os.Exit(1)
	}
}

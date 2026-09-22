package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/maltzsama/urutau/internal/eventlog"
	"github.com/maltzsama/urutau/internal/historyserver"
	"github.com/maltzsama/urutau/internal/logging"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "urutau-history-server:", err)
		os.Exit(1)
	}
}

func run() error {
	root := &cobra.Command{
		Use:           "urutau-history-server",
		Short:         "Urutau history server — read-only API over terminated pipeline runs",
		SilenceUsage:  true,
		SilenceErrors: true, // main prints the single prefixed error line
	}
	root.AddCommand(serveCmd())

	// SIGINT/SIGTERM cancel the command context: the HTTP server drains.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return root.ExecuteContext(ctx)
}

type serveFlags struct {
	rootURI   string
	listen    string
	region    string
	endpoint  string
	accessKey string
	secretKey string
	pageLimit int
	logLevel  string
	logFormat string
}

func serveCmd() *cobra.Command {
	f := &serveFlags{}
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Serve the read-only history API over the eventlog trail",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			logger, err := logging.New(f.logLevel, f.logFormat)
			if err != nil {
				return err
			}
			slog.SetDefault(logger)
			if f.rootURI == "" {
				return fmt.Errorf("--root is required (s3://<bucket>/<prefix>)")
			}
			root, err := eventlog.ParseRoot(f.rootURI)
			if err != nil {
				return err
			}
			// Connection knobs come from flags; credentials otherwise follow
			// the standard AWS chain.
			root.Region = f.region
			root.Endpoint = f.endpoint
			root.AccessKey = f.accessKey
			root.SecretKey = f.secretKey
			return historyserver.Run(cmd.Context(), historyserver.Config{
				Root:      root,
				Listen:    f.listen,
				PageLimit: f.pageLimit,
				Logger:    logger,
			})
		},
	}
	fl := cmd.Flags()
	fl.StringVar(&f.rootURI, "root", "", "eventlog root: s3://<bucket>/<prefix> (required)")
	fl.StringVar(&f.listen, "listen", ":8080", "HTTP listen address")
	fl.StringVar(&f.region, "region", "", "S3 region (default us-east-1)")
	fl.StringVar(&f.endpoint, "endpoint", "", "S3 API endpoint override (MinIO-style path addressing)")
	fl.StringVar(&f.accessKey, "access-key", "", "S3 access key (else the standard AWS chain)")
	fl.StringVar(&f.secretKey, "secret-key", "", "S3 secret key")
	fl.IntVar(&f.pageLimit, "page-limit", 1000, "max events per page")
	fl.StringVar(&f.logLevel, "log-level", "info", "log level: debug|info|warn|error")
	fl.StringVar(&f.logFormat, "log-format", "text", "log format: text|json")
	return cmd
}

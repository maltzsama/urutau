package main

import (
	"os"

	"github.com/spf13/cobra"

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

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
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

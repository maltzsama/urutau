package main

import (
	"log/slog"
	"os"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	log.Error("worker not implemented yet", "component", "worker")
	os.Exit(1)
}

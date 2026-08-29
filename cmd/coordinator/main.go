package main

import (
	"log/slog"
	"os"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	log.Error("coordinator not implemented yet", "component", "coordinator")
	os.Exit(1)
}

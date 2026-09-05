// Package builtin blank-imports the built-in source and sink drivers so
// their init() functions register with the driver registry. Every binary
// that boots a pipeline imports this package; a future external plugin
// registers the same way from its own module.
package builtin

import (
	_ "github.com/maltzsama/urutau/internal/sink/iceberg"
	_ "github.com/maltzsama/urutau/internal/source/kafka"
	_ "github.com/maltzsama/urutau/internal/source/mysql"
	_ "github.com/maltzsama/urutau/internal/source/postgres"
)

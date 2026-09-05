// Package e2e holds end-to-end tests against the docker-compose stack
// (MySQL + Postgres + Polaris catalog + Trino). The proof is reading back in
// Trino and watching the old version disappear. The builtin blank import
// registers the source/sink drivers so the runner and coordinator can resolve
// them through the driver registry.
package e2e

import _ "github.com/maltzsama/urutau/internal/builtin"

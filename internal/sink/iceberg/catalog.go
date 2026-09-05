package iceberg

import (
	"strings"

	"github.com/apache/iceberg-go/table"

	"github.com/maltzsama/urutau/spec"
)

// TargetIdent resolves a target table name into an iceberg identifier,
// falling back to the spec's sink namespace for bare names.
func TargetIdent(s *spec.Spec, target string) table.Identifier {
	if ns, name, ok := strings.Cut(target, "."); ok {
		return table.Identifier{ns, name}
	}
	return table.Identifier{s.Sink.Namespace, target}
}

// CatalogConfig renders the sink config from a spec's sink section.
func CatalogConfig(s *spec.Spec) Config {
	return Config{
		URI:          s.Sink.URI,
		Warehouse:    s.Sink.Warehouse,
		ClientID:     s.Sink.ClientID,
		ClientSecret: s.Sink.ClientSecret,
		Scope:        s.Sink.Scope,
	}
}

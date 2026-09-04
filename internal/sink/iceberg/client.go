// Package iceberg wraps the Apache Iceberg REST catalog and the write paths
// exercised by the spike: create/load, append, and the
// data+equality-delete transaction that mirrors upsert. It is intentionally
// minimal — enough to prove semantics, not a general sink yet.
package iceberg

import (
	"context"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"

	// Register the S3 FileIO implementation.
	_ "github.com/apache/iceberg-go/io/gocloud"
)

// Config carries the REST catalog connection parameters.
type Config struct {
	URI          string // e.g. http://localhost:8181/api/catalog
	Warehouse    string // catalog name
	ClientID     string
	ClientSecret string
	Scope        string // OAuth2 scope, e.g. PRINCIPAL_ROLE:ALL
}

// NewCatalog dials the Polaris REST catalog using OAuth2 client credentials.
func NewCatalog(ctx context.Context, cfg Config) (catalog.Catalog, error) {
	return rest.NewCatalog(ctx, cfg.Warehouse, cfg.URI,
		rest.WithCredential(cfg.ClientID+":"+cfg.ClientSecret),
		rest.WithScope(cfg.Scope),
		rest.WithWarehouseLocation(cfg.Warehouse),
	)
}

// EnsureNamespace creates the namespace, tolerating an existing one.
func EnsureNamespace(ctx context.Context, cat catalog.Catalog, ns table.Identifier) error {
	_ = cat.CreateNamespace(ctx, ns, nil)
	return nil
}

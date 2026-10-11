// Package convert maps the engine's data to Iceberg's and Arrow's: a
// canonical schema to an Iceberg schema with field IDs (FromCanonical), and
// canonical values to Arrow builders (AppendColumn), including nested types.
// It also holds the batch helpers the writers share: splitting a batch by
// operation and reading its key values.
//
// Nothing here touches a catalog or a table.
package convert

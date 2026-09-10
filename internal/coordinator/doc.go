// Package coordinator implements the coordinator plane: the single binlog
// reader, routing to workers, the global in-flight memory budget, worker
// supervision, and the position index.
//
// # Table-name convention
//
// The name fields are easy to confuse and the confusion has caused real
// bugs (delete-image location, target routing). The rule:
//
//   - spec/source side: a table is named by its SOURCE ("db.table").
//   - ChunkRequest.Table is the SOURCE table (what to SELECT).
//   - BatchMeta.Table is the TARGET table (where the batch is written).
//   - dataplane.Batch.Table is the TARGET table (SchemaSetter confirms: the
//     resolved schema is keyed by target).
//
// Routing (route, workers) is keyed by TARGET; the canonical schema map is
// keyed by SOURCE and resolved to target when a batch or marker is built.
package coordinator

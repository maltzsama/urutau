// Package chunk reads a PostgreSQL table through SQL for the snapshot: it
// picks a strategy for the table (CTID ranges, a typed key domain, or key
// batches), splits it into chunks and scans them, as row maps or straight
// into Arrow. It also compiles the spec's row filter into the WHERE it scans
// with, and holds the row normalization the incremental path shares.
//
// The replication stream, and everything that needs a logical decoding
// plugin, lives in the parent package.
package chunk

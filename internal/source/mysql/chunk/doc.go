// Package chunk reads a MySQL table for the snapshot: it splits the table
// into primary-key ranges (Bounds, Partitions) and scans one range at a time,
// as row maps (Scan) or straight into Arrow (ScanArrow, ScanArrowPages).
//
// It knows the table only through database/sql. The binlog side of the
// source, and the spec, live in the parent package, which compiles the
// table's filter into a Filter and hands it over.
package chunk

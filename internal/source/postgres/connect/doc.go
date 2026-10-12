// Package connect resolves the source's connection settings, from a URI or
// from the nested spec, into one pgx config that the query pool and the
// replication connection share, including TLS and the SSH tunnel.
package connect

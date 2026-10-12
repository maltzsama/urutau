// Package replication manages what logical replication needs on the server
// before the stream starts: the slot, the publication and each table's
// replica identity. It also reads and advances the slot's confirmed LSN.
package replication

// Package columnar is the engine's columnar batch library: collapsing a
// batch to the last change per key, encoding keys, concatenating and
// splitting batches, sizing them.
//
// It is not the data plane's contract. That is the top-level package
// dataplane, which holds the seam type every source, sink and plugin speaks
// (dataplane.Batch) and nothing else; it is public so those contracts can
// name it. This package holds what the engine does to such batches, and stays
// internal. The two used to share one name, which read as an interface and
// its implementation; they are a type and a library that operates on it.
//
// The package knows nothing of workers, coordinators or sinks (see the
// architecture tests).
package columnar

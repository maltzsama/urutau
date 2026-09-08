package dataplane

import publicdp "github.com/maltzsama/urutau/dataplane"

// Batch is the seam type. It lives in the public dataplane package so
// that source, sink, and plugin contracts can reference it without
// violating the internal/ boundary. The alias means internal/dataplane
// and dataplane.Batch are the same type — zero conversion cost.
type Batch = publicdp.Batch

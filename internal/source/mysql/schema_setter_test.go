package mysql

import "github.com/maltzsama/urutau/source"

// The coordinator and the runner hand the resolved schema to the stream only
// through source.SchemaSetter; a stream that stops satisfying it would
// silently encode unsigned (KindUnknown) columns without their cast kind.
var _ source.SchemaSetter = stream{}

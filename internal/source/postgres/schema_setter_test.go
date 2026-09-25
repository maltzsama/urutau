package postgres

import "github.com/maltzsama/urutau/source"

// See the MySQL twin: the resolved schema reaches the stream only through
// source.SchemaSetter.
var _ source.SchemaSetter = stream{}

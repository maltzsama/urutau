// Package driver is the plugin registry. It maps a source kind or sink type
// to the factory that opens it, so the orchestration consumes only the
// source and sink contracts and never a concrete implementation. Concrete
// drivers register themselves from init() via a blank import; the zero
// Registry is safe to use and lazy-initializes its maps.
package driver

import (
	"context"
	"fmt"
	"sync"

	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// SourceFactory opens a source from a spec and runtime knobs.
type SourceFactory func(s *spec.Spec, rt source.Runtime) (source.Source, error)

// SinkFactory opens a sink from a spec.
type SinkFactory func(ctx context.Context, s *spec.Spec) (sink.Sink, error)

// DefaultSinkType is the sink type used when the spec does not declare one.
// It is the only sink type today; a future non-Iceberg sink registers its
// own type and the spec names it explicitly.
const DefaultSinkType = "iceberg+rest"

type registry struct {
	mu      sync.RWMutex
	sources map[string]sourceEntry
	sinks   map[string]SinkFactory
}

type sourceEntry struct {
	caps    source.Capabilities
	factory SourceFactory
}

var reg registry

// RegisterSource registers a source kind (mysql, postgres, kafka) with its
// static capabilities. Called from init() in each source package.
func RegisterSource(kind string, caps source.Capabilities, factory SourceFactory) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if reg.sources == nil {
		reg.sources = make(map[string]sourceEntry)
	}
	reg.sources[kind] = sourceEntry{caps: caps, factory: factory}
}

// RegisterSink registers a sink type (iceberg+rest, delta, …). Called from
// init() in each sink package.
func RegisterSink(scheme string, factory SinkFactory) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if reg.sinks == nil {
		reg.sinks = make(map[string]SinkFactory)
	}
	reg.sinks[scheme] = factory
}

// OpenSource resolves and instantiates a source for a spec's source kind.
func OpenSource(s *spec.Spec, rt source.Runtime) (source.Source, error) {
	reg.mu.RLock()
	entry, ok := reg.sources[s.Source.Kind]
	reg.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("driver: unknown source kind %q", s.Source.Kind)
	}
	return entry.factory(s, rt)
}

// CapsForKind returns a source kind's capabilities without a full spec. Used
// by admission validation, which must check resource ceilings before boot.
func CapsForKind(kind string) (source.Capabilities, error) {
	reg.mu.RLock()
	entry, ok := reg.sources[kind]
	reg.mu.RUnlock()
	if !ok {
		return source.Capabilities{}, fmt.Errorf("driver: unknown source kind %q", kind)
	}
	return entry.caps, nil
}

// OpenSink resolves and instantiates a sink for a spec's sink type.
func OpenSink(ctx context.Context, s *spec.Spec) (sink.Sink, error) {
	scheme := s.Sink.Type
	if scheme == "" {
		scheme = DefaultSinkType
	}
	reg.mu.RLock()
	factory, ok := reg.sinks[scheme]
	reg.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("driver: unknown sink type %q", scheme)
	}
	return factory(ctx, s)
}

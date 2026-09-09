// Package driver is the plugin registry. It maps a source kind or sink type
// to the factory that opens it, so the orchestration consumes only the
// source and sink contracts and never a concrete implementation. Concrete
// drivers register themselves from init() via a blank import or dynamically
// via LoadPlugin; the zero Registry is safe to use and lazy-initializes
// its maps.
package driver

import (
	"context"
	"fmt"
	"plugin"
	"slices"
	"sync"

	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// SourceFactory opens a source from a spec and runtime knobs.
type SourceFactory func(s *spec.Spec, rt source.Runtime) (source.Source, error)

// SinkFactory opens a sink from a neutral config.
type SinkFactory func(ctx context.Context, cfg sink.Config) (sink.Sink, error)

// DefaultSinkType is the sink type used when a config does not declare one.
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
//
// A duplicate registration is an error: the last-write-wins behavior would
// let a plugin silently hijack a builtin kind and redirect every pipeline
// that names it. An empty kind is an error too. Callers in init() panic on
// error — a duplicate at boot is a programming error, not a runtime choice.
func RegisterSource(kind string, caps source.Capabilities, factory SourceFactory) error {
	if kind == "" {
		return fmt.Errorf("driver: source kind must not be empty")
	}
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if _, dup := reg.sources[kind]; dup {
		return fmt.Errorf("driver: source kind %q already registered — a plugin or builtin is trying to hijack it", kind)
	}
	if reg.sources == nil {
		reg.sources = make(map[string]sourceEntry)
	}
	reg.sources[kind] = sourceEntry{caps: caps, factory: factory}
	return nil
}

// RegisterSink registers a sink type (iceberg+rest, delta, …). Called from
// init() in each sink package. A duplicate or empty type is an error (see
// RegisterSource).
func RegisterSink(scheme string, factory SinkFactory) error {
	if scheme == "" {
		return fmt.Errorf("driver: sink type must not be empty")
	}
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if _, dup := reg.sinks[scheme]; dup {
		return fmt.Errorf("driver: sink type %q already registered — a plugin or builtin is trying to hijack it", scheme)
	}
	if reg.sinks == nil {
		reg.sinks = make(map[string]SinkFactory)
	}
	reg.sinks[scheme] = factory
	return nil
}

// LoadPlugin opens a Go plugin (.so) and calls its exported Init function.
// The plugin must export:
//
//	func Init() error
//
// Init is responsible for calling RegisterSource/RegisterSink.
// Rules: same Go version, same dependency graph, Linux/macOS only.
func LoadPlugin(path string) (err error) {
	// A panicking Init must not take down the host: registration is partial
	// at that point and the error is far more actionable than a crash.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("driver: plugin %s Init panicked: %v", path, r)
		}
	}()
	p, err := plugin.Open(path)
	if err != nil {
		return fmt.Errorf("driver: open plugin %s: %w", path, err)
	}
	sym, err := p.Lookup("Init")
	if err != nil {
		return fmt.Errorf("driver: plugin %s missing exported Init: %w", path, err)
	}
	initFunc, ok := sym.(func() error)
	if !ok {
		return fmt.Errorf("driver: plugin %s Init has wrong signature (want func() error)", path)
	}
	return initFunc()
}

// registeredKinds lists the registered source kinds, sorted so diagnostics
// read deterministically. Callers must NOT hold the map lock — this acquires
// it.
func registeredKinds() []string {
	reg.mu.RLock()
	defer reg.mu.RUnlock()
	kinds := make([]string, 0, len(reg.sources))
	for k := range reg.sources {
		kinds = append(kinds, k)
	}
	slices.Sort(kinds)
	return kinds
}

// registeredSinks lists the registered sink types, sorted so diagnostics
// read deterministically. Callers must NOT hold the map lock — this acquires
// it.
func registeredSinks() []string {
	reg.mu.RLock()
	defer reg.mu.RUnlock()
	schemes := make([]string, 0, len(reg.sinks))
	for s := range reg.sinks {
		schemes = append(schemes, s)
	}
	slices.Sort(schemes)
	return schemes
}

// unknownSourceErr is the unknown-kind error. Registration happens via
// blank import (built-in) or LoadPlugin (dynamic); an absent kind usually
// means the binary was built without the driver or the plugin was not loaded.
func unknownSourceErr(kind string) error {
	return fmt.Errorf("driver: unknown source kind %q (registered: %v) — check that its package is blank-imported in internal/builtin or loaded via --plugin", kind, registeredKinds())
}

func unknownSinkErr(scheme string) error {
	return fmt.Errorf("driver: unknown sink type %q (registered: %v) — check that its package is blank-imported in internal/builtin or loaded via --plugin", scheme, registeredSinks())
}

// OpenSource resolves and instantiates a source for a spec's source kind.
func OpenSource(s *spec.Spec, rt source.Runtime) (source.Source, error) {
	if s == nil {
		return nil, fmt.Errorf("driver: nil spec")
	}
	reg.mu.RLock()
	entry, ok := reg.sources[s.Source.Kind]
	reg.mu.RUnlock()
	if !ok {
		return nil, unknownSourceErr(s.Source.Kind)
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
		return source.Capabilities{}, unknownSourceErr(kind)
	}
	return entry.caps, nil
}

// SinkTypeExists reports whether a sink type is registered. An empty type
// resolves to the default sink. Used by admission validation.
func SinkTypeExists(scheme string) bool {
	if scheme == "" {
		scheme = DefaultSinkType
	}
	reg.mu.RLock()
	_, ok := reg.sinks[scheme]
	reg.mu.RUnlock()
	return ok
}

// OpenSink resolves and instantiates a sink for a spec's sink section.
func OpenSink(ctx context.Context, s *spec.Spec) (sink.Sink, error) {
	return OpenSinkConfig(ctx, SinkConfig(s))
}

// Sink option keys — the neutral-config contract between SinkConfig and each
// sink factory. Named constants so a typo is a compile error, not a silent
// "" read. client_secret is a credential: never log the Options map.
const (
	OptWarehouse    = "warehouse"
	OptClientID     = "client_id"
	OptClientSecret = "client_secret"
	OptScope        = "scope"
	OptCommitMode   = "commit_mode"
)

// SinkConfig renders a spec's sink section into the neutral config.
func SinkConfig(s *spec.Spec) sink.Config {
	return sink.Config{
		Type:      s.Sink.Type,
		URI:       s.Sink.URI,
		Namespace: s.Sink.Namespace,
		Options: map[string]string{
			OptWarehouse:    s.Sink.Warehouse,
			OptClientID:     s.Sink.ClientID,
			OptClientSecret: s.Sink.ClientSecret,
			OptScope:        s.Sink.Scope,
			OptCommitMode:   string(s.Sink.CommitMode),
		},
	}
}

// OpenSinkConfig resolves and instantiates a sink from a neutral config.
func OpenSinkConfig(ctx context.Context, cfg sink.Config) (sink.Sink, error) {
	scheme := cfg.Type
	if scheme == "" {
		scheme = DefaultSinkType
	}
	reg.mu.RLock()
	factory, ok := reg.sinks[scheme]
	reg.mu.RUnlock()
	if !ok {
		return nil, unknownSinkErr(scheme)
	}
	return factory(ctx, cfg)
}

// ValidateParallelism rejects a parallel-chunk setting above the ceiling the
// source driver declares: a shared MySQL with max_connections=100 does not
// tolerate the same snapshot concurrency as a dedicated Postgres. A ceiling
// of 0 means the driver has no opinion.
func ValidateParallelism(kind string, maxParallelChunks int) error {
	if maxParallelChunks < 0 {
		return fmt.Errorf("driver: maxParallelChunks %d is negative", maxParallelChunks)
	}
	if maxParallelChunks == 0 {
		return nil
	}
	caps, err := CapsForKind(kind)
	if err != nil {
		return err
	}
	if caps.MaxConnections > 0 && maxParallelChunks > caps.MaxConnections {
		return fmt.Errorf("driver: maxParallelChunks (%d) exceeds the %s driver ceiling (%d connections)",
			maxParallelChunks, kind, caps.MaxConnections)
	}
	return nil
}

// resetRegistry clears the registry. TEST-ONLY: it exists so driver tests can
// assert empty-registry diagnostics without being polluted by other tests'
// registrations. Never call from production code.
func resetRegistry() {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	reg.sources = nil
	reg.sinks = nil
}

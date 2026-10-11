// Package plugin implements adapters that wrap the Flight client into the
// standard source.Source and sink.Sink interfaces. This lets the runner
// drive external plugins identically to native drivers.
package plugin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/plugin/client"
	"github.com/maltzsama/urutau/internal/plugin/contract"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// ServerStopper is an in-process Flight server an adapter owns and must stop
// when it closes. flightwrap passes one; the subprocess path passes none — the
// stage supervisor owns that process (issue #496).
type ServerStopper interface{ Stop() }

// ClientFunc resolves the plugin's CURRENT Flight client, or nil when it is
// not connected. A subprocess plugin can be restarted by its supervisor, so an
// adapter must resolve the client per use instead of pinning the one that was
// live when it was built; an in-process plugin (flightwrap) passes a constant
// (issue #570).
type ClientFunc func() *client.Client

// StringPosition is an opaque position backed by a base64 offset string.
// External plugins use opaque offsets; the runner never inspects them.
type StringPosition struct {
	Offset string
}

func (p StringPosition) String() string { return p.Offset }

// Compare is identity-only. An opaque offset is a plugin cookie (contract
// §8.2), not an ordinal — the base64 alphabet does not preserve byte order,
// so lexicographic comparison is meaningless and can silently reorder
// resume points (skipping uncommitted data). The only defined relation is
// identity; anything else is position.Incomparable and callers MUST handle
// it conservatively.
func (p StringPosition) Compare(other position.Position) int {
	o, ok := other.(StringPosition)
	if !ok || p.Offset != o.Offset {
		return position.Incomparable
	}
	return 0
}

// Contains is identity-only for the same reason: an opaque offset does not
// contain another unless it is the same cookie.
func (p StringPosition) Contains(other position.Position) bool {
	o, ok := other.(StringPosition)
	return ok && p.Offset == o.Offset
}

// SourceAdapter wraps a Flight client as a source.Source. It speaks the
// external plugin contract (GetFlightInfo + DoGet) and projects the plugin's
// Arrow CDC record stream (contract §8.1) straight into the flat urutau wire
// schema.
type SourceAdapter struct {
	clients ClientFunc
	spec    spec.Source
	logger  *slog.Logger
	alloc   memory.Allocator
	// server is the in-process Flight server this adapter owns (flightwrap);
	// nil for a subprocess plugin, whose supervisor owns the process.
	server    ServerStopper
	closeOnce sync.Once
	closeErr  error
}

// NewSourceAdapter creates a source adapter over a connected Flight client.
// An optional ServerStopper is the in-process server flightwrap started, which
// Close stops (issue #496).
func NewSourceAdapter(clients ClientFunc, src spec.Source, logger *slog.Logger, servers ...ServerStopper) *SourceAdapter {
	if logger == nil {
		logger = slog.Default()
	}
	a := &SourceAdapter{
		clients: clients,
		spec:    src,
		logger:  logger,
		alloc:   memory.NewGoAllocator(),
	}
	if len(servers) > 0 {
		a.server = servers[0]
	}
	return a
}

// use resolves the adapter's current client, or nil when the plugin is down.
func (a *SourceAdapter) use() *client.Client {
	if a.clients == nil {
		return nil
	}
	return a.clients()
}

// Close releases the adapter: it closes the Flight client and stops the
// in-process server flightwrap started. Idempotent, so a reader's Close and a
// direct caller may both call it (issue #496).
func (a *SourceAdapter) Close() error {
	a.closeOnce.Do(func() {
		if c := a.use(); c != nil {
			a.closeErr = c.Close()
		}
		if a.server != nil {
			a.server.Stop()
		}
	})
	return a.closeErr
}

// Introspect resolves one spec table into its ref, schema, and warnings.
// For external plugins, schema comes from GetFlightInfo in snapshot mode.
// Validates that declared primary key columns exist in the returned schema.
func (a *SourceAdapter) Introspect(ctx context.Context, t spec.Table) (core.TableRef, core.Schema, []core.Warning, error) {
	c := a.use()
	if c == nil {
		return core.TableRef{}, core.Schema{}, nil, errors.New("plugin source: not connected")
	}
	info, err := c.GetFlightInfo(ctx, contract.GetFlightInfoRequest{
		Table: t.Source,
		Mode:  "snapshot",
	})
	if err != nil {
		return core.TableRef{}, core.Schema{}, nil, fmt.Errorf("introspect %s: %w", t.Source, err)
	}
	arrowSchema, err := flight.DeserializeSchema(info.Schema, a.alloc)
	if err != nil {
		return core.TableRef{}, core.Schema{}, nil, fmt.Errorf("deserialize schema: %w", err)
	}
	schema := arrowToCoreSchema(arrowSchema)

	// Validate PK columns exist in the schema.
	for _, pk := range t.PrimaryKey {
		found := false
		for _, c := range schema.Columns {
			if c.Name == pk {
				found = true
				break
			}
		}
		if !found {
			return core.TableRef{}, core.Schema{}, nil, fmt.Errorf("introspect %s: primary key column %q not in plugin schema", t.Source, pk)
		}
	}

	ref := core.TableRef{
		Source:     t.Source,
		Target:     t.Target,
		PrimaryKey: t.PrimaryKey,
	}
	return ref, schema, nil, nil
}

// InitialPosition returns the empty position (first boot). External
// plugins always start from the beginning unless a snapshot resume is
// in progress.
func (a *SourceAdapter) InitialPosition(_ context.Context) (position.Position, error) {
	return StringPosition{}, nil
}

// ParsePosition decodes a stored position string.
func (a *SourceAdapter) ParsePosition(s string) (position.Position, error) {
	return StringPosition{Offset: s}, nil
}

// Open opens the replication reader. It calls GetFlightInfo in changes
// mode to get the schema and then starts streaming via DoGet.
func (a *SourceAdapter) Open(ctx context.Context, refs []core.TableRef) (source.Reader, error) {
	ctx, cancel := context.WithCancel(ctx)
	// An in-process source's client and server are owned by this adapter, and
	// the reader's Close is the only teardown hook the source contract gives
	// (source.Reader.Close). A subprocess source's supervisor owns its client,
	// so leave it alone (issue #496).
	var closeAdapter func() error
	if a.server != nil {
		closeAdapter = a.Close
	}
	r := &sourceReader{
		clients:      a.clients,
		alloc:        a.alloc,
		refs:         refs,
		ctx:          ctx,
		cancel:       cancel,
		logger:       a.logger,
		out:          make(chan sourceResult, 16),
		closeAdapter: closeAdapter,
	}
	return r, nil
}

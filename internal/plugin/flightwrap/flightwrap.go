// Package flightwrap implements driver.PluginWrap: it turns a .so's raw
// registered source/sink factories into factories that serve the real
// driver over an in-process Arrow Flight server (internal/plugin/
// flightserver) and hand back a Flight client adapter (internal/plugin) —
// the SAME contract a subprocess plugin speaks. A .so author's
// RegisterSource/RegisterSink code never changes; this package is only
// wired in by cmd/* right after driver.LoadPlugin's Init call.
package flightwrap

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"log/slog"

	"github.com/maltzsama/urutau/driver"
	pluginadapter "github.com/maltzsama/urutau/internal/plugin"
	"github.com/maltzsama/urutau/internal/plugin/client"
	"github.com/maltzsama/urutau/internal/plugin/flightserver"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// Wrap is the driver.PluginWrap implementation. The zero value is ready to
// use.
type Wrap struct{}

var _ driver.PluginWrap = Wrap{}

// WrapSource returns a SourceFactory that builds the plugin's real source,
// serves it over an in-process Flight server, and returns a Flight client
// adapter in its place.
func (Wrap) WrapSource(path string, real driver.SourceFactory) driver.SourceFactory {
	return func(s *spec.Spec, rt source.Runtime) (source.Source, error) {
		realSrc, err := real(s, rt)
		if err != nil {
			return nil, fmt.Errorf("flightwrap: plugin %s: %w", path, err)
		}
		token, err := randomToken()
		if err != nil {
			return nil, err
		}
		srv, err := flightserver.Start("", token, flightserver.NewSourceServer(realSrc, s, rt))
		if err != nil {
			return nil, fmt.Errorf("flightwrap: plugin %s: start in-process flight server: %w", path, err)
		}
		c, err := client.Connect(context.Background(), srv.Addr(), token, rt.Logger)
		if err != nil {
			srv.Stop()
			return nil, fmt.Errorf("flightwrap: plugin %s: connect in-process flight server: %w", path, err)
		}
		return pluginadapter.NewSourceAdapter(c, s.Source, rt.Logger), nil
	}
}

// WrapSink returns a SinkFactory that builds the plugin's real sink, serves
// it over an in-process Flight server, and returns a Flight client adapter
// in its place.
func (Wrap) WrapSink(path string, real driver.SinkFactory) driver.SinkFactory {
	return func(ctx context.Context, cfg sink.Config) (sink.Sink, error) {
		realSnk, err := real(ctx, cfg)
		if err != nil {
			return nil, fmt.Errorf("flightwrap: plugin %s: %w", path, err)
		}
		token, err := randomToken()
		if err != nil {
			return nil, err
		}
		srv, err := flightserver.Start("", token, flightserver.NewSinkServer(realSnk))
		if err != nil {
			return nil, fmt.Errorf("flightwrap: plugin %s: start in-process flight server: %w", path, err)
		}
		c, err := client.Connect(ctx, srv.Addr(), token, slog.Default())
		if err != nil {
			srv.Stop()
			return nil, fmt.Errorf("flightwrap: plugin %s: connect in-process flight server: %w", path, err)
		}
		return pluginadapter.NewSinkAdapter(c, slog.Default()), nil
	}
}

// randomToken generates the bearer token for one in-process Flight server —
// never persisted, only used within this process's own dial.
func randomToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("flightwrap: generate plugin token: %w", err)
	}
	return base64.URLEncoding.EncodeToString(b), nil
}

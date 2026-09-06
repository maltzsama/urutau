# Dynamic Plugins

Urutau supports dynamic driver loading via Go plugins (`.so` files). This allows you to add custom sources and sinks without recompiling the Urutau binary.

## How It Works

1. Write a Go plugin that exports an `Init()` function
2. Compile it as a `.so` file with `-buildmode=plugin`
3. Pass the plugin path via `--plugin` flag when running Urutau

## Writing a Plugin

A plugin must export a single function:

```go
package main

import (
    "github.com/maltzsama/urutau/driver"
    "github.com/maltzsama/urutau/source"
    "github.com/maltzsama/urutau/spec"
)

func Init() error {
    driver.RegisterSource("my_source", source.Capabilities{
        Stream: true,
    }, func(s *spec.Spec, rt source.Runtime) (source.Source, error) {
        return &mySource{}, nil
    })
    return nil
}
```

### Requirements

- The `Init()` function must have signature `func() error`
- Use `driver.RegisterSource()` for source drivers
- Use `driver.RegisterSink()` for sink drivers
- Import only public packages from the Urutau module

### Example: Source Plugin

```go
package main

import (
    "context"
    
    "github.com/maltzsama/urutau/core"
    "github.com/maltzsama/urutau/driver"
    "github.com/maltzsama/urutau/position"
    "github.com/maltzsama/urutau/source"
    "github.com/maltzsama/urutau/spec"
)

func Init() error {
    driver.RegisterSource("custom_db", source.Capabilities{
        Stream: true,
    }, func(s *spec.Spec, rt source.Runtime) (source.Source, error) {
        return &customSource{uri: s.Source.URI}, nil
    })
    return nil
}

type customSource struct {
    uri string
}

func (s *customSource) Introspect(ctx context.Context, t spec.Table) (core.TableRef, core.Schema, []core.Warning, error) {
    // Return table schema
    return core.TableRef{}, core.Schema{}, nil, nil
}

func (s *customSource) InitialPosition(ctx context.Context) (position.Position, error) {
    return nil, nil
}

func (s *customSource) ParsePosition(p string) (position.Position, error) {
    return nil, nil
}

func (s *customSource) Open(ctx context.Context, refs []source.TableRef) (source.Reader, error) {
    return nil, nil
}
```

## Compiling a Plugin

```bash
go build -buildmode=plugin -o my_plugin.so
```

### Important: Version Compatibility

The plugin **must** be compiled with:

- **Exactly the same Go version** as the Urutau binary
- **The same dependency versions** (check `go.mod`)

If versions mismatch, `plugin.Open()` will fail with a version error.

## Using Plugins

### Local Mode (urutau run)

```bash
urutau run --plugin ./my_plugin.so -f pipeline.yaml
```

### Distributed Mode (coordinator + worker)

```bash
# Coordinator
urutau-coordinator run --plugin ./my_plugin.so -f pipeline.yaml

# Worker (must have access to the same .so file)
urutau-worker run --plugin ./my_plugin.so --coordinator localhost:50051
```

### Multiple Plugins

```bash
urutau run --plugin ./source.so --plugin ./sink.so -f pipeline.yaml
```

## Limitations

| Limitation | Description |
|------------|-------------|
| **Linux/macOS only** | Go plugins are not supported on Windows |
| **Same Go version** | Plugin and binary must use identical Go versions |
| **Same dependencies** | Public dependencies must match exactly |
| **No sandbox** | Plugin code runs in the same process with full access |
| **No unloading** | Plugins cannot be unloaded once loaded |

## Troubleshooting

### "plugin.Open: not implemented"

Platform not supported. Use Linux or macOS.

### "plugin: version mismatch"

Go version mismatch. Recompile the plugin with the same Go version as Urutau.

### "plugin: not a plugin file"

The `.so` file was not compiled with `-buildmode=plugin`.

### "missing exported Init"

The plugin doesn't export an `Init` function. Add `func Init() error` to your plugin.

## Architecture

```
┌─────────────────────────────────────┐
│           Urutau Binary             │
│  ┌───────────────────────────────┐  │
│  │       driver/registry.go      │  │
│  │  RegisterSource()             │  │
│  │  RegisterSink()               │  │
│  │  LoadPlugin() ──────────────┐ │  │
│  └───────────────────────────────┘  │
│                                     │
│  ┌─────────────┐  ┌─────────────┐  │
│  │  built-in   │  │   plugin    │  │
│  │  drivers    │  │   (.so)     │  │
│  │  (init())   │  │  Init()     │  │
│  └─────────────┘  └─────────────┘  │
└─────────────────────────────────────┘
```

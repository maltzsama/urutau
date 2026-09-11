# Writing a driver

The engine is closed; the driver seam is open. A source or sink is a package
that implements the public contracts (`source`, `sink`, `core`, `dataplane`,
`position`) and never imports anything under `internal/`.

There are **two ways** to add a driver. Pick one:

| | Go plugin (`.so`) | Subprocess (Arrow Flight) |
|---|---|---|
| Language | Go only | any language that speaks Arrow Flight/gRPC |
| Runs | in-process, same binary | separate OS process |
| Isolation | none — full access, a crash takes Urutau down with it | full — a crash is contained, restarted with backoff |
| Build/deploy | same Go version + dependency versions as the Urutau binary | independent binary, own build/release cycle |
| Best for | internal drivers you build and deploy alongside Urutau, same repo/CI | third-party drivers, drivers in another language, anything you don't want sharing a process with Urutau |

If you're not sure: **use the subprocess contract.** It's slower to write
(you implement Arrow Flight RPCs, not just Go interfaces) but it's the one
with isolation, independent versioning, and a normative spec another
implementer can follow without reading Urutau's source.

## Reference implementation

[`test/plugin`](../../test/plugin/fake.go) is a working source and sink
written against nothing but the public contracts (`source`, `sink`, `core`,
`dataplane`, `position`), exercised end-to-end by its own test. It proves
the contracts are implementable from outside `internal/` — it isn't wired
as a `.so` or a Flight server itself, but the `Source`/`Sink` interface
implementations inside it are exactly what either mechanism wraps.

---

## Option A: Go plugin (`.so`)

In-process, same Go version and dependencies as the Urutau binary,
no sandbox.

### Write it

A plugin is a `package main` that exports `func Init() error`:

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

`Init()` must have exactly this signature. Use `driver.RegisterSource` /
`driver.RegisterSink`. Import only public packages from the Urutau module —
never `internal/`.

### Build and run it

```sh
go build -buildmode=plugin -o my_plugin.so
urutau run --plugin ./my_plugin.so -f pipeline.yaml
```

Distributed mode: pass `--plugin` to both `urutau-coordinator` and
`urutau-worker` (the worker needs the same `.so` file). Multiple plugins:
repeat the flag.

### The one hard requirement

**The plugin must be compiled with the exact same Go version and dependency
versions as the Urutau binary.** `plugin.Open()` fails with a version
mismatch error otherwise — there is no compatibility window, no partial
match.

### Limitations

- Linux/macOS only (Go plugins aren't supported on Windows).
- No sandbox: plugin code runs in the same process with full access, and a
  plugin panic can take Urutau down.
- No unloading once loaded.

### Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `plugin.Open: not implemented` | platform not supported | use Linux or macOS |
| `plugin: version mismatch` | Go version differs | recompile the plugin with the same Go version as Urutau |
| `plugin: not a plugin file` | missing `-buildmode=plugin` | rebuild with that flag |
| `missing exported Init` | no `Init()` func | add `func Init() error` |

---

## Option B: subprocess (Arrow Flight)

Any language. The plugin is an Arrow Flight gRPC **server**; Urutau is the
client. It runs as its own OS process, spawned and supervised by Urutau —
restarted with backoff on crash, killed on an anti-orphan watchdog if
Urutau itself dies.

This is the **normative** contract: **[`docs/reference/plugin-contract.md`](../reference/plugin-contract.md)**.
If anything here (or anywhere else) conflicts with that document, the
contract wins.

### The shape, briefly

1. Urutau spawns your binary with env vars (`URUTAU_STAGE`, `URUTAU_TOKEN`,
   `URUTAU_CONFIG`, a socket or TCP address).
2. Your process opens a Flight server, listens, then writes one JSON
   readiness line to stdout: `{"ready":true,"protocolVersion":1,"pid":1234}`.
3. Urutau connects, does a token handshake, and starts calling RPCs:
   `GetFlightInfo`/`DoGet` (source, snapshot then CDC), or `DoPut`+`flush`
   (sink).
4. Your process answers a heartbeat action every ≤1s and dies on its own if
   heartbeats stop arriving for 15s (anti-orphan watchdog) — Urutau crashing
   must never leave your process running.

That's the summary. The actual RPC map, wire schema for CDC records, offset
rules, error codes, and the five numbered invariants (I1–I5) a conformant
plugin must satisfy are all in the contract doc — read it fully before
implementing either role.

### Conformance

The contract's §16 checklist is the spec against which a plugin's
conformance is judged (a `urutau plugin validate --smoke` command is
described there — check the CLI's own `--help` for whether it has landed
yet). Work through the checklist by hand against your binary either way
before calling it done.

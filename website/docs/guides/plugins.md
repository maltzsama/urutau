---
sidebar_position: 1
---

# Writing a plugin

The engine is closed; the driver seam is open. A source or sink is a
package that implements the public contracts (`source`, `sink`, `core`,
`dataplane`, `position`) and never imports anything under `internal/`.

**The contract is Arrow Flight.** [Plugin Contract](../reference/plugin-contract.md)
is the normative spec — gRPC, any language, its own process, restarted
with backoff on crash. If anything anywhere conflicts with that document,
the contract wins. This is what "writing a plugin" means; read that
document, not this page, for the actual RPC map, wire schema, offset
rules, and the five invariants a conformant plugin must satisfy.

This page is the on-ramp: what the contract looks like in outline, and the
one alternative — an in-process Go `.so` — that exists for a narrower
case.

## The shape, briefly

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

### Conformance

The contract's §16 checklist is the spec against which a plugin's
conformance is judged (a `urutau plugin validate --smoke` command is
described there — check the CLI's own `--help` for whether it has landed
yet). Work through the checklist by hand against your binary either way
before calling it done.

### Reference implementation

[`test/plugin`](https://github.com/maltzsama/urutau/tree/main/test/plugin/fake.go)
is a working source and sink written against nothing but the public
contracts, exercised end-to-end by its own test. It isn't wired as a
Flight server itself, but the `Source`/`Sink` implementations inside it
are exactly what the Flight server wraps — read it to see the interfaces
in isolation from the RPC plumbing.

## The alternative: in-process Go `.so`

`driver.LoadPlugin` loads a Go plugin built with `-buildmode=plugin` into
the same process — no gRPC, no subprocess, no isolation. It exists for
one case: a driver you're building **in Go**, deployed **alongside**
Urutau in the same build/release, where you don't need another language
and don't need the crash containment a separate process buys you.

It is not a second normative contract. There's no invariant checklist, no
protocol version, no conformance suite — just Go interfaces
(`source.Source`, `sink.Sink`) loaded at runtime. Treat it as a shortcut
for internal use, not something to design a third-party integration
around.

```go
// package main
func Init() error {
    driver.RegisterSource("my_source", source.Capabilities{
        Stream: true,
    }, func(s *spec.Spec, rt source.Runtime) (source.Source, error) {
        return &mySource{}, nil
    })
    return nil
}
```

```sh
go build -buildmode=plugin -o my_plugin.so
urutau run --plugin ./my_plugin.so -f pipeline.yaml
```

Distributed mode: pass `--plugin` to both `urutau-coordinator` and
`urutau-worker` (the worker needs the same `.so` file).

**The hard requirement:** the plugin must be compiled with the exact same
Go version and dependency versions as the Urutau binary — `plugin.Open()`
fails with a version-mismatch error otherwise, no partial match. Add: no
sandbox (a plugin panic takes Urutau down with it), Linux/macOS only, no
unloading once loaded.

| Error | Cause | Fix |
|---|---|---|
| `plugin.Open: not implemented` | platform not supported | use Linux or macOS |
| `plugin: version mismatch` | Go version differs | recompile with the same Go version as Urutau |
| `plugin: not a plugin file` | missing `-buildmode=plugin` | rebuild with that flag |
| `missing exported Init` | no `Init()` func | add `func Init() error` |

If you're building anything you'll ship independently, in any language, or
just don't want sharing a process with Urutau: use the Flight contract
above instead.

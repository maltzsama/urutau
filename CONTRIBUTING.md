# Contributing

Thanks for looking. This is a small project; the goal is a short review loop
and a readable history.

## Ground rules

- **English only** — code, comments, commit messages, issues, and docs.
- **Atomic commits** — one logical change per commit, each one builds and
  passes the suite.
- **Conventional Commits** — `feat:`, `fix:`, `docs:`, `refactor:`, `test:`,
  `chore:`, `ci:`, with an optional scope (`fix(coordinator): ...`). Releases
  are generated from these by release-please: a `feat` bumps the minor, a
  `fix` bumps the patch, and `!` marks a breaking change.
- **Keep diffs reviewable** — no drive-by reformatting.

## Before you open a PR

```sh
make bootstrap        # buf, golangci-lint, setup-envtest into ./bin
make envtest-setup    # operator envtest control plane (tests skip without it)
make lint             # golangci-lint
make test             # go test -race ./...
make build            # the four binaries
```

The e2e suite needs Docker; `make e2e-test` brings the stack up. See the
[Development](README.md#development) section and the
[architecture overview](https://maltzsama.github.io/urutau/docs/architecture/overview).

## Tests

A fix without a test that fails before it is usually a fix that comes back.
Prefer a test against the live path over one against a copy of the logic — the
engine is columnar, so assert on columns.

## File and function size

`make test` runs a size ratchet (`internal/architecture/size_test.go`). In
production code (not `_test.go`, `*.pb.go` or generated files), a file may
not exceed **800 lines** and a function **120 lines**, counted from `func` to
its closing brace.

Code that was already over the limits is listed in
`internal/architecture/size_allowlist.txt` at its size when the ratchet
landed. The list only shrinks:

- an allowlisted item that grows past its recorded size fails; split it
  instead;
- an entry whose item drops back under the limit, or no longer exists, fails
  as stale; delete the entry in the same PR. To shrink every entry after a
  split, run
  `URUTAU_SIZE_ALLOWLIST_WRITE=1 go test ./internal/architecture -run TestSizeRatchet`
  and review the diff: it must only lower sizes or remove lines;
- a genuine exemption (a data table, an exhaustive type switch) goes in the
  list with a `# why` comment, in the PR that adds it.

## Sign-off (DCO vs CLA) — open

This project has not chosen a contributor sign-off policy yet. The two options
are a per-commit **DCO** (`Signed-off-by`, no paperwork, no rights grant) or a
one-time **CLA** (a rights grant, required for a future Apache Foundation
donation). Until that is decided, contributions are accepted under the
project's [Apache-2.0 license](LICENSE) as submitted.

The decision is tracked in the
[roadmap](https://maltzsama.github.io/urutau/docs/reference/roadmap#registered-for-v2-not-v1-gaps).

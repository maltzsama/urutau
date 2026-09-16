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

## Sign-off (DCO vs CLA) — open

This project has not chosen a contributor sign-off policy yet. The two options
are a per-commit **DCO** (`Signed-off-by`, no paperwork, no rights grant) or a
one-time **CLA** (a rights grant, required for a future Apache Foundation
donation). Until that is decided, contributions are accepted under the
project's [Apache-2.0 license](LICENSE) as submitted.

The decision is tracked in the
[roadmap](https://maltzsama.github.io/urutau/docs/reference/roadmap#registered-for-v2-not-v1-gaps).

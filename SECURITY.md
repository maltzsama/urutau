# Security policy

## Reporting a vulnerability

**Do not open a public issue for a security problem.** Use GitHub's private
vulnerability reporting:

<https://github.com/maltzsama/urutau/security/advisories/new>

That opens a private advisory only the maintainers can see. If you cannot use
it, email the maintainer (the address on their GitHub profile) with
`urutau security` in the subject.

Please include what you found, how to reproduce it, and the impact you believe
it has. We aim to acknowledge within a few days and to agree on a disclosure
timeline with you before anything is public.

## Scope

Urutau connects to production databases and message logs, so the
highest-value targets are:

- **Credential handling** — source DSNs and catalog credentials, wherever they
  travel: specs, Kubernetes Secrets, environment, logs, error messages, and
  the audit trail. A credential leaking through a log line or an error string
  is a security bug, not a cosmetic one.
- **The control plane** — the coordinator↔worker gRPC/Flight channel carries
  the source DSN in the worker assignment. Running it without mTLS is
  documented as plaintext and warns at startup; a way to read it *despite*
  TLS is a bug.
- **SQL and identifier handling** — table names, column names, and filters come
  from the pipeline spec and are interpolated into queries against the source
  and the sinks.
- **The plugin seam** — the Arrow Flight subprocess contract and its token.

## Verifying released images

The container image published to `ghcr.io/maltzsama/urutau` is signed with
[cosign](https://docs.sigstore.dev/cosign/overview/) in keyless mode (OIDC via
the GitHub Actions workflow identity) and carries SBOM and provenance
attestations. Verify a tag before running it:

```sh
cosign verify ghcr.io/maltzsama/urutau:latest \
  --certificate-identity-regexp '^https://github.com/maltzsama/urutau/' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

Each release is also scanned for vulnerabilities; the workflow fails on any
fixable `CRITICAL`/`HIGH` CVE, so a published tag has no known fixable
high-severity finding at the time it was built.

## Supported versions

The latest release is supported. Fixes land on `main` and ship in the next
release.

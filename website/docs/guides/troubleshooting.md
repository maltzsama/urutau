---
sidebar_position: 9
---

# Troubleshooting

Every entry follows the same shape:

> **Symptom** — what you see. **Cause** — why. **Fix** — what to do.

Start from the symptom. Entries are grouped by where the problem lives:
[Startup](#startup), [Kubernetes](#kubernetes), [Workers](#workers),
[Data](#data).

## Startup

### The process exits immediately with a list of problems

**Symptom** — `urutau run` or the coordinator exits at boot, printing
lines like `spec: tables[0].primaryKey: required when writeMode is upsert`.

**Cause** — validation failed. The messages name the exact field path.

**Fix** — correct the spec. The same rules run in the Kubernetes webhook,
so a spec that passes admission will not fail here for *shape* reasons —
only for connectivity or a missing Secret. See
[Pipeline specification](../reference/pipeline-spec.md#validation).

### `driver: unknown source kind` / `unknown sink type`

**Symptom** — boot fails with
`driver: unknown source kind "bogus" (registered: [kafka mysql postgres])`.

**Cause** — the `kind`/`type` names a driver that is not registered.

**Fix** — check the spelling. If it is a third-party driver, make sure its
package is blank-imported in `internal/builtin` or loaded with `--plugin`.
The error lists what is registered.

## Kubernetes

### The operator Pod is in `CreateContainerConfigError`

**Symptom** — the operator Pod never starts; the event says
`container has runAsNonRoot and image has non-numeric user (nonroot)`.

**Cause** — the distroless image's `USER` is the non-numeric `nonroot`,
which `runAsNonRoot: true` alone cannot verify.

**Fix** — the shipped manifest pins `runAsUser: 65532` /
`runAsGroup: 65532`. If you copied the Deployment elsewhere, carry those
fields.

### A worker crashloops with `required flag(s) ... not set`

**Symptom** — a worker Pod restarts with
`urutau-worker: required flag(s) "client-id", "client-secret" not set`.

**Cause** — the worker could not read its catalog credentials.

**Fix** — a worker gets its catalog settings from the `URUTAU_SINK_*` env
the operator mounts from the Secret; it cannot be passed as a flag, because
the operator does not know a Secret's value. If a worker demands
`--client-id`, you are running an image older than the env fallback —
rebuild. Otherwise check that the catalog Secret exists and carries the
expected keys ([key convention](deploy-kubernetes.md#3-provide-credentials-as-secrets)).

### The webhook rejects a spec that has no `uri`

**Symptom** — `kubectl apply` fails with
`spec.definition.inline: source.uri: required`, even though the inline spec
is supposed to leave it empty.

**Cause** — the inline spec is *supposed* to omit the URIs; the operator
mounts the Secrets and the coordinator resolves them at boot.

**Fix** — the webhook validates with `spec.WithoutCredentials()`, so it must
accept that shape. If it rejects an empty `uri`, you are running an operator
older than that fix — rebuild the image.

### `no such host` for `host.minikube.internal`

**Symptom** — a Pod cannot resolve `host.minikube.internal`.

**Cause** — some minikube builds put the name in the node's `/etc/hosts`
but not in CoreDNS, so Pods cannot see it.

**Fix** — patch CoreDNS, or use the gateway IP (`192.168.49.1`) directly.
See [Local end-to-end](deploy-kubernetes.md#local-end-to-end-with-minikube).

### `minikube image load` reports a conflict

**Symptom** — `minikube image load` fails because a container is using the
image.

**Cause** — a running Pod still holds the tag, so the node will not replace
it.

**Fix** — build into the daemon instead, which retags regardless:

```sh command="make k8s-load"
make k8s-load      # eval $(minikube docker-env) && docker build -t urutau:dev .
```

## Workers

### A worker never connects and the coordinator times out

**Symptom** — the coordinator waits `--wait-worker` and then fails, with no
session for one of the expected workers.

**Cause** — the derived worker name never showed up.

**Fix** — check:

- the worker's `--name` matches the derived name
  `<pipeline>-<target>-<index>`;
- the worker can reach `--coordinator` (address, firewall, Service DNS);
- under mTLS, all three TLS flags are set on both sides.

The coordinator logs every `worker session` as it arrives; the missing one
is the problem.

### `coordinator: reset worker` keeps appearing

**Symptom** — a steady stream of `coordinator: reset worker` log lines.

**Cause** — a worker is not acking within `--ack-timeout`, so the
coordinator re-routes its partition. A few resets are normal (a worker
restart); a steady stream means the worker is slow or dying — usually the
sink, not the network.

**Fix** — check the sink. After `--max-resets` within `--reset-window` the
job terminates by design; tune those in
[Reliability](reliability.md#supervision-the-coordinator-heals-workers).

## Data

### Data is not showing up, but the process is running

**Symptom** — the process is healthy, the destination table is empty or
stale.

**Cause** — several possibilities, in order of likelihood.

**Fix** — work through it in order:

1. **Is the snapshot done?** Watch `urutau_worker_snapshot_progress_ratio`.
   Streaming starts after the snapshot.
2. **Is the position advancing?** `urutau_coordinator_lag_seconds` and
   `/statusz` (see [Monitoring](monitoring.md)).
3. **Is the commit landing?** `urutau_coordinator_commits_total` and
   `urutau_worker_commit_duration_seconds`.
4. **Read it back through a real engine.** A `SELECT` in Trino/ClickHouse
   is the only proof; the engine's own logs are not.

An `UPDATE`/`DELETE` takes a commit interval to appear — the worker batches
changes, it is not row-by-row instant.

### `urutau_worker_commit_failures_total` is rising

**Symptom** — the commit-failure counter climbs.

**Cause** — the catalog is rejecting commits.

**Fix** — check catalog reachability, credentials, and the warehouse's
storage (S3/rustfs) health. Persisted failures are a data-loss risk: commits
are how the position advances.

### The pipeline re-snapshots on every restart

**Symptom** — every restart re-reads all rows instead of resuming.

**Cause** — the position is not being read back from the destination.

**Fix** — check that the `target` table is the same (a changed `target` is
a new table, so it has no position) and that the sink is the one you think
it is. The position lives in the destination; see
[Position](../architecture/state-position.md).

## Still stuck

Open an issue with the spec (redact credentials), the exact error, and the
relevant logs (`--log-format json` helps). If a doc page did not work as
written, that is a doc bug — say so in the issue.

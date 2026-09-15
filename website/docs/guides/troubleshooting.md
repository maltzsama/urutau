---
sidebar_position: 8
---

# Troubleshooting

Symptom first. Each entry says what to check and what it usually means.

## The process exits immediately with a list of problems

Validation failed at boot. The messages name the exact field path:

```
spec: tables[0].primaryKey: required when writeMode is upsert
```

Fix the spec. The same rules run in the webhook, so a spec that passes
admission will not fail here for *shape* reasons — only for connectivity
or a missing Secret. See [Specification](../reference/spec.md#validation).

## `driver: unknown source kind` / `unknown sink type`

The kind/type is not a registered driver. Check the spelling, and that the
driver's package is blank-imported in `internal/builtin` (or loaded with
`--plugin`). The error lists what is registered:

```
driver: unknown source kind "bogus" (registered: [kafka mysql postgres])
```

## Kubernetes: the operator Pod is in `CreateContainerConfigError`

The kubelet cannot verify the container's user. The distroless image's
`USER` is the **non-numeric** `nonroot`, which `runAsNonRoot: true` alone
cannot check. The shipped manifest pins `runAsUser: 65532` /
`runAsGroup: 65532`; if you copied the Deployment elsewhere, carry those
fields.

```
Error: container has runAsNonRoot and image has non-numeric user (nonroot),
cannot verify user is non-root
```

## Kubernetes: the webhook rejects a spec that has no `uri`

The inline spec is *supposed* to omit the URIs — the operator mounts the
Secrets and the coordinator resolves them at boot. The webhook validates
with `spec.WithoutCredentials()`, so it must accept that shape. If it
rejects an empty `uri`, you are running an operator older than that fix;
rebuild the image.

## Kubernetes: a worker crashloops with `required flag(s) ... not set`

A worker gets its catalog credentials from the `URUTAU_SINK_*` env the
operator mounts from the Secret — it cannot be passed as a flag, because
the operator does not know a Secret's value. If a worker demands
`--client-id`, you are running an older image. The current worker falls
back to the environment.

## Kubernetes: `no such host` for `host.minikube.internal`

Some minikube builds put `host.minikube.internal` in the node's
`/etc/hosts` but not in CoreDNS, so Pods cannot resolve it. Either patch
CoreDNS or use the gateway IP (`192.168.49.1`) directly — see
[Local end-to-end](../guides/deploy-kubernetes.md#local-end-to-end-with-minikube).

## Kubernetes: `minikube image load` reports a conflict

A running Pod still holds the tag, so the node will not replace it. Build
into the daemon instead, which retags regardless:

```sh
make k8s-load      # eval $(minikube docker-env) && docker build -t urutau:dev .
```

## A worker never connects and the coordinator times out

The coordinator waits `--wait-worker` for **exactly** the derived worker
names, then fails. Check:

- The worker's `--name` matches the derived name
  `<pipeline>-<target>-<index>`.
- The worker can reach `--coordinator` (address, firewall, Service DNS).
- Under mTLS, all three TLS flags are set on both sides.

The coordinator logs every `worker session` as it arrives; the missing one
is the problem.

## `coordinator: reset worker` keeps appearing

A worker is not acking within `--ack-timeout`, so the coordinator re-routes
its partition. Some resets are normal (a worker restart). A steady stream
means the worker is slow or dying — usually the sink, not the network.
After `--max-resets` within `--reset-window` the job terminates by design.
See [Operations](../guides/operations.md#supervision).

## `urutau_worker_commit_failures_total` is rising

The catalog is rejecting commits. Check catalog reachability, credentials,
and the warehouse's storage (S3/rustfs) health. Persisted failures are a
data-loss risk: commits are how the position advances.

## Data is not showing up, but the process is running

Work through it in order:

1. **Is the snapshot done?** Watch `urutau_worker_snapshot_progress_ratio`.
   Streaming starts after the snapshot.
2. **Is the position advancing?** `urutau_coordinator_lag_seconds` and
   `/statusz`.
3. **Is the commit landing?** `urutau_coordinator_commits_total` and
   `urutau_worker_commit_duration_seconds`.
4. **Read it back through a real engine.** A `SELECT` in Trino/ClickHouse
   is the only proof; the engine's own logs are not.

An `UPDATE`/`DELETE` takes a commit interval to appear — the worker batches
changes, it is not row-by-row instant. See
[Semantics](../reference/semantics.md).

## The pipeline re-snapshots on every restart

It should not: the position lives in the sink, and a restart resumes from
it. If it re-snapshots, the sink is not persisting the position — check
that the target table is the same (a changed `target` is a new table) and
that the sink is the one you think it is.

## Still stuck

Open an issue with: the spec (redact credentials), the exact error, and
the relevant logs (`--log-format json` helps). If a doc page did not work
as written, that is a doc bug — say so in the issue.

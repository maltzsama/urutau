---
sidebar_position: 4
---

# Operator: worker provisioning

The k8s operator (`cmd/operator`) reconciles one `CDCPipeline` custom
resource into a coordinator `StatefulSet`, its `ServiceAccount`/`Role`, a
headless `Service`, and a `ConfigMap`. It does **not** create worker
Deployments itself — that split mirrors Spark's driver/executor model:
the operator builds the *mold* (a Pod template per table), and the
coordinator clones it into live Deployments once it's actually running
and knows its own per-table partition names.

## Two different `Workers`, one resolution

There are two separate types named `Workers` in this codebase, at two
different layers, and they don't share all their fields:

**`api/v1alpha1.WorkerDefaults`** — the CRD's `spec.worker`, a
**pipeline-wide default**:

```go
type WorkerDefaults struct {
	CPU            string `json:"cpu,omitempty"`
	CPUOverhead    string `json:"cpu_overhead,omitempty"`
	Memory         string `json:"memory,omitempty"`
	MemoryOverhead string `json:"memory_overhead,omitempty"`
}
```

**`spec.WorkerSpec`** — the pipeline YAML's `table.workers`, a **per-table
override**:

```go
type WorkerSpec struct {
	Number int    `json:"number,omitempty"`
	CPU    string `json:"cpu,omitempty"`
	Memory string `json:"memory,omitempty"`
}
```

Neither has `number`+`cpu`+`memory`+`cpuOverhead`+`memoryOverhead` all at
once. The default carries the overhead knobs (a default is baseline
sizing policy, not a partition count); the per-table override carries the
partition count and can override CPU/Memory but never overhead — every
worker's overhead always comes from the pipeline-wide default,
regardless of whether its CPU/Memory came from the default or a
per-table override.

`Table.Workers == nil` or `Number <= 1` means today's single-worker
behavior: no partitioning, one group, byte-for-byte unchanged from before
this feature existed.

## Resolution: request vs. limit

For each table, the coordinator's worker Deployment resources resolve as:

```go
func workerResources(cr *CDCPipeline, t spec.Table) corev1.ResourceRequirements {
	cpu, memory := cr.Spec.Worker.CPU, cr.Spec.Worker.Memory
	if t.Workers != nil {
		if t.Workers.CPU != "" { cpu = t.Workers.CPU }
		if t.Workers.Memory != "" { memory = t.Workers.Memory }
	}
	// overhead always comes from the pipeline-wide default
	return resourceRequirements(cpu, cr.Spec.Worker.CPUOverhead, memory, cr.Spec.Worker.MemoryOverhead)
}
```

**CPU/Memory become the container's resource `request`. CPU+CPUOverhead
and Memory+MemoryOverhead become its `limit`.** This mirrors Spark's
`spark.executor.memoryOverhead`: headroom for the process's own
bookkeeping beyond its declared working set. If overhead is empty, limit
equals request — no separate headroom. If neither CPU nor Memory is set
anywhere, the container gets no `resources` block at all (BestEffort
QoS).

The coordinator's own CPU/Memory (`CoordinatorSpec.CPU/Memory`) have no
overhead knob — request equals limit always. Rationale: the coordinator
is a single long-lived process, not a pool sized like the workers.

## Example

```yaml
apiVersion: urutau.io/v1alpha1
kind: CDCPipeline
metadata:
  name: shop-mysql
spec:
  image: ghcr.io/you/urutau:v1.2.3
  secrets:
    source: shop-mysql-creds
    catalog: polaris-creds
  worker:
    cpu: "1"
    memory: "2Gi"
    cpu_overhead: "500m"
    memory_overhead: "512Mi"
  definition:
    inline:
      pipeline: shop-mysql
      source:
        kind: mysql
        uri: mysql://repl@mysql:3306/shop
        serverId: "1101"
      sink:
        uri: polaris://polaris:8181/api/catalog
        namespace: raw
      tables:
        - source: shop.orders
          target: raw.orders
          primaryKey: [id]
          workers:
            number: 3
            cpu: "2"
            memory: "4Gi"
          writeMode: append
```

`raw.orders` gets 3 workers, each requesting 2 CPU / 4Gi (table override)
with a limit of 2.5 CPU / 4.5Gi (pipeline-wide overhead). Any other table
in the same pipeline with no `workers:` block gets 1 worker at the
pipeline default: 1 CPU / 2Gi request, 1.5 CPU / 2.5Gi limit.

Worker group names are always derived, never operator-chosen:
`<pipeline>-<target>-<index>` — here, `shop-mysql-raw.orders-0`,
`shop-mysql-raw.orders-1`, `shop-mysql-raw.orders-2`. This is the single
source of truth both the coordinator's internal PK-range routing and its
Deployment provisioning use, so they can never disagree on what a
partition is called.

## Why range partitioning, not hashing

A table's PK range is split into contiguous ranges up front, not hashed
across workers. No canonical hash function exists that both MySQL and
Postgres sources could agree on (MySQL `CRC32` vs. Postgres `hashtext`
would disagree between the DBLog bootstrap snapshot and the live binlog/
WAL stream). Range boundaries are computed once and reused by both
phases, so a key can never switch partition ownership between snapshot
and stream.

## What "provisioned from pod templates" actually means

The operator does not write a literal Kubernetes `PodTemplate` object.
For each table, it builds a `corev1.PodTemplateSpec` Go value (image,
command, env — the same Secret-backed credentials as the coordinator's
own container, and the resolved `Resources` above), marshals it to YAML,
and stores it in the coordinator's `ConfigMap` under the key
`worker-pod-template.<target>.yaml`.

This only happens when an image resolves — `spec.image` on the CR, or
the operator's own `--coordinator-image` flag as a cluster-wide fallback.
A pipeline that never sets an image gets no worker pod templates and the
coordinator's provisioning path below is a no-op.

## Coordinator: cloning the mold into live Deployments

`internal/coordinator/workers_k8s.go` is where Deployments actually get
created — inside the **coordinator** binary, not the operator. On boot,
`provisionWorkers` first checks whether any worker pod template file
exists under `/etc/urutau` (the same mount as `pipeline.yaml`). If none
exist — every pipeline that never set `spec.image` — it returns
immediately: **zero Kubernetes API calls, zero in-cluster client
construction.** This is what keeps the feature fully backward compatible
with every pipeline that doesn't use it.

If templates exist: the coordinator builds an in-cluster client, resolves
its own Pod (via `os.Hostname()`, which k8s sets to the Pod name) to
attach an `OwnerReference` to every Deployment it creates — so when the
coordinator's Pod dies or its StatefulSet scales to 0, Kubernetes garbage
collection cascades and removes every worker Deployment with it. For each
derived worker group name, it loads that table's template, deep-copies
it, and stamps exactly two things the operator couldn't have known ahead
of time: the worker's own `--name <name>` argument, and the
OwnerReference. Everything else — image, command, env, resources — is
exactly what the operator already assembled.

## RBAC

Two separate roles are involved:

- **The operator's ClusterRole** (`config/rbac/operator.yaml`):
  cluster-wide CRUD on `cdcpipelines` (+ `status`/`finalizers`), full CRUD
  on `statefulsets`, read on `secrets`, CRUD on `configmaps`/`services`,
  and CRUD on `serviceaccounts`/`roles`/`rolebindings` (it mints a
  per-pipeline identity dynamically).
- **Each coordinator Pod's own per-pipeline `Role`+`RoleBinding`**
  (created by the operator, scoped by `resourceNames` to that one CR):
  additionally grants `apps/deployments` get/create/update and
  `core/pods` get — this is what lets the coordinator provision worker
  Deployments.

Two details are easy to get wrong when writing this RBAC by hand:

- **`list`/`watch`, not just `get`/`create`.** The controller-runtime
  manager caches every type it creates, so a role that grants only
  `get`/`create` on `serviceaccounts`/`roles`/`rolebindings` never syncs
  its informers and the reconciler stalls.
- **You cannot grant what you do not hold.** RBAC forbids a subject from
  creating a Role that carries permissions it lacks, so the operator's
  ClusterRole must itself hold `apps/deployments` get/create/update and
  `core/pods` get — even though the operator never touches a Deployment or
  a Pod. The coordinator does, through the per-pipeline Role.

## Concurrent writers: two different fixes for the same problem

Naively, N workers committing to one table race on the position: the last
committer silently wins and can clobber a lagging partition's watermark.
Both built-in sinks solve this (`sink.Sink.SupportsConcurrentWriters()`
returns true for both), but with different mechanisms:

- **Iceberg**: workers never commit directly.
  `TableWriter.WriteStaged` writes each batch's data files without
  committing them, returning an opaque descriptor. The coordinator
  collects one binlog cycle's descriptors across all workers and commits
  them as a single unit via `Sink.CommitStaged` — one committer, one
  position write, no race by construction.
- **ClickHouse**: workers commit independently (no staging), but the
  `ReplacingMergeTree` version column is the coordinator's own shared
  sequence, and the durable position is kept **per partition** in a side
  table and read back as the **minimum safe** value across partitions
  (see [Sinks](../reference/sinks#clickhouse)) — so a resume never jumps
  past a lagging partition even though workers wrote independently.

A sink that returns `false` from `SupportsConcurrentWriters` rejects
`workers: {number: N > 1}` at boot, not silently — the coordinator fails
fast with an error naming the table and pointing at the fix (`workers: 1`,
or for Couchbase, `sink.commitMode: atomic`).

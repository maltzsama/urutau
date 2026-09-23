---
sidebar_position: 6
---

# Deploy on Kubernetes

Run Urutau as a Kubernetes-native CDC service: submit a `CDCPipeline`
custom resource, and an operator turns it into a running coordinator plus
its workers. This is the same coordinator/worker engine described in
[Distributed mode](distributed.md) — Kubernetes just automates the
lifecycle.

If you only want to move one table on your laptop, the
[Quickstart](../quickstart.md) is shorter. Come here when you want the
cluster to own scheduling, restarts, and scaling.

## What the operator builds

You submit **one** object — a `CDCPipeline`. The operator reconciles it
into everything else:

```text title="Operator reconciliation tree"
CDCPipeline (you)
  └─ operator ──────────────► ServiceAccount + Role + RoleBinding   (per-pipeline identity)
                              Service (headless, :50051)            (stable coordinator address)
                              ConfigMap (pipeline.yaml + worker templates)
                              StatefulSet (the coordinator)
                                   └─ coordinator ─► worker Deployment × N   (one per table partition)
```

The split matters: the **operator** never creates worker Deployments. It
renders a Pod template per table into the coordinator's ConfigMap; the
**coordinator** clones that template once it is running and knows its own
partition names. This mirrors Spark's driver/executor model. The
[Operator](../architecture/operator.md) page has the full mechanics.

## 1. Prerequisites

- A Kubernetes cluster (v1.28+) and `kubectl`.
- **cert-manager** — the validating webhook needs a TLS certificate, and
  the manifests obtain it from cert-manager. Install it once per cluster:

```sh title="Install cert-manager"
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.21.2/cert-manager.yaml
kubectl -n cert-manager rollout status deploy/cert-manager deploy/cert-manager-cainjector deploy/cert-manager-webhook
```

- An image containing the Urutau binaries. The repo ships one Dockerfile
  that builds **all four** binaries into a single image
  (`build/Dockerfile`); the coordinator StatefulSet and every worker
  Deployment run that same image, only the container `command` differs.

### Building and publishing the image

For a real cluster, build and push:

```sh title="Build and push image"
make docker OPERATOR_IMAGE=ghcr.io/you/urutau:v1.2.3
docker push ghcr.io/you/urutau:v1.2.3
```

Then point the deployment at it:

```sh title="Deploy with kustomize"
cd config/default
kustomize edit set image urutau=ghcr.io/you/urutau:v1.2.3
cd ../..
kubectl apply -k config/default
```

`kustomize edit set image` is one edit: the kustomization's `replacements`
rule copies the operator's own image into its `--coordinator-image` flag,
so the coordinator and workers follow automatically.

For **minikube**, build straight into the cluster's Docker daemon — no
registry, and it retags even while a Pod still holds the old image:

```sh title="Build for minikube"
make k8s-load          # eval $(minikube docker-env) && docker build -t urutau:dev .
```

## 2. Install the operator

```sh command="make k8s-deploy"
make k8s-deploy        # kubectl apply -k config/default
```

This creates the namespace `urutau-system`, the `CDCPipeline` CRD, the
operator's RBAC, the operator Deployment, and the webhook
`Service`/`Certificate`/`ValidatingWebhookConfiguration`. Check it:

```sh title="Check operator status"
kubectl -n urutau-system get deploy,pod
# deployment.apps/urutau-operator   1/1   Running
```

The operator logs should show the webhook registering at
`/validate-urutau-io-v1alpha1-cdcpipeline` and the controller starting.

### Helm install (alternative)

A Helm chart ships at `charts/urutau-operator/` as a second, parallel install
path — it is not a replacement for the kustomize base, and neither is
deprecated. It drives the namespace, image, replicas, resources and the
webhook's cert-manager dependency from values:

```sh title="Helm install"
helm install urutau charts/urutau-operator \
  --namespace urutau-system --create-namespace \
  --wait
```

The namespace comes from `--namespace` in all three places the kustomize base
hardcodes `urutau-system` (the `Certificate` dnsNames, the `inject-ca-from`
annotation, and the webhook `clientConfig.service.namespace`), so installing
into a different namespace needs no edits. cert-manager is a hard prerequisite
unless you disable the webhook (`webhook.enabled=false`) or supply the
certificate and caBundle yourself (`webhook.certManager.enabled=false` with
`webhook.caBundle`). See the chart's
[README](https://github.com/maltzsama/urutau/tree/main/charts/urutau-operator)
for the values.

## 3. Provide credentials as Secrets

Credentials are **never** inline in the CR. The operator mounts Secrets
into the coordinator and worker Pods as environment variables, and the
engine resolves them at boot. The key convention is fixed:

| Secret (`spec.secrets.source`) | Env it fills | Becomes |
| --- | --- | --- |
| `uri` | `URUTAU_SOURCE_URI` | `source.uri` |

| Secret (`spec.secrets.catalog`) | Env it fills | Becomes |
| --- | --- | --- |
| `uri` | `URUTAU_SINK_URI` | `sink.uri` |
| `clientId` | `URUTAU_SINK_CLIENT_ID` | `sink.clientId` |
| `clientSecret` | `URUTAU_SINK_CLIENT_SECRET` | `sink.clientSecret` |
| `scope` | `URUTAU_SINK_SCOPE` | `sink.scope` |

```sh title="Create secrets"
kubectl create secret generic shop-mysql-creds \
  --from-literal=uri='mysql://repl:replpass@mysql.default.svc:3306/shop'

kubectl create secret generic polaris-creds \
  --from-literal=uri='http://polaris.default.svc:8181/api/catalog' \
  --from-literal=clientId=root \
  --from-literal=clientSecret=s3cr3t \
  --from-literal=scope=PRINCIPAL_ROLE:ALL
```

Any field you *do* set inline in `spec.definition.inline` wins over the
environment — the Secret only fills what you leave empty. That is why the
webhook (which cannot read your Secrets) validates the inline spec
*without* requiring the URIs: see [The webhook](#the-webhook) below.

## 4. Submit a pipeline

`config/samples/cdcpipeline.yaml` is a complete, working example. The
essential shape:

```yaml title="CDCPipeline CR"
apiVersion: urutau.io/v1alpha1
kind: CDCPipeline
metadata:
  name: shop-mysql
  namespace: default
spec:
  image: urutau:dev                 # coordinator + every worker it provisions
  secrets:
    source: shop-mysql-creds
    catalog: polaris-creds
  coordinator:
    cpu: "1"
    memory: "1Gi"
    metricsAddr: ":8080"
    snapshot:
      chunkSize: 10000
      maxParallelChunks: 4
    supervision:
      ackTimeout: 30s
      maxResets: 5
      window: 15m
  worker:
    cpu: "500m"
    cpu_overhead: "100m"
    memory: "1Gi"
    memory_overhead: "256Mi"
  definition:
    inline:                          # the same YAML `urutau run -f` accepts
      pipeline: shop-mysql
      source:
        kind: mysql
        serverId: "1101"
      sink:
        type: iceberg+rest
        namespace: raw
        warehouse: quickstart_catalog
      tables:
        - source: shop.orders
          target: raw.orders
          primaryKey: [id]
          createIfNotExists: true
          workers:
            number: 3
```

Note what is **absent** from `definition.inline`: no `source.uri`, no
`sink.uri`, no catalog credentials. The Secrets fill those. Everything
else — tables, `serverId`, namespace, warehouse, worker counts — lives in
the spec and travels with the CR.

### SSH-tunneled Postgres sources

When the database is reachable only through a bastion, the worker Pods need
the SSH private key on disk (a DSN cannot carry an SSH tunnel). Set
`secrets.ssh` to a Secret whose `privateKey` entry holds the key; the
operator mounts it read-only at `/etc/urutau/ssh/privateKey` in every worker
Pod, and the inline spec's `source.postgres.ssh.privateKey` names that path:

```yaml title="SSH-tunneled Postgres source"
spec:
  secrets:
    source: shop-postgres-creds
    catalog: polaris-creds
    ssh: shop-postgres-ssh          # Secret with key: privateKey
  definition:
    inline:
      source:
        kind: postgres
        slotName: shop_slot
        postgres:
          host: db.internal
          database: shop
          ssh:
            host: bastion.example.com
            username: tunnel
            privateKey: /etc/urutau/ssh/privateKey
            knownHosts: /etc/urutau/ssh/known_hosts
```

```sh title="Create SSH secret"
kubectl create secret generic shop-postgres-ssh \
  --from-file=privateKey=~/.ssh/id_ed25519 \
  --from-file=known_hosts=~/.ssh/known_hosts
```

The Secret holds two keys — `privateKey` and `known_hosts` — mounted at
`/etc/urutau/ssh/privateKey` and `/etc/urutau/ssh/known_hosts`; the inline
spec's `ssh.privateKey` and `ssh.knownHosts` must name those paths. The same
Secret is mounted into the **coordinator** (which opens the replication
connection) and into the worker Pods. When a scoped `source.snapshotUri` is
set, the workers connect directly and the key is mounted only into the
coordinator.

```sh command="kubectl apply -f config/samples/cdcpipeline.yaml"
kubectl apply -f config/samples/cdcpipeline.yaml
```

### Autoscaling workers with KEDA

A table's worker count is fixed at `workers.number` by default. Set
`workers.max` to let it scale at runtime, and point the operator at
Prometheus so it renders one KEDA `ScaledObject` per such table — set the
operator's `KEDA_PROMETHEUS_ADDRESS` env (in `config/manager/operator.yaml`,
or the equivalent in your install):

```yaml title="Enable autoscaling in the operator"
- name: KEDA_PROMETHEUS_ADDRESS
  value: "http://prometheus.monitoring.svc:9090"
```

```yaml title="A table that may scale out"
tables:
  - source: shop.orders
    target: raw.orders
    primaryKey: [id]
    workers:
      number: 1      # minReplicaCount
      max: 8         # maxReplicaCount — also enables the ScaledObject
```

Each table's workers run as one StatefulSet named `<pipeline>-<target>` (its
pod ordinals are the derived worker names), and the `ScaledObject` drives its
`spec.replicas` from `urutau_coordinator_pending_batches{table="<target>"}` —
the table's outstanding batches — with `--keda-threshold` (default `30`) as
the per-replica backlog target. The operator never scales the table itself:
the coordinator follows `spec.replicas` and re-slices to match, so KEDA (or a
manual `kubectl scale`) is the only writer.

The Helm chart does not expose `--keda-prometheus-address` yet; use the
kustomize install for autoscaling. See
[Distributed mode → Autoscaling with KEDA](distributed.md#autoscaling-with-keda)
for the metric choice and the current barrier limit under a large backlog.

## 5. Watch it come up

```sh title="Watch pods come up"
kubectl get cdcpipelines -A
kubectl -n default get pod
# shop-mysql-coordinator-0                 1/1   Running
# shop-mysql-raw.orders-0-...              1/1   Running
# shop-mysql-raw.orders-1-...              1/1   Running
# shop-mysql-raw.orders-2-...              1/1   Running
```

The coordinator logs should show the snapshot, the switch to streaming,
and `worker session` lines for each worker. The workers log
`phase=WORKER_PHASE_STREAMING`.

## 6. Prove the data landed

Read it back through a real query engine — do not trust the write. With
the repo's e2e stack (see [Local end-to-end](#local-end-to-end-with-minikube)):

```sh title="Query via Trino"
docker compose -f test/e2e/docker-compose.yml exec -T trino \
  trino --execute "SELECT * FROM iceberg.raw.orders ORDER BY id"
```

You should see the rows that exist in the source MySQL table, upserted by
`id`.

## The webhook

The operator runs a **validating** admission webhook. Every `CDCPipeline`
`CREATE`/`UPDATE` is checked with the *same* server-side rules the
coordinator runs at boot (`spec.Validate` plus the driver registry), so a
bad spec is rejected at `kubectl apply` time instead of surfacing as a
CrashLoopBackOff:

```sh title="Webhook rejection example"
$ kubectl apply -f bad.yaml
Error from server (Forbidden): admission webhook "vcdcpipeline.urutau.io" denied the request:
spec.definition.inline: driver: unknown source kind "bogus" (registered: [kafka mysql postgres])
```

Because the webhook cannot read the Secrets the coordinator will mount, it
validates the inline spec with `spec.WithoutCredentials()` — it checks
everything **except** the URI/credential fields, which are empty by design
on the Kubernetes path. The coordinator still enforces them on the
resolved spec at boot. So a spec that passes admission can still fail at
runtime if a Secret is missing or malformed — admission validates *shape*,
not connectivity.

To turn the webhook off (e.g. cert-manager unavailable), run the operator
with `--enable-webhook=false`; the reconciler still validates, so you lose
the admission-time check, not correctness.

## Control-plane security

The coordinator↔worker channel carries the **source DSN** in the worker
assignment. The coordinator binary refuses to boot without mTLS unless
`--allow-insecure-control-plane` is passed explicitly. The operator does not
wire control-plane mTLS yet, so it passes that opt-out: the DSN travels
plaintext **inside the cluster**. That is acceptable when the network and
nodes are trusted; wire mTLS (a cert-manager `Certificate` for the
coordinator Service plus worker client certs) before running the control
plane across an untrusted network. See
[Distributed mode](distributed.md#secure-the-control-plane).

## Multi-tenant install (namespace per team)

The default install (`config/default`) binds the operator's `ClusterRole`
**cluster-wide**. That is fine when the cluster is yours: the operator only
ever acts on the namespaces that hold `CDCPipeline`s. On a shared cluster it
is a real blast radius — the binding grants `pods/delete` and
`deployments`/`statefulsets` writes in *every* namespace, so a compromised
operator is not confined to its tenants.

For a shared cluster, install the multi-tenant overlay instead. It keeps the
same `ClusterRole` (the permission *set* is unchanged) but drops the
cluster-wide binding and binds the operator into each managed namespace with
a `RoleBinding` — scoping the permissions to where you actually run
pipelines:

```sh command="kubectl apply -k config/multi-tenant"
kubectl apply -k config/multi-tenant
```

The convention is **one namespace per team**. `config/multi-tenant/tenant-example.yaml`
is a complete onboarding for one namespace; copy it per team and:

1. Give the namespace a `RoleBinding` to the operator's `ClusterRole` (the
   operator's ServiceAccount lives in `urutau-system`):

```yaml title="RoleBinding for tenant"
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: urutau-operator
  namespace: team-a
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: urutau-operator-role
subjects:
  - kind: ServiceAccount
    name: urutau-operator
    namespace: urutau-system
```

2. Add that namespace to the operator's `--watch-namespaces` (the overlay
   patches this arg). The two must stay in sync: the operator's cache never
   lists a namespace it has no `RoleBinding` in, and a `RoleBinding` in a
   namespace it does not watch is unused.

The operator then reconciles `CDCPipeline`s only in the namespaces you list —
a `CDCPipeline` created elsewhere is simply invisible to it, not an error.

### Bounding a tenant's footprint

The overlay also ships a `ResourceQuota` and `LimitRange` per namespace, so a
team cannot exhaust the cluster. Size the quota against what a `CDCPipeline`
will actually request:

- the **coordinator** uses `coordinator.cpu` / `coordinator.memory`;
- each **worker** Deployment uses `worker.cpu` + `worker.cpu_overhead` and
  `worker.memory` + `worker.memory_overhead` — or the table's own
  `workers.cpu` / `workers.memory` when set, which overrides the worker
  default;
- a pipeline's footprint is the coordinator **plus the sum over its tables of
  `workers.number` × the per-worker request**.

So a pipeline with three workers at `500m`/`1Gi` (request) and a `1`/`1Gi`
coordinator needs roughly `2.5` CPU and `4Gi` of requests. The
`count/cdcpipelines.urutau.io` entry caps how many pipelines the namespace
may submit.

## Local end-to-end with minikube

The repo's `test/e2e/docker-compose.yml` gives you MySQL and a Polaris
catalog on the **host**. From inside a minikube pod they are reachable at
`host.minikube.internal` (the host gateway, `192.168.49.1`). Two gotchas:

1. **CoreDNS may not know that name.** Some minikube builds put
   `host.minikube.internal` in the node's `/etc/hosts` but not in the
   cluster DNS, so pods fail with `no such host`. Check, and patch if
   needed:

```sh title="CoreDNS patch"
kubectl -n kube-system get cm coredns -o jsonpath='{.data.Corefile}' | grep hosts
# if empty:
kubectl -n kube-system get cm coredns -o json | jq \
  '.data.Corefile |= (split("\n") | (.[0:1] + ["    hosts {","        192.168.49.1 host.minikube.internal","        fallthrough","    }"] + .[1:]) | join("\n"))' \
  | kubectl apply -f -
kubectl -n kube-system rollout restart deploy/coredns
```

   Or just use `192.168.49.1` directly in the Secret URIs.

2. **The source must be reachable on the host's published ports.** The
   compose file publishes MySQL on `3306` and Polaris on `8181`; use
   `root`/`rootpass` for MySQL (the e2e seed user) and `root`/`s3cr3t`
   for the catalog.

Full loop:

```sh title="Full minikube loop"
# 1. host services
docker compose -f test/e2e/docker-compose.yml up -d --wait mysql polaris trino rustfs bucket-init polaris-setup

# 2. image into minikube + operator
make k8s-load
make k8s-deploy

# 3. secrets + pipeline (the sample already points at host.minikube.internal)
kubectl create secret generic shop-mysql-creds \
  --from-literal=uri='mysql://root:rootpass@host.minikube.internal:3306/shop'
kubectl create secret generic polaris-creds \
  --from-literal=uri='http://host.minikube.internal:8181/api/catalog' \
  --from-literal=clientId=root --from-literal=clientSecret=s3cr3t \
  --from-literal=scope=PRINCIPAL_ROLE:ALL
kubectl apply -f config/samples/cdcpipeline.yaml

# 4. verify
kubectl -n default get pod
docker compose -f test/e2e/docker-compose.yml exec -T trino \
  trino --execute "SELECT count(*) FROM iceberg.raw.orders"
```

### Fully in-cluster (no host docker)

Running the whole stack inside the cluster avoids a host docker daemon
competing for memory, and — unlike the host-gateway setup — lets Polaris
vend an **in-cluster** S3 endpoint the pods reach directly. `test/e2e`
carries the same data services as the compose file (MySQL, RustFS, Polaris,
Trino) as kustomize manifests:

```sh title="In-cluster e2e stack"
kubectl apply -k test/e2e     # ns e2e: mysql, rustfs, polaris, trino + bootstrap jobs
kubectl -n e2e wait --for=condition=complete job/bucket-init job/polaris-setup --timeout=300s

# then deploy the operator and a CR whose URIs point at the in-cluster services:
#   source:  mysql://root:rootpass@mysql.e2e.svc.cluster.local:3306/shop
#   catalog: http://polaris.e2e.svc.cluster.local:8181/api/catalog
```

The `quickstart_catalog` is created with the rustfs Service FQDN as its S3
endpoint, so the coordinator and workers reach the warehouse with no host
gateway involved. Seed `shop.orders` before the coordinator boots — a
partitioned table splits its primary-key range from the rows that exist.

## Updating a pipeline

Edit the CR and re-apply. The operator stamps a hash of the spec onto the
coordinator's Pod template, so a spec change rolls the StatefulSet, which
re-reads the ConfigMap and re-provisions workers.

```sh title="Update pipeline"
kubectl edit cdcpipelines shop-mysql
```

A worker-template change from a **new operator build** is different: the
coordinator reads the template once at boot, so restart the coordinator
Pod to pick it up.

## Teardown

```sh title="Teardown"
kubectl delete cdcpipelines --all -A     # stops pipelines, GCs their workers
make k8s-undeploy                        # removes the operator + CRD
```

## Next steps

- **How the pieces fit, and why**: [Operator](../architecture/operator.md).
- **The same engine without Kubernetes**: [Distributed mode](distributed.md).
- **Running it for real** (metrics, audit log, checkpoints):
  [Monitoring](monitoring.md) and [Reliability](reliability.md).
- **Every field**: [CLI reference](../reference/cli.md) and the
  [CRD type](../architecture/operator.md).

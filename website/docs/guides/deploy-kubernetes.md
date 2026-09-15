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

```
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

  ```sh
  kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.21.2/cert-manager.yaml
  kubectl -n cert-manager rollout status deploy/cert-manager deploy/cert-manager-cainjector deploy/cert-manager-webhook
  ```

- An image containing the Urutau binaries. The repo ships one Dockerfile
  that builds **all four** binaries into a single image
  (`build/Dockerfile`); the coordinator StatefulSet and every worker
  Deployment run that same image, only the container `command` differs.

### Building and publishing the image

For a real cluster, build and push:

```sh
make docker OPERATOR_IMAGE=ghcr.io/you/urutau:v1.2.3
docker push ghcr.io/you/urutau:v1.2.3
```

Then point the deployment at it:

```sh
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

```sh
make k8s-load          # eval $(minikube docker-env) && docker build -t urutau:dev .
```

## 2. Install the operator

```sh
make k8s-deploy        # kubectl apply -k config/default
```

This creates the namespace `urutau-system`, the `CDCPipeline` CRD, the
operator's RBAC, the operator Deployment, and the webhook
`Service`/`Certificate`/`ValidatingWebhookConfiguration`. Check it:

```sh
kubectl -n urutau-system get deploy,pod
# deployment.apps/urutau-operator   1/1   Running
```

The operator logs should show the webhook registering at
`/validate-urutau-io-v1alpha1-cdcpipeline` and the controller starting.

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

```sh
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

```yaml
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

```sh
kubectl apply -f config/samples/cdcpipeline.yaml
```

## 5. Watch it come up

```sh
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

```sh
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

```sh
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

## Local end-to-end with minikube

The repo's `test/e2e/docker-compose.yml` gives you MySQL and a Polaris
catalog on the **host**. From inside a minikube pod they are reachable at
`host.minikube.internal` (the host gateway, `192.168.49.1`). Two gotchas:

1. **CoreDNS may not know that name.** Some minikube builds put
   `host.minikube.internal` in the node's `/etc/hosts` but not in the
   cluster DNS, so pods fail with `no such host`. Check, and patch if
   needed:

   ```sh
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

```sh
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

## Updating a pipeline

Edit the CR and re-apply. The operator stamps a hash of the spec onto the
coordinator's Pod template, so a spec change rolls the StatefulSet, which
re-reads the ConfigMap and re-provisions workers.

```sh
kubectl edit cdcpipelines shop-mysql
```

A worker-template change from a **new operator build** is different: the
coordinator reads the template once at boot, so restart the coordinator
Pod to pick it up.

## Teardown

```sh
kubectl delete cdcpipelines --all -A     # stops pipelines, GCs their workers
make k8s-undeploy                        # removes the operator + CRD
```

## Next steps

- **How the pieces fit, and why**: [Operator](../architecture/operator.md).
- **The same engine without Kubernetes**: [Distributed mode](distributed.md).
- **Running it for real** (metrics, audit log, checkpoints):
  [Operations](operations.md).
- **Every field**: [CLI reference](../reference/cli.md) and the
  [CRD type](../architecture/operator.md).

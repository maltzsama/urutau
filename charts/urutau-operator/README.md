# urutau-operator

The Urutau operator: it reconciles `CDCPipeline` custom resources into
per-pipeline coordinator StatefulSets.

This chart is a **second, parallel install path** to the kustomize base in the
repository's `config/` (`make k8s-deploy`). Neither replaces the other; they
ship the same resources.

## Prerequisites

- Kubernetes v1.28+.
- **cert-manager**, if `webhook.enabled` and `webhook.certManager.enabled`
  (the defaults): it issues the webhook serving certificate. Install it once
  per cluster. Without it, the `Issuer`/`Certificate` are unknown kinds and
  the install fails — install with `helm install --wait` so a partial install
  does not look successful.
- An image carrying the Urutau binaries. The chart defaults to
  `ghcr.io/maltzsama/urutau`, tagged with the chart's `appVersion`.

## Install

```sh
helm install urutau charts/urutau-operator \
  --namespace urutau-system --create-namespace \
  --wait
```

The namespace is driven by `--namespace` (i.e. `{{ .Release.Namespace }}`) in
every place the kustomize base hardcodes `urutau-system` — the webhook
`Certificate` dnsNames, the `inject-ca-from` annotation, and the webhook
`clientConfig.service.namespace`.

## Values

| Key | Default | Meaning |
| --- | --- | --- |
| `nameOverride` | `urutau-operator` | Resource name base |
| `fullnameOverride` | _(empty)_ | Overrides every cluster-scoped resource name except the CRD (fixed at `cdcpipelines.urutau.io`); a second release also needs `crds.install=false` |
| `crds.install` | `true` | Whether this release owns the CDCPipeline CRD; set `false` for a second release in the same cluster |
| `rbac.clusterWide` | `true` | Bind the operator's ClusterRole cluster-wide; `false` binds it into each `operator.watchNamespaces` namespace |
| `image.repository` | `ghcr.io/maltzsama/urutau` | Image for the operator (and `--coordinator-image`) |
| `image.tag` | chart `appVersion` | Image tag |
| `image.pullPolicy` | `IfNotPresent` | Image pull policy |
| `imagePullSecrets` | `[]` | Pull secrets for the image (private mirrors) |
| `operator.replicaCount` | `2` | Operator replicas (leader election makes >1 safe) |
| `operator.fieldManager` | `urutau-operator` | Server-Side Apply field manager |
| `operator.watchNamespaces` | _(all)_ | Comma-separated namespaces to watch — scopes the CACHE; pair with `rbac.clusterWide=false` for the permissions boundary |
| `operator.keda.prometheusAddress` | `""` | Prometheus address for KEDA ScaledObjects; empty disables worker autoscaling |
| `operator.keda.threshold` | `""` | Per-replica backlog target for KEDA; empty uses the operator default (30) |
| `operator.resources` | `100m/128Mi` → `500m/256Mi` | Operator container resources |
| `podAnnotations` | `{}` | Extra annotations on the operator Pod |
| `nodeSelector` / `tolerations` / `affinity` / `topologySpreadConstraints` | `{}`/`[]` | Pod placement |
| `priorityClassName` | `""` | Pod priority class |
| `podDisruptionBudget.enabled` | `false` | Render a PDB (`minAvailable: 1`) |
| `serviceMonitor.enabled` | `false` | Render a metrics Service + ServiceMonitor |
| `serviceMonitor.interval` | `30s` | Scrape interval |
| `webhook.enabled` | `true` | Run the admission webhook |
| `webhook.certManager.enabled` | `true` | cert-manager issues the webhook cert |
| `webhook.caBundle` | `""` | Base64 PEM for the webhook's caBundle; required when `webhook.certManager.enabled=false` |

## Worker autoscaling (KEDA)

Set `operator.keda.prometheusAddress` to turn on KEDA worker autoscaling
(issue #298). The operator then renders one KEDA `ScaledObject` per table that
sets `spec.workers.max`, driven by the coordinator's per-table backlog metric.

Prerequisite: **KEDA** installed in the cluster, and a **Prometheus** that
scrapes the coordinator Pods' `/metrics` — KEDA's prometheus scaler queries
Prometheus, not the Pods directly.

```sh
helm install urutau charts/urutau-operator \
  --namespace urutau-system --create-namespace \
  --set operator.keda.prometheusAddress=http://prometheus.monitoring.svc:9090
```

`operator.keda.threshold` sets the per-replica backlog target; leave it empty
to use the operator default (30). An empty `prometheusAddress` keeps the
feature off — no `ScaledObject` is rendered.

## Multiple releases in one cluster

`fullnameOverride` renames every cluster-scoped resource the chart creates
**except the CRD**: its name (`cdcpipelines.urutau.io`) is the API identity
and cannot be renamed. So a second release must also set `crds.install=false`
and share the first release's CRD:

```sh
# second release, sharing the CRD and the cluster-wide RBAC of the first
helm install urutau-b charts/urutau-operator \
  --namespace urutau-b-system --create-namespace \
  --set fullnameOverride=urutau-operator-b \
  --set crds.install=false
```

Only the release that owns the CRD (the one that created it) should leave
`crds.install=true`.

## Namespace-scoped RBAC

`operator.watchNamespaces` scopes the operator's **cache** only; by default
the operator is still bound cluster-wide. To make it a real permissions
boundary — the repo's `config/multi-tenant` model — set
`rbac.clusterWide=false`; the chart then binds the operator's ClusterRole into
each watched namespace with a `RoleBinding` and drops the
`ClusterRoleBinding`:

```sh
helm install urutau charts/urutau-operator \
  --namespace urutau-system --create-namespace \
  --set rbac.clusterWide=false \
  --set 'operator.watchNamespaces=team-a\,team-b'
```

`rbac.clusterWide=false` with an empty `operator.watchNamespaces` fails the
render: the operator would have no permissions anywhere. A namespace repeated
in the list is bound once.

## The CDCPipeline CRD survives `helm uninstall`

The CRD carries `helm.sh/resource-policy: keep`, so `helm uninstall` leaves it
— and every `CDCPipeline` in the cluster — in place. Deleting the CRD cascades
to all pipelines across all namespaces, including ones this chart never
created, so it is deliberately a manual step:

```sh
kubectl delete crd cdcpipelines.urutau.io
```

## Install without cert-manager

If cert-manager is not available, the simplest path is to disable the webhook
entirely:

```sh
helm install urutau charts/urutau-operator \
  --namespace urutau-system --create-namespace \
  --set webhook.enabled=false
```

The reconciler still validates every spec, so this loses only the
admission-time check, not correctness.

### Keep the webhook, manage the certificate yourself

Set `webhook.certManager.enabled=false` and supply both halves of the trust
chain, or the install fails fast (the webhook declares `failurePolicy: Fail`,
so a missing caBundle would reject every `CDCPipeline` write):

1. **The caBundle** — `--set webhook.caBundle=$(base64 -w0 ca.crt)`.
2. **The serving Secret** — pre-create `urutau-operator-webhook-cert` (keys
   `tls.crt`/`tls.key`) in the release namespace. The operator Pod mounts it,
   so it must exist before the Pod starts.

```sh
helm install urutau charts/urutau-operator \
  --namespace urutau-system --create-namespace \
  --set webhook.certManager.enabled=false \
  --set webhook.caBundle="$(base64 -w0 ca.crt)"
```

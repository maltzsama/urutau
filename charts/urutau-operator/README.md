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
| `image.repository` | `ghcr.io/maltzsama/urutau` | Image for the operator (and `--coordinator-image`) |
| `image.tag` | chart `appVersion` | Image tag |
| `image.pullPolicy` | `IfNotPresent` | Image pull policy |
| `operator.replicaCount` | `2` | Operator replicas (leader election makes >1 safe) |
| `operator.fieldManager` | `urutau-operator` | Server-Side Apply field manager |
| `operator.watchNamespaces` | _(all)_ | Comma-separated namespaces to watch; each needs a RoleBinding |
| `operator.resources` | `100m/128Mi` → `500m/256Mi` | Operator container resources |
| `webhook.enabled` | `true` | Run the admission webhook |
| `webhook.certManager.enabled` | `true` | cert-manager issues the webhook cert |

## Install without cert-manager

If cert-manager is not available, disable the webhook entirely:

```sh
helm install urutau charts/urutau-operator \
  --namespace urutau-system --create-namespace \
  --set webhook.enabled=false
```

The reconciler still validates every spec, so this loses only the
admission-time check, not correctness.

To keep the webhook but manage the certificate yourself, set
`webhook.certManager.enabled=false` and provide the
`urutau-operator-webhook-cert` Secret (keys `tls.crt`/`tls.key`) plus the
`ValidatingWebhookConfiguration`'s `caBundle`.

#!/usr/bin/env bash
# Bring up the pod e2e environment — the engine's real deployment topology:
# cert-manager (the operator webhook needs it), the in-cluster data services
# with a FRESH Polaris catalog whose S3 endpoint is in-cluster, and the
# operator built from the race-instrumented image so the coordinator and every
# worker Pod it provisions run under the race detector.
#
#   make e2e-pods-up
#
# It is destructive on purpose: the `e2e` namespace is recreated so the
# catalog is never left pointing at a host-only endpoint (as the in-process
# e2e left it). The data services are stateless (emptyDir / in-memory
# Polaris), so recreating them is safe and cheap.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT"

KUBECTL="${KUBECTL:-kubectl}"
CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.16.2}"
RACE_IMAGE="${RACE_IMAGE:-urutau:dev-race}"

say() { printf '\n==> %s\n' "$*"; }

say "race-instrumented image ($RACE_IMAGE)"
# The image is built and loaded by `make k8s-load-race` (host build + minikube
# image load) — loading ~1.7GB on every bring-up would be pointless. Require
# it to be present, so a missing image is a clear message, not a Pod that
# sits in ErrImagePull.
if ! minikube image ls 2>/dev/null | grep -q "$RACE_IMAGE"; then
  echo "error: image $RACE_IMAGE is not loaded into minikube" >&2
  echo "       run: make k8s-load-race" >&2
  exit 1
fi

say "cert-manager ($CERT_MANAGER_VERSION)"
if ! "$KUBECTL" get namespace cert-manager >/dev/null 2>&1; then
  "$KUBECTL" apply -f "https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml"
fi
"$KUBECTL" -n cert-manager wait --for=condition=Available --timeout=300s deployment --all

say "fresh in-cluster data services (namespace e2e)"
"$KUBECTL" delete -k test/e2e --ignore-not-found --wait=true
# The namespace delete is asynchronous; apply would race a Terminating ns.
"$KUBECTL" wait --for=delete namespace/e2e --timeout=180s 2>/dev/null || true
"$KUBECTL" apply -k test/e2e
"$KUBECTL" -n e2e wait --for=condition=complete job/bucket-init --timeout=300s
"$KUBECTL" -n e2e wait --for=condition=complete job/polaris-setup --timeout=300s
"$KUBECTL" -n e2e wait --for=condition=Available --timeout=300s deployment --all

say "operator ($RACE_IMAGE)"
"$KUBECTL" apply -k test/e2e/pods/k8s/operator
"$KUBECTL" -n urutau-system rollout status deployment/urutau-operator --timeout=300s

say "pod e2e environment ready"

#!/usr/bin/env bash
# Tear down the pod e2e environment: every CDCPipeline (and the Pods it owns),
# the operator, and the in-cluster data services. cert-manager is left in
# place — it is a cluster add-on, slow to reinstall, and harmless to keep.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT"

KUBECTL="${KUBECTL:-kubectl}"

# Under `set -e`, deleting a CRD-backed resource that does not exist fails the
# whole teardown; skip it when the CRD was never installed.
if "$KUBECTL" get crd cdcpipelines.urutau.io >/dev/null 2>&1; then
  "$KUBECTL" delete cdcpipelines --all -A --ignore-not-found --wait=true
fi
"$KUBECTL" delete -k test/e2e/pods/k8s/operator --ignore-not-found
"$KUBECTL" delete -k test/e2e --ignore-not-found

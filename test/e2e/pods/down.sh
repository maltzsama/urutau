#!/usr/bin/env bash
# Tear down the pod e2e environment: every CDCPipeline (and the Pods it owns),
# the operator, and the in-cluster data services. cert-manager is left in
# place — it is a cluster add-on, slow to reinstall, and harmless to keep.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT"

KUBECTL="${KUBECTL:-kubectl}"

"$KUBECTL" delete cdcpipelines --all -A --ignore-not-found --wait=true
"$KUBECTL" delete -k test/e2e/pods/k8s/operator --ignore-not-found
"$KUBECTL" delete -k test/e2e --ignore-not-found

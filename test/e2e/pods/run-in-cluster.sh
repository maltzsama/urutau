#!/usr/bin/env bash
# Run the pod e2e harness INSIDE minikube, as a Job next to the pipeline it
# drives. The load generator and the oracle then reach MySQL and Trino
# directly (see incluster_test.go) instead of through a kubectl port-forward.
#
#   make e2e-pods-test                                   # every scenario
#   RUN='^TestProductionReadinessChaos$' make e2e-pods-test
#   URUTAU_E2E_PROFILE=full RUN='^TestProductionReadinessMatrix$' TIMEOUT=240m make e2e-pods-test
#
# Needs `make e2e-pods-up` (the stack and the operator) and `make
# e2e-pods-image` (this harness as urutau-e2e:dev). The test's output streams
# here; its artifacts (diagnostics, logs) are copied to $ARTIFACTS, and the
# script exits with the test binary's code.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT"

KUBECTL="${KUBECTL:-kubectl}"
NS=e2e
JOB="${JOB:-urutau-e2e}"
IMAGE="${E2E_IMAGE:-urutau-e2e:dev}"
RUN="${RUN:-.}"
TIMEOUT="${TIMEOUT:-90m}"
ARTIFACTS="${ARTIFACTS:-$ROOT/.e2e-artifacts/$(date +%Y%m%d-%H%M%S)}"

if ! minikube image ls 2>/dev/null | grep -q "$IMAGE"; then
  echo "error: image $IMAGE is not loaded into minikube; run: make e2e-pods-image" >&2
  exit 1
fi

"$KUBECTL" apply -f test/e2e/pods/k8s/runner/rbac.yaml >/dev/null
"$KUBECTL" -n "$NS" delete job "$JOB" --ignore-not-found --wait=true >/dev/null

# Scenario knobs pass through as env; empty ones are left unset.
envs=""
for v in URUTAU_E2E_PROFILE URUTAU_E2E_SEED URUTAU_E2E_TABLES URUTAU_E2E_PODS_IMAGE; do
  if [ -n "${!v:-}" ]; then
    envs+="            - {name: $v, value: \"${!v}\"}"$'\n'
  fi
done

# The container runs the tests, records the exit code, then waits for this
# script to copy the artifacts out (the files die with the Pod).
"$KUBECTL" apply -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: $JOB
  namespace: $NS
spec:
  backoffLimit: 0
  template:
    metadata:
      labels: {app: urutau-e2e}
    spec:
      serviceAccountName: e2e-runner
      restartPolicy: Never
      containers:
        - name: e2e
          image: $IMAGE
          imagePullPolicy: Never
          command: ["sh", "-c"]
          args:
            - |
              pods.test -test.v -test.count=1 -test.timeout=$TIMEOUT -test.run '$RUN'
              echo \$? > /artifacts/.exit
              while [ ! -f /artifacts/.collected ]; do sleep 2; done
          env:
${envs}          resources:
            requests: {cpu: "2", memory: 2Gi}
            limits: {memory: 6Gi}
          volumeMounts:
            - {name: artifacts, mountPath: /artifacts}
      volumes:
        - {name: artifacts, emptyDir: {}}
EOF

"$KUBECTL" -n "$NS" wait --for=condition=Ready pod -l job-name="$JOB" --timeout=300s >/dev/null
POD="$("$KUBECTL" -n "$NS" get pod -l job-name="$JOB" -o jsonpath='{.items[0].metadata.name}')"
echo "==> $JOB running as $POD (run=$RUN timeout=$TIMEOUT)"

"$KUBECTL" -n "$NS" logs -f "$POD" &
LOGS=$!
trap 'kill $LOGS 2>/dev/null || true' EXIT

until code="$("$KUBECTL" -n "$NS" exec "$POD" -- cat /artifacts/.exit 2>/dev/null)"; do
  if ! "$KUBECTL" -n "$NS" get pod "$POD" >/dev/null 2>&1; then
    echo "error: $POD is gone before the tests finished" >&2
    exit 1
  fi
  sleep 10
done
sleep 2 # let the log stream drain

mkdir -p "$ARTIFACTS"
"$KUBECTL" -n "$NS" cp "$POD:/artifacts" "$ARTIFACTS" >/dev/null 2>&1 || echo "warning: artifact copy failed" >&2
"$KUBECTL" -n "$NS" exec "$POD" -- touch /artifacts/.collected
echo "==> exit $code; artifacts in $ARTIFACTS"
exit "$code"

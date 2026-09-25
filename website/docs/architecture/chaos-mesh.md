---
sidebar_position: 7
---

# Chaos Mesh in the pod e2e cluster

Issue #383 provisions real [Chaos Mesh](https://chaos-mesh.org) in the pod e2e
minikube cluster and gives the harness (`test/e2e/pods`) primitives to create,
observe and remove experiments against the real Urutau coordinator/worker
Pods. It is deliberately scoped to that foundation: the *randomized*
scheduling of overlapping faults belongs to issue #385's nondeterministic
chaos controller, built on top of these primitives, not to this page.

The acceptance bar this exists to satisfy: fault injection here must be a
real Chaos Mesh resource acting on the real MySQL → Iceberg pipeline, never
`deletePod` or in-process goroutine control standing in for it.

## Install

`test/e2e/pods/up.sh` installs Chaos Mesh the same way it installs every
other e2e cluster add-on (cert-manager, KEDA): pin a version, apply, wait for
`Available`. Left installed across `down.sh` teardown, same as cert-manager
and KEDA — it is a slow-to-reinstall cluster add-on, not per-run state.

Chaos Mesh's only supported install path is Helm; there is no pinned
single-file manifest upstream the way there is for cert-manager and KEDA. To
keep `up.sh`'s "kubectl + minikube only" contract, `test/e2e/pods/k8s/chaos-mesh/manifest.yaml`
is a one-time static render of the chart (dashboard disabled — the harness
drives everything through kubectl, not the web UI). The file's header
documents the exact `helm template` invocation to regenerate it on a version
bump.

Several of its CRDs (`schedules`, `workflows`, `workflownodes`) exceed the
client-side `last-applied-configuration` annotation size limit — the same
problem KEDA's CRDs hit — so the install uses `kubectl apply --server-side
--force-conflicts`, not a plain `apply -f`.

## Harness primitives (`test/e2e/pods/chaos_mesh_test.go`)

Thin, kubectl-based wrappers, matching the shell-out style of the rest of the
harness (no client-go/controller-runtime dependency):

| Function | Purpose |
|----------|---------|
| `verifyChaosMeshReady` | Asserts the controller manager and daemon are Ready. Fails the test hard if Chaos Mesh is missing or broken — chaos must never be silently skipped. |
| `applyPodChaos` | Creates a `PodChaos` (`pod-kill` or `pod-failure`) targeting pods by label selector. |
| `applyNetworkChaos` | Creates a `NetworkChaos` between a selector and a target selector (e.g. worker ↔ coordinator). |
| `applyStressChaos` | Creates a `StressChaos` (`cpu` or `memory`) targeting pods by label selector. |
| `waitChaosExperimentInjected` | Polls the `AllInjected` condition. |
| `waitChaosExperimentFinished` | Polls `.status.experiment.desiredPhase` for `Finished`. |
| `deleteChaosExperiment` | Removes the experiment CR, best-effort — safe from `t.Cleanup`. |

Targets are the same coordinator/worker label selectors the rest of the
harness already uses (`deletePod`, `workerSTSs`):

| Target | Selector |
|--------|----------|
| A pipeline's coordinator | `app=urutau-coordinator,urutau.io/pipeline=<name>` |
| All of a pipeline's workers | `app=urutau-worker,urutau.io/pipeline=<name>` |
| One table's worker StatefulSet | `urutau.io/worker=<pipeline>-<target>` |

Namespace: `pod-e2e` (the harness's `testNS`), alongside the CDCPipeline the
experiment targets.

## `pod-kill` is continuous, not one-shot

`PodChaos` with `action: pod-kill` never reaches `desiredPhase: Finished` —
Chaos Mesh reapplies the kill to any new pod matching the selector until the
CR is deleted. `waitChaosExperimentFinished` hangs on it forever; use
`waitChaosExperimentInjected` (the `AllInjected` condition) instead, then
delete the experiment once the kill has been observed.

`pod-kill` also deletes the Pod object outright, the same as the harness's
own `deletePod` — the owning StatefulSet recreates it as a **new Pod with the
same ordinal name but a new UID**. `restartCount` stays `0` on the recreated
Pod (it is a new object, not a restarted container), so it cannot prove the
kill happened. Compare `metadata.uid` before and after instead — that is how
`TestChaosMeshPodKillAffectsRealPipeline` (`chaos_mesh_smoke_test.go`) proves
Chaos Mesh, not the test process, did the killing.

Duration-bound actions (`pod-failure`, `NetworkChaos`, `StressChaos`) behave
as expected and do reach `Finished` on their own once their `duration`
elapses.

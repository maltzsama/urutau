// Package operator is the Kubernetes operator: it reconciles CDCPipeline CRs
// into the coordinator workload (its StatefulSet, Service, ConfigMap and RBAC)
// and validates spec mutations at admission.
//
// Naming: the reconciler type is CoordinatorReconciler for the workload it
// manages — the coordinator — not for itself. The reconciler IS the operator.
package operator

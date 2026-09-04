// Package v1alpha1 holds the CDCPipeline CR types (apiVersion
// urutau.io/v1alpha1) and the machine-written status. The spec mirrors the
// resolved spec contract; the status is written only by the coordinator.
// One coordinator StatefulSet per CR; the operator creates it and stops
// reconciling once status.terminated is set.
package v1alpha1

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CDCPipeline is the submitted unit of replication work.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=cdcpipelines,scope=Namespaced,singular=cdcpipeline
type CDCPipeline struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CDCPipelineSpec   `json:"spec"`
	Status CDCPipelineStatus `json:"status,omitempty"`
}

// CDCPipelineSpec is what the developer submits (§8.2).
type CDCPipelineSpec struct {
	// Image is the ONE container image for both the coordinator and every
	// worker it provisions — the same coordinator/worker split that ships
	// today as three binaries in one image (build/Dockerfile); only the
	// Command differs per Pod. Mirrors Spark's single spec.image for
	// driver and executor alike. Required for Kubernetes worker
	// provisioning; the coordinator itself can still run without it if
	// Image is left empty (workers must then be launched some other way).
	Image string `json:"image,omitempty"`

	// Definition points at the job: a user image (official), an s3 script,
	// or an inline table list.
	Definition Definition `json:"definition"`

	// Secrets reference the source and catalog credentials (never inline).
	Secrets Secrets `json:"secrets"`

	Coordinator CoordinatorSpec `json:"coordinator"`
	// Worker is the resource DEFAULT applied to every worker Deployment a
	// table provisions when that table's own spec.Table.Workers.CPU/Memory
	// is unset — a table-level Workers.CPU/Memory always overrides this.
	// Overhead is added on top of CPU/Memory to form the container's
	// limit (mirrors Spark's executor memoryOverhead): CPU/Memory become
	// the resource request, CPU+CPUOverhead/Memory+MemoryOverhead become
	// the limit.
	Worker WorkerDefaults `json:"worker,omitempty"`
}

// Definition is exactly one of image | s3 | inline.
type Definition struct {
	// Image is the user image (FROM urutau-runtime) whose entrypoint runs
	// `urutau plan`. Mutually exclusive with s3 and inline.
	Image      string `json:"image,omitempty"`
	Entrypoint string `json:"entrypoint,omitempty"`
	S3         string `json:"s3,omitempty"`
	// Inline is the full pipeline spec (source, sink, tables) verbatim —
	// the same artifact the planner would render from image/s3. Credentials
	// stay out: URI and credential fields left empty are filled from the
	// secrets mounted into the coordinator pod at boot. Validated with the
	// same server-side rules the coordinator runs.
	//
	// Secret key convention (spec.secrets):
	//   source: uri
	//   catalog: uri, clientId, clientSecret, scope
	Inline map[string]any `json:"inline,omitempty"`
}

// Secrets names the k8s Secrets holding credentials.
type Secrets struct {
	Source  string `json:"source"`
	Catalog string `json:"catalog"`
}

// CoordinatorSpec tunes the coordinator deployment. CPU/Memory are
// Kubernetes resource quantities (e.g. "1", "2Gi") applied as both
// request and limit on the coordinator's own container — the
// coordinator is a single long-lived process, not a pool sized like the
// workers, so it has no overhead knob.
type CoordinatorSpec struct {
	Snapshot       SnapshotSpec    `json:"snapshot,omitempty"`
	Supervision    SupervisionSpec `json:"supervision,omitempty"`
	StatusInterval string          `json:"statusInterval,omitempty"`
	CPU            string          `json:"cpu,omitempty"`
	Memory         string          `json:"memory,omitempty"`
	MetricsAddr    string          `json:"metricsAddr,omitempty"`
}

// SnapshotSpec tunes the DBLog snapshot phase.
type SnapshotSpec struct {
	ChunkSize         int `json:"chunkSize,omitempty"`
	MaxParallelChunks int `json:"maxParallelChunks,omitempty"`
}

// SupervisionSpec tunes the reset/terminate window.
type SupervisionSpec struct {
	AckTimeout string `json:"ackTimeout,omitempty"`
	MaxResets  int    `json:"maxResets,omitempty"`
	Window     string `json:"window,omitempty"`
}

// WorkerDefaults is the resource default for every worker Deployment —
// overridden per table by that table's own spec.Table.Workers.CPU/Memory.
// CPU/Memory become the container's resource request; CPU+CPUOverhead and
// Memory+MemoryOverhead become its limit (mirrors Spark's
// spark.executor.memoryOverhead: headroom for the process's own
// bookkeeping beyond its declared working set, e.g. Arrow/JVM-style
// off-heap buffers).
type WorkerDefaults struct {
	CPU            string `json:"cpu,omitempty"`
	CPUOverhead    string `json:"cpu_overhead,omitempty"`
	Memory         string `json:"memory,omitempty"`
	MemoryOverhead string `json:"memory_overhead,omitempty"`
}

// CDCPipelineStatus is written only by the coordinator (§9).
type CDCPipelineStatus struct {
	Phase              string      `json:"phase,omitempty"`
	ObservedGeneration int64       `json:"observedGeneration,omitempty"`
	RunID              string      `json:"runId,omitempty"`
	SpecHash           string      `json:"specHash,omitempty"`
	Terminated         *Terminated `json:"terminated,omitempty"`
}

// Terminated records the reason and time of a terminal exit.
type Terminated struct {
	Reason string `json:"reason"`
	At     string `json:"at"`
}

// +kubebuilder:object:root=true
type CDCPipelineList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CDCPipeline `json:"items"`
}

func init() {
	SchemeBuilder.Register(&CDCPipeline{}, &CDCPipelineList{})
}

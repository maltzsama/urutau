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
	// Definition points at the job: a user image (official), an s3 script,
	// or an inline table list.
	Definition Definition `json:"definition"`

	// Secrets reference the source and catalog credentials (never inline).
	Secrets Secrets `json:"secrets"`

	Coordinator    CoordinatorSpec `json:"coordinator"`
	WorkerDefaults WorkerDefaults  `json:"workerDefaults"`
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

// CoordinatorSpec tunes the coordinator deployment.
type CoordinatorSpec struct {
	Snapshot       SnapshotSpec         `json:"snapshot,omitempty"`
	Supervision    SupervisionSpec      `json:"supervision,omitempty"`
	StatusInterval string               `json:"statusInterval,omitempty"`
	Resources      ResourceRequirements `json:"resources,omitempty"`
	MetricsAddr    string               `json:"metricsAddr,omitempty"`
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

// WorkerDefaults applies to every worker pod.
type WorkerDefaults struct {
	Resources ResourceRequirements `json:"resources,omitempty"`
}

// ResourceRequirements is a subset of the core resource fields.
type ResourceRequirements struct {
	Requests map[string]string `json:"requests,omitempty"`
	Limits   map[string]string `json:"limits,omitempty"`
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

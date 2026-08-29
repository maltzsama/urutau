// Package spec defines the resolvedSpec: the contract between the planner,
// the admission webhook, and coordinator boot. Validation is single and
// server-side. JSON tags are the wire contract with the planner and do not
// change.
package spec

import "errors"

var ErrNotImplemented = errors.New("spec: validation not implemented yet")

type WriteMode string

const (
	WriteModeUpsert WriteMode = "upsert"
	WriteModeAppend WriteMode = "append"
)

type Spec struct {
	Pipeline string  `json:"pipeline"`
	Source   Source  `json:"source"`
	Sink     Sink    `json:"sink"`
	Tables   []Table `json:"tables"`
}

type Source struct {
	Kind     string `json:"kind"`
	URI      string `json:"uri"`
	ServerID string `json:"serverId,omitempty"`
	SlotName string `json:"slotName,omitempty"`
}

type Sink struct {
	URI       string   `json:"uri"`
	Namespace string   `json:"namespace"`
	Defaults  Defaults `json:"defaults"`
}

type Defaults struct {
	WriteMode      WriteMode `json:"writeMode,omitempty"`
	TargetFileSize string    `json:"targetFileSize,omitempty"`
}

type Table struct {
	Source            string    `json:"source"`
	Target            string    `json:"target"`
	PrimaryKey        []string  `json:"primaryKey,omitempty"`
	PartitionBy       []string  `json:"partitionBy,omitempty"`
	Filter            *Filter   `json:"filter,omitempty"`
	WriteMode         WriteMode `json:"writeMode,omitempty"`
	Worker            string    `json:"worker,omitempty"`
	CreateIfNotExists bool      `json:"createIfNotExists,omitempty"`
	FilterImmutable   bool      `json:"filterImmutable,omitempty"`
}

func (s *Spec) Validate() error {
	return ErrNotImplemented
}

// Package spec defines the resolvedSpec: the contract between the planner,
// the admission webhook, and coordinator boot. Validation is single and
// server-side. JSON tags are the wire contract with the planner and do not
// change.
package spec

import (
	"github.com/maltzsama/urutau/internal/core"
)

// WriteMode selects how a table is written: upsert reflects state through
// the primary key; append emits every change as a new row.

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
	Kind          string `json:"kind"`
	URI           string `json:"uri"`
	ServerID      string `json:"serverId,omitempty"`
	SlotName      string `json:"slotName,omitempty"`
	SnapshotMode  string `json:"snapshotMode,omitempty"`
	BootstrapAdds string `json:"bootstrapServers,omitempty"`
	GroupID       string `json:"groupId,omitempty"`
}

type Sink struct {
	URI          string   `json:"uri"`
	Namespace    string   `json:"namespace"`
	Warehouse    string   `json:"warehouse,omitempty"`
	ClientID     string   `json:"clientId,omitempty"`
	ClientSecret string   `json:"clientSecret,omitempty"`
	Scope        string   `json:"scope,omitempty"`
	Defaults     Defaults `json:"defaults"`
}

type Defaults struct {
	WriteMode      WriteMode `json:"writeMode,omitempty"`
	TargetFileSize string    `json:"targetFileSize,omitempty"`
}

type Table struct {
	Source            string                `json:"source"`
	Target            string                `json:"target"`
	PrimaryKey        []string              `json:"primaryKey,omitempty"`
	PartitionBy       []string              `json:"partitionBy,omitempty"`
	Filter            *Filter               `json:"filter,omitempty"`
	WriteMode         WriteMode             `json:"writeMode,omitempty"`
	Worker            string                `json:"worker,omitempty"`
	CreateIfNotExists bool                  `json:"createIfNotExists,omitempty"`
	FilterImmutable   bool                  `json:"filterImmutable,omitempty"`
	// Metadata lands pipeline metadata columns (op, commit_ts, position, …)
	// in the target table. The destination name is explicit via As.
	Metadata []core.MetadataColumn `json:"metadata,omitempty"`
	// Cast overrides one source column's canonical type. Key is the source
	// column name; value is the textual canonical target (e.g. "string",
	// "decimal(20,4)", "timestamptz(assume_utc)").
	Cast map[string]string `json:"cast,omitempty"`
	// Columns defines the source schema explicitly for sources that cannot
	// be introspected (e.g. Kafka). Each entry maps a column name to its
	// canonical type string. Ignored for SQL sources which introspect
	// automatically.
	Columns map[string]string `json:"columns,omitempty"`
}

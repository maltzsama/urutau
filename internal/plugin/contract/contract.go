package contract

import "github.com/maltzsama/urutau/core"

const ProtocolVersion = 1

type Role string

const (
	RoleSource Role = "source"
	RoleSink   Role = "sink"
)

type GetFlightInfoRequest struct {
	Table      string `json:"table"`
	Mode       string `json:"mode"`
	FromOffset string `json:"fromOffset,omitempty"`
}

type FlightInfoResponse struct {
	EndOffset    string `json:"endOffset,omitempty"`
	EstimatedLag *int64 `json:"estimatedLag,omitempty"`
}

type ListTablesResponse struct {
	Tables []TableInfo `json:"tables"`
}

type TableInfo struct {
	Name             string `json:"name"`
	SupportsSnapshot bool   `json:"supportsSnapshot"`
}

type StatusResponse struct {
	State     string                 `json:"state"`
	Tables    map[string]TableStatus `json:"tables,omitempty"`
	UptimeSec int64                  `json:"uptimeSec"`
}

type TableStatus struct {
	Offset    string `json:"offset,omitempty"`
	LagEvents *int64 `json:"lagEvents,omitempty"`
}

type DoPutRequest struct {
	Mode  string `json:"mode"`
	Table string `json:"table"`
}

// EnsureTableRequest is the body of the "urutau.ensure_table" action: the
// table's canonical schema, primary key and write mode, so an external sink has
// the type shape and the dedup key instead of re-inferring them from
// stringified rows (issue #569).
type EnsureTableRequest struct {
	Table       string      `json:"table"`
	Schema      core.Schema `json:"schema"`
	PrimaryKey  []string    `json:"primaryKey,omitempty"`
	PartitionBy []string    `json:"partitionBy,omitempty"`
	Mode        string      `json:"mode"` // "upsert" | "append"
}

// PositionRequest is the body of the "urutau.position" action.
type PositionRequest struct {
	Table string `json:"table"`
}

// PositionResponse is that action's result: the table's committed position, or
// empty when it has never been written (issue #569).
type PositionResponse struct {
	Position string `json:"position"`
}

package contract

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

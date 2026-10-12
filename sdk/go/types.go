package sdk

// Data model types (PROTOCOL.md §4).
//
// Compatibility rule: all structs here are decoded with encoding/json, which
// ignores unknown JSON fields and tolerates missing fields by design, so
// responses from newer hub versions carrying new fields never break this SDK.
// Pointer fields distinguish "absent" from "null" where the protocol allows null.

// Node is a registered controlled node (PROTOCOL.md §4.1).
type Node struct {
	ID           string         `json:"id,omitempty"`
	Name         string         `json:"name,omitempty"`
	Status       string         `json:"status,omitempty"` // online|busy|offline|unknown
	Capabilities []string       `json:"capabilities,omitempty"`
	Tags         []string       `json:"tags,omitempty"`
	Metadata     map[string]any `json:"metadata,omitempty"`
	LastSeen     string         `json:"last_seen,omitempty"`
	RegisteredAt string         `json:"registered_at,omitempty"`
	SeatID       *string        `json:"seat_id,omitempty"` // occupied seat (PROTOCOL.md §4.7)
}

// SeatExpect is the expected profile a filling node must match
// (PROTOCOL.md §4.7): capabilities = all required, tags = any-match,
// metadata = key/value equality. All fields optional.
type SeatExpect struct {
	Capabilities []string       `json:"capabilities,omitempty"`
	Tags         []string       `json:"tags,omitempty"`
	Metadata     map[string]any `json:"metadata,omitempty"`
}

// Seat is a reservable position in the engine (PROTOCOL.md §4.7): the unit of
// extensibility. Kind "standing" = occupied by a node; "vacant" = reserved.
type Seat struct {
	ID         string      `json:"id,omitempty"`
	Kind       string      `json:"kind,omitempty"` // standing|vacant
	Name       string      `json:"name,omitempty"`
	BindPolicy string      `json:"bind_policy,omitempty"` // auto|manual
	Expect     *SeatExpect `json:"expect,omitempty"`
	NodeID     *string     `json:"node_id,omitempty"`
	CreatedAt  string      `json:"created_at,omitempty"`
	FilledAt   *string     `json:"filled_at,omitempty"`
	ReleasedAt *string     `json:"released_at,omitempty"`
}

// SeatSpec is the seat.create parameter: creates a vacant seat.
// BindPolicy defaults to "manual" server-side; Expect is optional.
type SeatSpec struct {
	Name       string      `json:"name,omitempty"`
	BindPolicy string      `json:"bind_policy,omitempty"` // auto|manual
	Expect     *SeatExpect `json:"expect,omitempty"`
}

// CommandError describes a failed command/task (PROTOCOL.md §4.2).
type CommandError struct {
	Code    int            `json:"code,omitempty"`
	Message string         `json:"message,omitempty"`
	Data    map[string]any `json:"data,omitempty"`
}

// Command is a single command sent to a node (PROTOCOL.md §4.2).
type Command struct {
	ID             string         `json:"id,omitempty"`
	NodeID         string         `json:"node_id,omitempty"`
	Method         string         `json:"method,omitempty"`
	Params         map[string]any `json:"params,omitempty"`
	Status         string         `json:"status,omitempty"` // pending|accepted|running|succeeded|failed|cancelled|timeout
	Result         any            `json:"result,omitempty"`
	Error          *CommandError  `json:"error,omitempty"`
	Timeout        int64          `json:"timeout,omitempty"`
	CreatedAt      string         `json:"created_at,omitempty"`
	StartedAt      *string        `json:"started_at,omitempty"`
	FinishedAt     *string        `json:"finished_at,omitempty"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
	TaskID         *string        `json:"task_id,omitempty"`
}

// Task is a collection of commands (PROTOCOL.md §4.3).
type Task struct {
	ID         string        `json:"id,omitempty"`
	Name       string        `json:"name,omitempty"`
	Status     string        `json:"status,omitempty"` // pending|running|succeeded|failed|cancelled|partial
	Progress   float64       `json:"progress,omitempty"`
	CommandIDs []string      `json:"command_ids,omitempty"`
	Error      *CommandError `json:"error,omitempty"`
	CreatedAt  string        `json:"created_at,omitempty"`
	UpdatedAt  string        `json:"updated_at,omitempty"`
}

// HubEvent is a server-pushed event (PROTOCOL.md §4.4). Unknown event types
// must be ignored by consumers instead of treated as errors.
type HubEvent struct {
	ID        string         `json:"id,omitempty"`
	Type      string         `json:"type,omitempty"`
	Time      string         `json:"time,omitempty"`
	NodeID    string         `json:"node_id,omitempty"`
	TaskID    string         `json:"task_id,omitempty"`
	CommandID string         `json:"command_id,omitempty"`
	Data      map[string]any `json:"data,omitempty"`
}

// Capability is an engine capability (PROTOCOL.md §4.6).
type Capability struct {
	ID      string `json:"id,omitempty"`
	Version string `json:"version,omitempty"`
}

// ClientInfo is the engine.info request parameter (PROTOCOL.md §3.2).
// All fields are optional.
type ClientInfo struct {
	Name             string         `json:"name,omitempty"`
	Version          string         `json:"version,omitempty"`
	ProtocolVersions []string       `json:"protocol_versions,omitempty"`
	Encodings        []string       `json:"encodings,omitempty"`
	Transports       []string       `json:"transports,omitempty"`
	Features         map[string]any `json:"features,omitempty"`
	Platform         map[string]any `json:"platform,omitempty"`
}

// SessionInfo describes the current session (PROTOCOL.md §3.3).
type SessionInfo struct {
	Authenticated bool   `json:"authenticated,omitempty"`
	ExpiresAt     string `json:"expires_at,omitempty"`
}

// ServerInfo is the engine.info result (PROTOCOL.md §3.3).
type ServerInfo struct {
	ProtocolVersion   string         `json:"protocol_version,omitempty"`
	Server            *ServerDesc    `json:"server,omitempty"`
	SupportedVersions []string       `json:"supported_versions,omitempty"`
	Encodings         []string       `json:"encodings,omitempty"`
	Transports        []string       `json:"transports,omitempty"`
	Features          map[string]any `json:"features,omitempty"`
	Capabilities      []Capability   `json:"capabilities,omitempty"`
	Session           *SessionInfo   `json:"session,omitempty"`
	ServerTime        string         `json:"server_time,omitempty"`
}

// ServerDesc identifies the engine binary.
type ServerDesc struct {
	Name    string `json:"name,omitempty"`
	Version string `json:"version,omitempty"`
}

// AuthResult is the auth.login / auth.refresh result (PROTOCOL.md §3.5).
type AuthResult struct {
	AccessToken  string `json:"access_token,omitempty"`
	TokenType    string `json:"token_type,omitempty"`
	ExpiresIn    int64  `json:"expires_in,omitempty"`
	RefreshToken string `json:"refresh_token,omitempty"`
}

// CommandSpec is the command.send / command.batch entry (PROTOCOL.md §5).
type CommandSpec struct {
	NodeID         string         `json:"node_id,omitempty"`
	Method         string         `json:"method,omitempty"`
	Params         map[string]any `json:"params,omitempty"`
	Timeout        int64          `json:"timeout,omitempty"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
}

// TaskSpec is the task.create parameter (PROTOCOL.md §5).
type TaskSpec struct {
	Name     string        `json:"name,omitempty"`
	Commands []CommandSpec `json:"commands,omitempty"`
	Parallel bool          `json:"parallel,omitempty"`
}

// NodeListResult is the node.list result.
type NodeListResult struct {
	Nodes []Node `json:"nodes,omitempty"`
	Total int    `json:"total,omitempty"`
}

// TaskListResult is the task.list result.
type TaskListResult struct {
	Tasks []Task `json:"tasks,omitempty"`
	Total int    `json:"total,omitempty"`
}

// SeatListResult is the seat.list result.
type SeatListResult struct {
	Seats []Seat `json:"seats,omitempty"`
	Total int    `json:"total,omitempty"`
}

// CommandBatchResult is the command.batch result (may include a task_id).
type CommandBatchResult struct {
	Commands []Command `json:"commands,omitempty"`
	TaskID   *string   `json:"task_id,omitempty"`
}

// EventFilters filters an event subscription (PROTOCOL.md §5 event.subscribe).
// event_types ∩ node/task filters apply; empty means all.
type EventFilters struct {
	EventTypes []string `json:"event_types,omitempty"`
	NodeID     string   `json:"node_id,omitempty"`
	TaskID     string   `json:"task_id,omitempty"`
}

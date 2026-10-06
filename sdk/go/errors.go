package sdk

import (
	"encoding/json"
	"fmt"
)

// Hub error codes (PROTOCOL.md §7). Codes are additive-only; never remove or repurpose.

const (
	// Standard JSON-RPC 2.0 errors (§7.1).
	ErrParseError     = -32700
	ErrInvalidRequest = -32600
	ErrMethodNotFound = -32601
	ErrInvalidParams  = -32602
	ErrInternalError  = -32603

	// Domain errors (§7.2).
	ErrAuthRequired            = -32001
	ErrAuthInvalid             = -32002
	ErrAuthExpired             = -32003
	ErrAuthForbidden           = -32004
	ErrProtocolVersionMismatch = -32005
	ErrEncodingNotSupported    = -32006
	ErrTransportNotSupported   = -32007
	ErrNodeNotFound            = -32010
	ErrNodeOffline             = -32011
	ErrNodeBusy                = -32012
	ErrCommandNotFound         = -32013
	ErrCommandCancelRejected   = -32014
	ErrTaskNotFound            = -32020
	ErrTaskCancelRejected      = -32021
	ErrSubscriptionNotFound    = -32030
	ErrEventTypeNotSupported   = -32031
	ErrFeatureDisabled         = -32040
	ErrRateLimited             = -32041
)

// HubError is a JSON-RPC error returned by the hub. It is returned as an error
// from Call and friends; use errors.As(err, new(*HubError)) to inspect it.
type HubError struct {
	Code    int            `json:"code"`
	Message string         `json:"message"`
	Data    map[string]any `json:"data,omitempty"`
}

func (e *HubError) Error() string {
	if len(e.Data) > 0 {
		return fmt.Sprintf("hub error %d: %s (data=%v)", e.Code, e.Message, e.Data)
	}
	return fmt.Sprintf("hub error %d: %s", e.Code, e.Message)
}

// IsAuth reports whether the error is authentication related.
func (e *HubError) IsAuth() bool {
	switch e.Code {
	case ErrAuthRequired, ErrAuthInvalid, ErrAuthExpired, ErrAuthForbidden:
		return true
	}
	return false
}

// IsVersionMismatch reports whether the error is a protocol version mismatch.
func (e *HubError) IsVersionMismatch() bool {
	return e.Code == ErrProtocolVersionMismatch
}

// UnmarshalJSON decodes the error keeping unknown fields inside Data so
// callers can always inspect extra error payload from newer hub versions.
func (e *HubError) UnmarshalJSON(b []byte) error {
	var raw struct {
		Code    int            `json:"code"`
		Message string         `json:"message"`
		Data    map[string]any `json:"data"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	e.Code = raw.Code
	e.Message = raw.Message
	e.Data = raw.Data
	return nil
}

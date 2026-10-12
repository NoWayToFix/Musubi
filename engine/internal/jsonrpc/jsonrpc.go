// Package jsonrpc implements the JSON-RPC 2.0 envelope (PROTOCOL.md §2) and
// the method router used by the Control Hub engine (§5). It is transport
// agnostic: WebSocket, HTTP POST and long-poll all dispatch through it.
package jsonrpc

import (
	"bytes"
	"encoding/json"
	"sync"
)

// JSON-RPC standard error codes (PROTOCOL.md §7.1).
const (
	CodeParseError     = -32700
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeInvalidParams  = -32602
	CodeInternalError  = -32603
)

// Domain error codes (PROTOCOL.md §7.2). Additive-only, never removed.
const (
	CodeAuthRequired    = -32001
	CodeAuthInvalid     = -32002
	CodeAuthExpired     = -32003
	CodeAuthForbidden   = -32004
	CodeVersionMismatch = -32005
	CodeEncodingNS      = -32006
	CodeTransportNS     = -32007

	CodeNodeNotFound     = -32010
	CodeNodeOffline      = -32011
	CodeNodeBusy         = -32012
	CodeCommandNotFound  = -32013
	CodeCommandCancelRej = -32014

	CodeTaskNotFound  = -32020
	CodeTaskCancelRej = -32021

	CodeSubNotFound = -32030
	CodeEventTypeNS = -32031

	CodeFeatureDisabled = -32040
	CodeRateLimited     = -32041

	CodeSeatNotFound       = -32050
	CodeSeatOccupied       = -32051
	CodeSeatExpectMismatch = -32052
)

// Error is a JSON-RPC error object (schema common.json#/definitions/error_object).
type Error struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// NewError builds an Error, marshalling data (an object) if non-nil.
func NewError(code int, message string, data any) *Error {
	var raw json.RawMessage
	if data != nil {
		raw, _ = json.Marshal(data)
	}
	return &Error{Code: code, Message: message, Data: raw}
}

// ErrParse reports -32700.
func ErrParse() *Error { return NewError(CodeParseError, "parse error", nil) }

// ErrInvalidRequest reports -32600.
func ErrInvalidRequest(msg string) *Error { return NewError(CodeInvalidRequest, msg, nil) }

// ErrInvalidParams reports -32602.
func ErrInvalidParams(msg string) *Error { return NewError(CodeInvalidParams, msg, nil) }

// ErrMethodNotFound reports -32601.
func ErrMethodNotFound(method string) *Error {
	return NewError(CodeMethodNotFound, "method not found", map[string]any{"method": method})
}

// Request is a JSON-RPC request envelope (PROTOCOL.md §2.1).
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

// IsNotification reports whether the request carries no id (a client-to-server
// notification, PROTOCOL.md §2.4 only defines server-to-client notifications).
func (r *Request) IsNotification() bool {
	return len(r.ID) == 0 || bytes.Equal(bytes.TrimSpace(r.ID), []byte("null"))
}

// Response is a JSON-RPC response envelope (PROTOCOL.md §2.2 / §2.3).
type Response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *Error          `json:"error,omitempty"`
}

// ErrorResponse builds a response carrying err. req may be nil (id becomes null).
func ErrorResponse(req *Request, err *Error) *Response {
	resp := &Response{JSONRPC: "2.0", Error: err}
	if req != nil {
		resp.ID = req.ID
	}
	return resp
}

// Parsed holds one request from a message, or an envelope-level error for it.
type Parsed struct {
	Req *Request
	Err *Error
}

// ParseMessage parses a JSON-RPC message: a single request object or a batch
// array (§2.5). Fatal errors (bad JSON, malformed top-level shape, empty
// batch) are returned in fatal; per-element errors are reported through
// Parsed.Err with a nil Req.
func ParseMessage(data []byte) (batch bool, parsed []Parsed, fatal *Error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return false, nil, ErrParse()
	}
	switch trimmed[0] {
	case '{':
		return false, []Parsed{parseRequest(trimmed)}, nil
	case '[':
		var elems []json.RawMessage
		if err := json.Unmarshal(trimmed, &elems); err != nil {
			return true, nil, ErrParse()
		}
		if len(elems) == 0 {
			return true, nil, ErrInvalidRequest("batch must not be empty")
		}
		parsed = make([]Parsed, 0, len(elems))
		for _, e := range elems {
			parsed = append(parsed, parseRequest(e))
		}
		return true, parsed, nil
	default:
		return false, nil, ErrInvalidRequest("request must be a JSON object")
	}
}

func parseRequest(raw json.RawMessage) Parsed {
	var req Request
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&req); err != nil {
		return Parsed{Err: ErrInvalidRequest("invalid request object")}
	}
	if req.JSONRPC != "2.0" {
		return Parsed{Err: ErrInvalidRequest(`"jsonrpc" must be "2.0"`)}
	}
	if req.Method == "" {
		return Parsed{Err: ErrInvalidRequest(`"method" is required`)}
	}
	// params, when present, must be an object (null == absent).
	if len(req.Params) > 0 {
		t := bytes.TrimSpace(req.Params)
		if !bytes.Equal(t, []byte("null")) && t[0] != '{' {
			return Parsed{Err: ErrInvalidParams("params must be an object")}
		}
	}
	// id, when present, must be a string or a number (null/absent = notification).
	if len(req.ID) > 0 {
		t := bytes.TrimSpace(req.ID)
		b := t[0]
		if b != '"' && b != '-' && !(b >= '0' && b <= '9') && !bytes.Equal(t, []byte("null")) {
			return Parsed{Err: ErrInvalidRequest("invalid id")}
		}
	}
	return Parsed{Req: &req}
}

// Handler executes one method. ctx carries per-call state (auth session,
// optional per-connection subscription tracking); params is the raw params
// object (nil when omitted). Handlers must tolerate unknown fields.
type Handler func(ctx any, params json.RawMessage) (any, *Error)

// Method is a registered method.
type Method struct {
	Name    string
	Handler Handler
	Public  bool // never requires authentication (engine.*, auth.login, auth.refresh)
}

// Router is the method registry and dispatcher.
type Router struct {
	mu      sync.RWMutex
	methods map[string]Method
}

// NewRouter creates an empty router.
func NewRouter() *Router {
	return &Router{methods: map[string]Method{}}
}

// Register adds or replaces a method.
func (r *Router) Register(name string, h Handler, public bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.methods[name] = Method{Name: name, Handler: h, Public: public}
}

// IsPublic reports whether the method bypasses authentication. Unknown
// methods are considered private (so auth errors surface before -32601 when
// the caller is unauthenticated, per §5 conventions).
func (r *Router) IsPublic(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m, ok := r.methods[name]
	return ok && m.Public
}

// Dispatch routes a single request. Notifications return nil.
// Authentication is NOT enforced here; the Hub layers it on top.
func (r *Router) Dispatch(ctx any, req *Request) *Response {
	if req.IsNotification() {
		return nil
	}
	r.mu.RLock()
	m, ok := r.methods[req.Method]
	r.mu.RUnlock()
	if !ok {
		return ErrorResponse(req, ErrMethodNotFound(req.Method))
	}
	result, rerr := m.Handler(ctx, req.Params)
	if rerr != nil {
		return ErrorResponse(req, rerr)
	}
	return &Response{JSONRPC: "2.0", ID: req.ID, Result: result}
}

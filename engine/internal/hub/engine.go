// Package hub implements the Control Hub core engine: JSON-RPC method
// handlers, session/auth, node registry, commands, tasks and the event bus.
// It is the single source of truth for protocol behaviour (PROTOCOL.md).
package hub

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"musubi/engine/internal/jsonrpc"
)

// Version is the hubd server version reported in server_info and /v1/health.
const Version = "1.0.0"

// tsLayout formats timestamps as ISO-8601 UTC with millisecond precision
// (PROTOCOL.md §1: all timestamps use this format).
const tsLayout = "2006-01-02T15:04:05.000Z"

// heartbeatInterval drives the node watchdog: real nodes are marked offline
// after missing 3 consecutive intervals. Demo nodes never expire.
const heartbeatInterval = 5 * time.Second

// Config configures a Hub.
type Config struct {
	AuthMode   string // none | token | basic
	Token      string // static token for token mode
	User       string // username for basic mode
	Pass       string // password for basic mode
	DemoNodes  int    // number of simulated nodes n-1..n-N (default 3)
	Logger     *slog.Logger
	AllowShell bool   // allow shell method execution on demo nodes
	AllowOrigin []string // allowed WS origins; if empty, allow absent origin, check present against Host/scheme
}

// CallCtx carries per-call state into method handlers: the resolved auth
// session and, for WebSocket connections, a callback to track subscriptions
// created on that connection (so they can be closed on disconnect).
type CallCtx struct {
	Session  *Session
	TrackSub func(id string)
}

// Hub is the engine root: router, auth, node registry, commands, tasks, events.
type Hub struct {
	cfg    Config
	log    *slog.Logger
	router *jsonrpc.Router

	auth   *authManager
	nodes  *nodeRegistry
	cmds   *commandRegistry
	tasks  *taskRegistry
	events *eventBus

	cmdSeq  atomic.Int64
	taskSeq atomic.Int64
	subSeq  atomic.Int64
	nodeSeq atomic.Int64

	startOnce sync.Once
	stopCh    chan struct{}
}

// NewHub builds a Hub, registers all methods and demo nodes.
func NewHub(cfg Config) *Hub {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.DemoNodes < 0 {
		cfg.DemoNodes = 0
	}
	h := &Hub{
		cfg:    cfg,
		log:    cfg.Logger,
		router: jsonrpc.NewRouter(),
		stopCh: make(chan struct{}),
	}
	h.nodeSeq.Store(int64(cfg.DemoNodes))
	h.auth = newAuthManager(cfg)
	h.events = newEventBus(cfg.Logger)
	h.nodes = newNodeRegistry(h)
	h.cmds = newCommandRegistry(h)
	h.tasks = newTaskRegistry(h)
	h.registerMethods()
	h.registerDemoNodes()
	return h
}

// reg registers one JSON-RPC method. Handlers receive a decoded params object
// (unknown fields ignored) and the per-call context.
func (h *Hub) reg(name string, public bool, fn func(ctx *CallCtx, p map[string]any) (any, *jsonrpc.Error)) {
	h.router.Register(name, func(ctx any, raw json.RawMessage) (any, *jsonrpc.Error) {
		cc, _ := ctx.(*CallCtx)
		return fn(cc, decodeParams(raw))
	}, public)
}

// registerMethods wires the PROTOCOL.md §5 method table (plus the additive
// extensions node.heartbeat / node.unregister for real-node lifecycle).
func (h *Hub) registerMethods() {
	// engine.* never require authentication (§8.5).
	h.reg("engine.info", true, h.engineInfo)
	h.reg("engine.ping", true, h.enginePing)
	h.reg("engine.capabilities", true, h.engineCapabilities)
	// auth: login/refresh are the entry points, logout needs a session.
	h.reg("auth.login", true, h.authLogin)
	h.reg("auth.refresh", true, h.authRefresh)
	h.reg("auth.logout", false, h.authLogout)
	// nodes.
	h.reg("node.list", false, h.nodeList)
	h.reg("node.get", false, h.nodeGet)
	h.reg("node.heartbeat", false, h.nodeHeartbeat)
	h.reg("node.unregister", false, h.nodeUnregister)
	h.reg("node.subscribe", false, h.nodeSubscribe)
	h.reg("node.unsubscribe", false, h.nodeUnsubscribe)
	// commands.
	h.reg("command.send", false, h.cmdSend)
	h.reg("command.batch", false, h.cmdBatch)
	h.reg("command.cancel", false, h.cmdCancel)
	// tasks.
	h.reg("task.create", false, h.taskCreate)
	h.reg("task.get", false, h.taskGet)
	h.reg("task.list", false, h.taskList)
	h.reg("task.cancel", false, h.taskCancel)
	h.reg("task.subscribe", false, h.taskSubscribe)
	// events.
	h.reg("event.subscribe", false, h.eventSubscribe)
	h.reg("event.unsubscribe", false, h.eventUnsubscribe)
}

// registerDemoNodes creates the always-online simulated nodes n-1..n-N.
func (h *Hub) registerDemoNodes() {
	for i := 1; i <= h.cfg.DemoNodes; i++ {
		id := fmt.Sprintf("n-%d", i)
		meta := map[string]any{"kind": "demo", "host": fmt.Sprintf("demo-%d", i)}
		h.nodes.register(RegisterRequest{
			ID:           id,
			Name:         fmt.Sprintf("worker-%d", i),
			Capabilities: &[]string{"shell.exec", "file.read"},
			Tags:         []string{"linux", "x64", "demo"},
			Metadata:     meta,
		}, true)
	}
}

// Start launches background loops (node heartbeat watchdog, idempotency TTL
// cleanup). The context cancels them.
func (h *Hub) Start(ctx context.Context) {
	h.startOnce.Do(func() {
		go h.watchdogLoop(ctx)
	})
}

func (h *Hub) watchdogLoop(ctx context.Context) {
	t := time.NewTicker(heartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			h.nodes.watchdogTick(heartbeatInterval)
			h.cmds.cleanupIdem()
		}
	}
}

// HandleRaw parses a JSON-RPC message (single request or batch, PROTOCOL.md
// §2.5), resolves authentication and returns the marshalled response. It
// returns nil when there is nothing to reply (all notifications).
func (h *Hub) HandleRaw(data []byte, authHeader string) []byte {
	return h.handleRaw(data, authHeader, nil)
}

// HandleRawCtx is HandleRaw with an explicit per-connection CallCtx (used by
// the WebSocket transport to track subscriptions for cleanup on close).
func (h *Hub) HandleRawCtx(data []byte, authHeader string, cc *CallCtx) []byte {
	return h.handleRaw(data, authHeader, cc)
}

func (h *Hub) handleRaw(data []byte, authHeader string, cc *CallCtx) []byte {
	batch, parsed, fatal := jsonrpc.ParseMessage(data)
	if fatal != nil {
		out, _ := json.Marshal(jsonrpc.ErrorResponse(nil, fatal))
		return out
	}
	var resp []*jsonrpc.Response
	for _, p := range parsed {
		if p.Err != nil {
			resp = append(resp, jsonrpc.ErrorResponse(p.Req, p.Err))
			continue
		}
		req := p.Req
		if req.IsNotification() {
			continue
		}
		sess, aerr := h.resolveAuth(authHeader, req)
		if aerr != nil && !h.router.IsPublic(req.Method) {
			// Public methods never fail auth: run them anonymously (§8.5).
			resp = append(resp, jsonrpc.ErrorResponse(req, aerr))
			continue
		}
		if aerr != nil {
			sess = &Session{Anonymous: true}
		}
		var trackSub func(string)
		if cc != nil {
			trackSub = cc.TrackSub
		}
		r := h.router.Dispatch(&CallCtx{Session: sess, TrackSub: trackSub}, req)
		if r != nil {
			resp = append(resp, r)
		}
	}
	if len(resp) == 0 {
		return nil
	}
	if batch {
		out, _ := json.Marshal(resp)
		return out
	}
	out, _ := json.Marshal(resp[0])
	return out
}

// resolveAuth resolves the session for one request: the Authorization header
// wins, then params.auth_token, then anonymous (or -32001 in token/basic
// modes).
func (h *Hub) resolveAuth(authHeader string, req *jsonrpc.Request) (*Session, *jsonrpc.Error) {
	if strings.TrimSpace(authHeader) != "" {
		return h.ResolveHeaderAuth(authHeader)
	}
	if p := decodeParams(req.Params); p != nil {
		if token := strParam(p["auth_token"]); token != "" {
			return h.auth.authenticate(token)
		}
	}
	return h.auth.authenticate("")
}

// ResolveHeaderAuth resolves auth from a raw Authorization header
// (Bearer <token> or Basic base64(user:pass)).
func (h *Hub) ResolveHeaderAuth(authHeader string) (*Session, *jsonrpc.Error) {
	if user, pass, ok := parseBasicAuth(authHeader); ok {
		if sess, ok2 := h.auth.authenticateBasic(user, pass); ok2 {
			return sess, nil
		}
	}
	token := strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer "))
	return h.auth.authenticate(token)
}

// AuthenticateToken resolves a bearer token from a query param (?token=).
func (h *Hub) AuthenticateToken(token string) (*Session, *jsonrpc.Error) {
	return h.auth.authenticate(token)
}

// AuthMode returns the configured auth mode.
func (h *Hub) AuthMode() string {
	return h.cfg.AuthMode
}

// Health returns the /v1/health payload.
func (h *Hub) Health() map[string]any {
	return map[string]any{"ok": true, "version": Version, "time": nowTS()}
}

// ---------------------------------------------------------------------------
// Shared helpers

func nowTS() string {
	return time.Now().UTC().Format(tsLayout)
}

func strPtr(s string) *string { return &s }

// decodeParams decodes raw params into an object. json.Number preserves
// integer literals so echo round-trips exactly.
func decodeParams(raw json.RawMessage) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil
	}
	return m
}

func strParam(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// intParam reads an integer param, tolerating json.Number/float64/int.
func intParam(v any, def int) int {
	switch n := v.(type) {
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return int(i)
		}
	case float64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	}
	return def
}

// parseBasicAuth decodes an HTTP Basic Authorization header.
func parseBasicAuth(header string) (user, pass string, ok bool) {
	const prefix = "Basic "
	if len(header) < len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return "", "", false
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(header[len(prefix):]))
	if err != nil {
		return "", "", false
	}
	user, pass, ok = strings.Cut(string(raw), ":")
	return user, pass, ok
}

// clampPage normalises limit/offset for list methods (default 100, max 1000).
func clampPage(limit, offset int) (int, int) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}

// makeSubscription validates event types and creates a subscription, tracking
// it on the calling connection when a tracker is present.
func (h *Hub) makeSubscription(cc *CallCtx, eventTypes []string, nodeID, taskID string) (map[string]any, *jsonrpc.Error) {
	for _, t := range eventTypes {
		if !knownEventTypes[t] {
			return nil, jsonrpc.NewError(jsonrpc.CodeEventTypeNS, "event.type.not_supported",
				map[string]any{"event_type": t})
		}
	}
	id := h.newSubID()
	h.events.newSub(id, eventTypes, nodeID, taskID)
	if cc != nil && cc.TrackSub != nil {
		cc.TrackSub(id)
	}
	return map[string]any{"subscription_id": id}, nil
}

func (h *Hub) newSubID() string {
	return fmt.Sprintf("sub-%d", h.subSeq.Add(1))
}

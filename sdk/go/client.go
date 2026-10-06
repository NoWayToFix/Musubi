package sdk

// HubClient is a JSON-RPC 2.0 client for the Control Hub (PROTOCOL.md).
//
// Transport selection: "ws" uses the WebSocket endpoint for calls and events,
// "http" uses HTTP POST /v1/rpc for calls plus SSE / long-poll for events,
// "poll" forces long-poll, "auto" negotiates via engine.info using the
// protocol fallback order ws -> sse -> poll.
//
// Compatibility: unknown fields in responses/events are ignoredchers, missing
// fields are tolerated, and unknown event types are delivered as-is
// (consumers must ignore them per PROTOCOL.md §4.5).

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultURL = "http://127.0.0.1:8080"
	protoVer   = "1.0"

	offlineQueueLimit = 1000
	eventDedupWindow  = 5000
	wsHeartbeatEvery  = 30 * time.Second
	backoffMax        = 30 * time.Second
)

// Options configures a HubClient.
type Options struct {
	URL           string        // hub base URL, e.g. http://127.0.0.1:8080 (default)
	Token         string        // optional static bearer token
	Transport     string        // "auto" | "ws" | "http" | "poll" (default "auto")
	Timeout       time.Duration // per-request timeout (default 30s)
	MaxRetries    int           // reconnect retries; <0 = unlimited (default -1)
	ReconnectBase time.Duration // backoff base (default 1s; factor 2, max 30s, ±20% jitter)
	Logger        *log.Logger   // optional debug logger
}

// HubClient talks to the hub over JSON-RPC 2.0.
type HubClient struct {
	opts Options

	mu        sync.Mutex
	idSeq     int64
	baseURL   *url.URL
	transport string // resolved: "ws" or "http"
	server    *ServerInfo
	token     string
	closed    bool

	// WebSocket state (transport == "ws").
	ws            *WSConn
	wsLoopStarted bool

	// Pending WS calls by request id.
	pending map[int64]chan *rpcResponse

	// Subscriptions and their background event loops.
	subs    map[string]*Subscription
	poller  *pollLoop // long-poll loop
	sseLoop *sseLoop  // SSE loop

	// Offline write queue (PROTOCOL.md §6.2). Optional best-effort replay.
	offline []map[string]any
}

type rpcResponse struct {
	Result json.RawMessage `json:"result"`
	Error  *HubError       `json:"error"`
	Err    error           `json:"-"`
}

// New creates a HubClient. No network I/O happens until the first call.
func New(opts Options) *HubClient {
	if opts.URL == "" {
		opts.URL = defaultURL
	}
	if opts.Transport == "" {
		opts.Transport = "auto"
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 30 * time.Second
	}
	if opts.ReconnectBase <= 0 {
		opts.ReconnectBase = time.Second
	}
	if opts.MaxRetries == 0 {
		opts.MaxRetries = -1 // unlimited by default
	}
	base, err := url.Parse(opts.URL)
	if err != nil {
		base = &url.URL{Scheme: "http", Host: "127.0.0.1:8080"}
	}
	c := &HubClient{
		opts:      opts,
		baseURL:   base,
		token:     opts.Token,
		transport: "http", // default; resolved in resolveTransport()
		pending:   make(map[int64]chan *rpcResponse),
		subs:      make(map[string]*Subscription),
	}
	if opts.Transport == "ws" {
		c.transport = "ws"
	}
	return c
}

// nextID returns a monotonically increasing request id.
func (c *HubClient) nextID() int64 { return atomic.AddInt64(&c.idSeq, 1) }

func (c *HubClient) logf(format string, args ...any) {
	if c.opts.Logger != nil {
		c.opts.Logger.Printf(format, args...)
	}
}

// ---------------------------------------------------------------------------
// Transport helpers
// ---------------------------------------------------------------------------

// rpcURL is the HTTP POST endpoint (/v1/rpc).
func (c *HubClient) rpcURL() string {
	u := *c.baseURL
	u.Path = strings.TrimSuffix(u.Path, "/") + "/v1/rpc"
	return u.String()
}

// wsURL is the WebSocket endpoint (ws[s]://host/v1/ws).
func (c *HubClient) wsURL() string {
	u := *c.baseURL
	switch u.Scheme {
	case "https":
		u.Scheme = "wss"
	default:
		u.Scheme = "ws"
	}
	u.Path = strings.TrimSuffix(u.Path, "/") + "/v1/ws"
	return u.String()
}

// authHeaders returns headers with the bearer token attached.
func (c *HubClient) authHeaders() http.Header {
	h := http.Header{}
	if c.token != "" {
		h.Set("Authorization", "Bearer "+c.token)
	}
	return h
}

// resolveTransport negotiates the transport using engine.info over HTTP POST
// (engine.info never requires auth, PROTOCOL.md §8.5). "auto" uses the
// protocol fallback order ws -> sse -> poll.
//
// It is idempotent and also runs when the caller already fetched server info
// explicitly (e.g. an app calling Info first): the preference is computed from
// the advertised transports instead of being skipped.
func (c *HubClient) resolveTransport(ctx context.Context) error {
	switch c.opts.Transport {
	case "http", "poll", "ws":
		return nil // forced transports need no negotiation
	}
	c.mu.Lock()
	si := c.server
	c.mu.Unlock()

	if si == nil {
		res, err := c.callHTTP(ctx, "engine.info", map[string]any{})
		if err != nil {
			return err
		}
		var s ServerInfo
		if err := remap(res, &s); err != nil {
			return err
		}
		si = &s
		c.mu.Lock()
		c.server = si
		c.mu.Unlock()
	}

	supports := mapList(si.Transports)
	c.mu.Lock()
	switch {
	case supports["ws"]:
		c.transport = "ws"
	case supports["sse"]:
		c.transport = "http" // sse used for events
	default:
		c.transport = "http" // poll used for events
	}
	c.mu.Unlock()
	return nil
}

func mapList(xs []string) map[string]bool {
	m := make(map[string]bool, len(xs))
	for _, x := range xs {
		m[x] = true
	}
	return m
}

// remap decodes src (already a decoded JSON object) into dst struct.
func remap(src map[string]any, dst any) error {
	b, err := json.Marshal(src)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, dst)
}

// ---------------------------------------------------------------------------
// Call
// ---------------------------------------------------------------------------

// Call performs a JSON-RPC request. It returns the result object on success
// or a *HubError (inspect with errors.As) on JSON-RPC error.
func (c *HubClient) Call(ctx context.Context, method string, params map[string]any) (map[string]any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := c.resolveTransport(ctx); err != nil {
		return nil, err
	}

	if c.opts.Transport == "ws" || c.transport == "ws" {
		res, err := c.callWS(ctx, method, params)
		if err == nil {
			return res, nil
		}
		// WS failed; fall back to HTTP POST (transport degradation per §6.1).
		c.logf("ws call %s failed: %v; falling back to http", method, err)
	}
	return c.callHTTP(ctx, method, params)
}

func (c *HubClient) callHTTP(ctx context.Context, method string, params map[string]any) (map[string]any, error) {
	id := c.nextID()
	req := map[string]any{"jsonrpc": "2.0", "id": id, "method": method}
	if params != nil {
		req["params"] = params
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.rpcURL(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.token)
	}

	timeout := c.opts.Timeout
	if deadline, ok := ctx.Deadline(); ok {
		if d := time.Until(deadline); d < timeout {
			timeout = d
		}
	}
	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var env rpcResponse
	dec := json.NewDecoder(io.LimitReader(resp.Body, 64<<20))
	if err := dec.Decode(&env); err != nil {
		return nil, fmt.Errorf("hub: decode response: %w", err)
	}
	if env.Error != nil {
		return nil, env.Error
	}
	var out map[string]any
	if len(env.Result) > 0 && string(env.Result) != "null" {
		if err := json.Unmarshal(env.Result, &out); err != nil {
			return nil, fmt.Errorf("hub: decode result: %w", err)
		}
	}
	return out, nil
}

// callWS sends the request over the WebSocket and waits for the matching id.
func (c *HubClient) callWS(ctx context.Context, method string, params map[string]any) (map[string]any, error) {
	if err := c.ensureWS(); err != nil {
		return nil, err
	}
	id := c.nextID()
	req := map[string]any{"jsonrpc": "2.0", "id": id, "method": method}
	if params != nil {
		req["params"] = params
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	ws := c.ws
	ch := make(chan *rpcResponse, 1)
	c.pending[id] = ch
	c.mu.Unlock()

	if err := ws.WriteText(body); err != nil {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, err
	}

	select {
	case env := <-ch:
		if env.Err != nil {
			return nil, env.Err
		}
		if env.Error != nil {
			return nil, env.Error
		}
		var out map[string]any
		if len(env.Result) > 0 && string(env.Result) != "null" {
			if err := json.Unmarshal(env.Result, &out); err != nil {
				return nil, err
			}
		}
		return out, nil
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, ctx.Err()
	}
}

// ensureWS starts the WebSocket read loop if needed and waits until a
// connection is established.
func (c *HubClient) ensureWS() error {
	c.mu.Lock()
	if !c.wsLoopStarted {
		c.wsLoopStarted = true
		go c.wsReadLoop()
	}
	ws := c.ws
	c.mu.Unlock()
	if ws != nil {
		return nil
	}
	deadline := time.Now().Add(c.opts.Timeout)
	for {
		c.mu.Lock()
		ws = c.ws
		closed := c.closed
		c.mu.Unlock()
		if closed {
			return errors.New("hub: client closed")
		}
		if ws != nil {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("hub: websocket not connected")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// wsReadLoop owns the WebSocket lifecycle: dial, read, reconnect with
// exponential backoff, and re-subscribe all active subscriptions on reconnect
// (PROTOCOL.md §6.2).
func (c *HubClient) wsReadLoop() {
	retries := 0
	for {
		if c.isClosed() {
			return
		}
		ws, err := c.dialWS()
		if err != nil {
			if !c.backoffWait(retries) {
				return
			}
			retries++
			continue
		}
		retries = 0
		go c.wsHeartbeat(ws)

		// Re-subscribe active subscriptions on (re)connect.
		for _, s := range c.snapshotSubs() {
			s.resubscribe()
		}

		for {
			_ = ws.SetReadDeadline(time.Now().Add(wsHeartbeatEvery + 30*time.Second))
			msg, err := ws.ReadMessage()
			if err != nil {
				c.failPending(err)
				c.closeWS(ws)
				break
			}
			c.handleWSMessage(msg)
		}
	}
}

// dialWS dials the WS endpoint and registers it as the active connection.
func (c *HubClient) dialWS() (*WSConn, error) {
	ws, err := DialWS(c.wsURL(), c.authHeaders(), c.opts.Timeout)
	if err != nil {
		c.logf("ws dial failed: %v", err)
		return nil, err
	}
	c.mu.Lock()
	c.ws = ws
	c.mu.Unlock()
	return ws, nil
}

func (c *HubClient) closeWS(ws *WSConn) {
	c.mu.Lock()
	if c.ws == ws {
		c.ws = nil
	}
	c.mu.Unlock()
	_ = ws.Close()
}

// failPending fails all in-flight WS calls with err.
func (c *HubClient) failPending(err error) {
	c.mu.Lock()
	for id, ch := range c.pending {
		delete(c.pending, id)
		select {
		case ch <- &rpcResponse{Err: err}:
		default:
		}
	}
	c.mu.Unlock()
}

func (c *HubClient) snapshotSubs() []*Subscription {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*Subscription, 0, len(c.subs))
	for _, s := range c.subs {
		out = append(out, s)
	}
	return out
}

// handleWSMessage processes one WS message: a response (has "id") or a
// notification (method == "event").
func (c *HubClient) handleWSMessage(msg []byte) {
	var env map[string]json.RawMessage
	if err := json.Unmarshal(msg, &env); err != nil {
		return // ignore garbage frames
	}
	if idRaw, ok := env["id"]; ok && idRaw != nil {
		// Parse numeric or string ids; our own requests use numeric ids, but
		// tolerate any id form the server may echo (PROTOCOL.md §2.1).
		var idAny any
		if err := json.Unmarshal(idRaw, &idAny); err != nil {
			return
		}
		var id int64
		switch v := idAny.(type) {
		case float64:
			id = int64(v)
		case string:
			id, _ = strconv.ParseInt(v, 10, 64)
		default:
			return
		}
		var rpc rpcResponse
		if res, ok := env["result"]; ok {
			rpc.Result = res
		}
		if e, ok := env["error"]; ok && string(e) != "null" {
			_ = json.Unmarshal(e, &rpc.Error)
		}
		c.mu.Lock()
		if ch, ok := c.pending[id]; ok {
			delete(c.pending, id)
			select {
			case ch <- &rpc:
			default:
			}
		}
		c.mu.Unlock()
		return
	}
	c.handleNotification(env)
}

func (c *HubClient) wsHeartbeat(ws *WSConn) {
	t := time.NewTicker(wsHeartbeatEvery)
	defer t.Stop()
	for range t.C {
		c.mu.Lock()
		alive := c.ws == ws
		c.mu.Unlock()
		if !alive || c.isClosed() {
			return
		}
		if err := ws.Ping([]byte("ping")); err != nil {
			c.closeWS(ws)
			return
		}
	}
}

// handleNotification processes a JSON-RPC notification (method == "event").
func (c *HubClient) handleNotification(env map[string]json.RawMessage) {
	method := ""
	_ = json.Unmarshal(env["method"], &method)
	if method != "event" {
		return // ignore unknown notifications
	}
	var params struct {
		SubscriptionID string    `json:"subscription_id"`
		Event          *HubEvent `json:"event"`
	}
	if err := json.Unmarshal(env["params"], &params); err != nil || params.Event == nil {
		return
	}
	c.dispatchEvent(params.SubscriptionID, params.Event)
}

// dispatchEvent delivers an event to the matching subscription. Unknown event
// types are still delivered; consumers ignore them per PROTOCOL.md §4.5.
//
// The subscription_id is used when it identifies a local subscription. Some
// engines stream SSE with a transport-owned subscription id that does not match
// the id returned by event.subscribe, so an unknown id falls back to fan-out
// over every local subscription whose filters match. That keeps the SDK working
// against any conforming engine instead of silently receiving nothing.
func (c *HubClient) dispatchEvent(subID string, ev *HubEvent) {
	c.mu.Lock()
	s, ok := c.subs[subID]
	if ok {
		c.mu.Unlock()
		c.pushEvent(s, ev)
		return
	}
	subs := make([]*Subscription, 0, len(c.subs))
	for _, s := range c.subs {
		subs = append(subs, s)
	}
	c.mu.Unlock()
	for _, s := range subs {
		c.pushEvent(s, ev)
	}
}

// pushEvent hands the event to one subscription, dropping it when the consumer
// is slow so the read loop is never blocked.
func (c *HubClient) pushEvent(s *Subscription, ev *HubEvent) {
	if !s.matches(ev) {
		return
	}
	select {
	case s.events <- *ev:
	default: // slow consumer: drop (best-effort delivery, PROTOCOL.md §6.2)
	}
}

func (c *HubClient) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// backoffWait sleeps with exponential backoff; returns false when the retry
// budget is exhausted. n is the number of retries already performed.
func (c *HubClient) backoffWait(n int) bool {
	if c.opts.MaxRetries >= 0 && n >= c.opts.MaxRetries {
		return false
	}
	d := c.opts.ReconnectBase
	for i := 0; i < n && d < backoffMax; i++ {
		d *= 2
	}
	if d > backoffMax {
		d = backoffMax
	}
	// ±20% jitter
	j := d / 5
	d += time.Duration(rand.Int63n(int64(2*j))) - j
	if d < 0 {
		d = c.opts.ReconnectBase
	}
	time.Sleep(d)
	return true
}

// ---------------------------------------------------------------------------
// engine.*
// ---------------------------------------------------------------------------

// Info performs version negotiation and returns the negotiated ServerInfo.
// It returns a *HubError with code -32005 when there is no version overlap.
func (c *HubClient) Info(ctx context.Context, clientInfo *ClientInfo) (*ServerInfo, error) {
	if clientInfo == nil {
		clientInfo = &ClientInfo{}
	}
	if len(clientInfo.ProtocolVersions) == 0 {
		clientInfo.ProtocolVersions = []string{protoVer}
	}
	params := map[string]any{"client_info": clientInfo}
	res, err := c.Call(ctx, "engine.info", params)
	if err != nil {
		return nil, err
	}
	var si ServerInfo
	if err := remap(res, &si); err != nil {
		return nil, err
	}
	if !negotiateVersion(&si, clientInfo.ProtocolVersions) {
		return nil, &HubError{
			Code:    ErrProtocolVersionMismatch,
			Message: "protocol.version.mismatch",
			Data: map[string]any{
				"supported": si.SupportedVersions,
				"client":    clientInfo.ProtocolVersions,
			},
		}
	}
	c.mu.Lock()
	c.server = &si
	c.mu.Unlock()
	return &si, nil
}

// negotiateVersion computes the highest common protocol version. Returns false
// when the intersection is empty. Omitting protocol_versions means ["1.0"].
func negotiateVersion(si *ServerInfo, clientVersions []string) bool {
	supported := si.SupportedVersions
	if len(supported) == 0 {
		supported = []string{protoVer}
	}
	best := ""
	for _, sv := range supported {
		for _, cv := range clientVersions {
			if sv == cv && versionNewer(sv, best) {
				best = sv
			}
		}
	}
	if best == "" {
		return false
	}
	si.ProtocolVersion = best
	return true
}

func versionNewer(a, b string) bool {
	if b == "" {
		return true
	}
	ai, aj := splitVersion(a)
	bi, bj := splitVersion(b)
	if ai != bi {
		return ai > bi
	}
	return aj > bj
}

func splitVersion(v string) (int, int) {
	parts := strings.SplitN(v, ".", 2)
	mi, _ := strconv.Atoi(parts[0])
	mi2 := 0
	if len(parts) > 1 {
		mi2, _ = strconv.Atoi(parts[1])
	}
	return mi, mi2
}

// Ping calls engine.ping.
func (c *HubClient) Ping(ctx context.Context) (map[string]any, error) {
	return c.Call(ctx, "engine.ping", map[string]any{})
}

// Capabilities calls engine.capabilities and returns the capability list.
func (c *HubClient) Capabilities(ctx context.Context) ([]Capability, map[string]any, error) {
	res, err := c.Call(ctx, "engine.capabilities", map[string]any{})
	if err != nil {
		return nil, nil, err
	}
	var caps []Capability
	if raw, ok := res["capabilities"].([]any); ok {
		b, _ := json.Marshal(raw)
		_ = json.Unmarshal(b, &caps)
	}
	features, _ := res["features"].(map[string]any)
	return caps, features, nil
}

// ---------------------------------------------------------------------------
// auth.*
// ---------------------------------------------------------------------------

// Login authenticates with the given params (PROTOCOL.md §3.5):
// token: {"token": "..."} / basic: {"username": "...", "password": "..."}.
func (c *HubClient) Login(ctx context.Context, params map[string]any) (*AuthResult, error) {
	if params == nil {
		params = map[string]any{}
	}
	res, err := c.Call(ctx, "auth.login", params)
	if err != nil {
		return nil, err
	}
	var ar AuthResult
	if err := remap(res, &ar); err != nil {
		return nil, err
	}
	if ar.AccessToken != "" {
		c.mu.Lock()
		c.token = ar.AccessToken
		c.mu.Unlock()
	}
	return &ar, nil
}

// LoginToken logs in with a static token.
func (c *HubClient) LoginToken(ctx context.Context, token string) (*AuthResult, error) {
	return c.Login(ctx, map[string]any{"token": token})
}

// LoginBasic logs in with a username/password pair.
func (c *HubClient) LoginBasic(ctx context.Context, username, password string) (*AuthResult, error) {
	return c.Login(ctx, map[string]any{"username": username, "password": password})
}

// Refresh exchanges a refresh token for a new access token.
func (c *HubClient) Refresh(ctx context.Context, refreshToken string) (*AuthResult, error) {
	res, err := c.Call(ctx, "auth.refresh", map[string]any{"refresh_token": refreshToken})
	if err != nil {
		return nil, err
	}
	var ar AuthResult
	if err := remap(res, &ar); err != nil {
		return nil, err
	}
	if ar.AccessToken != "" {
		c.mu.Lock()
		c.token = ar.AccessToken
		c.mu.Unlock()
	}
	return &ar, nil
}

// Logout revokes the current token.
func (c *HubClient) Logout(ctx context.Context) error {
	_, err := c.Call(ctx, "auth.logout", map[string]any{})
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.token = ""
	c.mu.Unlock()
	return nil
}

// ---------------------------------------------------------------------------
// node.*
// ---------------------------------------------------------------------------

// NodeList lists nodes (filter: {status?, tags?}, limit/offset optional).
func (c *HubClient) NodeList(ctx context.Context, filter map[string]any, limit, offset int) (*NodeListResult, error) {
	params := map[string]any{}
	if filter != nil {
		params["filter"] = filter
	}
	if limit > 0 {
		params["limit"] = limit
	}
	if offset > 0 {
		params["offset"] = offset
	}
	res, err := c.Call(ctx, "node.list", params)
	if err != nil {
		return nil, err
	}
	var out NodeListResult
	if err := remap(res, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// NodeGet fetches one node by id.
func (c *HubClient) NodeGet(ctx context.Context, nodeID string) (*Node, error) {
	res, err := c.Call(ctx, "node.get", map[string]any{"node_id": nodeID})
	if err != nil {
		return nil, err
	}
	var out struct {
		Node *Node `json:"node"`
	}
	if err := remap(res, &out); err != nil {
		return nil, err
	}
	return out.Node, nil
}

// ---------------------------------------------------------------------------
// command.*
// ---------------------------------------------------------------------------

// CommandSend sends a single command to a node.
func (c *HubClient) CommandSend(ctx context.Context, spec CommandSpec) (*Command, error) {
	res, err := c.Call(ctx, "command.send", specParams(spec))
	if err != nil {
		return nil, err
	}
	var out struct {
		Command *Command `json:"command"`
	}
	if err := remap(res, &out); err != nil {
		return nil, err
	}
	return out.Command, nil
}

// CommandBatch sends commands sequentially in one request.
func (c *HubClient) CommandBatch(ctx context.Context, commands []CommandSpec, idempotencyKey string) (*CommandBatchResult, error) {
	params := map[string]any{"commands": commands}
	if idempotencyKey != "" {
		params["idempotency_key"] = idempotencyKey
	}
	res, err := c.Call(ctx, "command.batch", params)
	if err != nil {
		return nil, err
	}
	var out CommandBatchResult
	if err := remap(res, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// CommandCancel cancels a pending/running command.
func (c *HubClient) CommandCancel(ctx context.Context, commandID string) (*Command, error) {
	res, err := c.Call(ctx, "command.cancel", map[string]any{"command_id": commandID})
	if err != nil {
		return nil, err
	}
	var out struct {
		Command *Command `json:"command"`
	}
	if err := remap(res, &out); err != nil {
		return nil, err
	}
	return out.Command, nil
}

func specParams(spec CommandSpec) map[string]any {
	p := map[string]any{"node_id": spec.NodeID, "method": spec.Method}
	if spec.Params != nil {
		p["params"] = spec.Params
	}
	if spec.Timeout > 0 {
		p["timeout"] = spec.Timeout
	}
	if spec.IdempotencyKey != "" {
		p["idempotency_key"] = spec.IdempotencyKey
	}
	return p
}

// ---------------------------------------------------------------------------
// task.*
// ---------------------------------------------------------------------------

// TaskCreate creates and immediately starts a task.
func (c *HubClient) TaskCreate(ctx context.Context, spec TaskSpec) (*Task, error) {
	res, err := c.Call(ctx, "task.create", map[string]any{
		"name":     spec.Name,
		"commands": spec.Commands,
		"parallel": spec.Parallel,
	})
	if err != nil {
		return nil, err
	}
	var out struct {
		Task *Task `json:"task"`
	}
	if err := remap(res, &out); err != nil {
		return nil, err
	}
	return out.Task, nil
}

// TaskGet fetches one task by id.
func (c *HubClient) TaskGet(ctx context.Context, taskID string) (*Task, error) {
	res, err := c.Call(ctx, "task.get", map[string]any{"task_id": taskID})
	if err != nil {
		return nil, err
	}
	var out struct {
		Task *Task `json:"task"`
	}
	if err := remap(res, &out); err != nil {
		return nil, err
	}
	return out.Task, nil
}

// TaskList lists tasks (filter: {status?}, limit/offset optional).
func (c *HubClient) TaskList(ctx context.Context, filter map[string]any, limit, offset int) (*TaskListResult, error) {
	params := map[string]any{}
	if filter != nil {
		params["filter"] = filter
	}
	if limit > 0 {
		params["limit"] = limit
	}
	if offset > 0 {
		params["offset"] = offset
	}
	res, err := c.Call(ctx, "task.list", params)
	if err != nil {
		return nil, err
	}
	var out TaskListResult
	if err := remap(res, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// TaskCancel cancels a running task.
func (c *HubClient) TaskCancel(ctx context.Context, taskID string) (*Task, error) {
	res, err := c.Call(ctx, "task.cancel", map[string]any{"task_id": taskID})
	if err != nil {
		return nil, err
	}
	var out struct {
		Task *Task `json:"task"`
	}
	if err := remap(res, &out); err != nil {
		return nil, err
	}
	return out.Task, nil
}

// ---------------------------------------------------------------------------
// events / subscriptions
// ---------------------------------------------------------------------------

// EventSubscribe subscribes to events matching filters (nil = all events).
func (c *HubClient) EventSubscribe(ctx context.Context, filters *EventFilters) (*Subscription, error) {
	params := map[string]any{}
	if filters != nil {
		if len(filters.EventTypes) > 0 {
			params["event_types"] = filters.EventTypes
		}
		if filters.NodeID != "" {
			params["node_id"] = filters.NodeID
		}
		if filters.TaskID != "" {
			params["task_id"] = filters.TaskID
		}
	}

	sub := &Subscription{
		filters: filters,
		events:  make(chan HubEvent, 256),
		done:    make(chan struct{}),
		c:       c,
	}
	useWS := c.opts.Transport == "ws" || c.transport == "ws"
	useSSE := !useWS && c.opts.Transport != "poll" && serverHasFeature(c.server, "sse")

	if useWS {
		// Register a placeholder first so events published while the RPC is in
		// flight are already fanned out to this consumer.
		pending := "pending-" + strconv.FormatInt(c.nextID(), 10)
		c.mu.Lock()
		c.subs[pending] = sub
		c.mu.Unlock()
		res, err := c.Call(ctx, "event.subscribe", params)
		if err != nil {
			c.dropSub(pending)
			return nil, err
		}
		var out struct {
			SubscriptionID string `json:"subscription_id"`
		}
		if err := remap(res, &out); err != nil || out.SubscriptionID == "" {
			c.dropSub(pending)
			if err == nil {
				err = errors.New("hub: event.subscribe returned no subscription_id")
			}
			return nil, err
		}
		c.mu.Lock()
		delete(c.subs, pending)
		sub.ID = out.SubscriptionID
		c.subs[sub.ID] = sub
		c.mu.Unlock()
		// The WS read loop must run for notifications to arrive; if the
		// socket cannot be established, degrade to long-polling (PROTOCOL §7).
		go func() {
			if err := c.ensureWS(); err != nil {
				c.logf("ws unavailable for events: %v (falling back to long-poll)", err)
				c.startPoller()
			}
		}()
		return sub, nil
	}

	// SSE and long-poll deliver the whole event stream at transport level, so
	// no server-side subscription is created: an unread one would only fill
	// up and drop events. Filters are applied client-side instead.
	sub.local = true
	sub.ID = "local-" + strconv.FormatInt(c.nextID(), 10)
	c.mu.Lock()
	c.subs[sub.ID] = sub
	c.mu.Unlock()
	if useSSE {
		if err := c.startSSE(); err != nil {
			c.logf("sse unavailable: %v (falling back to long-poll)", err)
			c.startPoller()
		}
	} else {
		c.startPoller()
	}
	return sub, nil
}

// dropSub removes a subscription registered under a provisional id.
func (c *HubClient) dropSub(id string) {
	c.mu.Lock()
	delete(c.subs, id)
	c.mu.Unlock()
}

// serverHasFeature reports whether the negotiated server info advertises the
// given feature.
func serverHasFeature(si *ServerInfo, feat string) bool {
	if si == nil {
		return false
	}
	v, ok := si.Features[feat]
	if !ok {
		return false
	}
	b, ok := v.(bool)
	return ok && b
}

// Subscription is an active event subscription.
type Subscription struct {
	ID      string
	local   bool // transport-level stream: no server-side subscription to remove
	filters *EventFilters
	events  chan HubEvent
	done    chan struct{}
	c       *HubClient
	once    sync.Once
}

// Events returns the channel of received events.
func (s *Subscription) Events() <-chan HubEvent { return s.events }

// Done is closed when the subscription is closed. Consumers should select on
// it together with Events() so their read loop can shut down cleanly:
//
//	for {
//		select {
//		case <-ctx.Done():
//			return
//		case <-sub.Done():
//			return
//		case ev := <-sub.Events():
//			handle(ev)
//		}
//	}
func (s *Subscription) Done() <-chan struct{} { return s.done }

// matches applies the local filters to an event.
func (s *Subscription) matches(ev *HubEvent) bool {
	if s.filters == nil {
		return true
	}
	if len(s.filters.EventTypes) > 0 {
		found := false
		for _, t := range s.filters.EventTypes {
			if t == ev.Type {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if s.filters.NodeID != "" && ev.NodeID != s.filters.NodeID {
		return false
	}
	if s.filters.TaskID != "" && ev.TaskID != s.filters.TaskID {
		return false
	}
	return true
}

// resubscribe re-registers the subscription after a reconnect. A new
// subscription id may be returned; the Subscription keeps routing events.
func (s *Subscription) resubscribe() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := s.c.Call(ctx, "event.subscribe", s.filtersParams())
	if err != nil {
		s.c.logf("resubscribe %s failed: %v", s.ID, err)
		return
	}
	var out struct {
		SubscriptionID string `json:"subscription_id"`
	}
	if err := remap(res, &out); err != nil || out.SubscriptionID == "" {
		return
	}
	s.c.mu.Lock()
	delete(s.c.subs, s.ID)
	s.ID = out.SubscriptionID
	s.c.subs[s.ID] = s
	s.c.mu.Unlock()
}

func (s *Subscription) filtersParams() map[string]any {
	params := map[string]any{}
	if s.filters != nil {
		if len(s.filters.EventTypes) > 0 {
			params["event_types"] = s.filters.EventTypes
		}
		if s.filters.NodeID != "" {
			params["node_id"] = s.filters.NodeID
		}
		if s.filters.TaskID != "" {
			params["task_id"] = s.filters.TaskID
		}
	}
	return params
}

// Close unsubscribes and releases the subscription.
func (s *Subscription) Close() error {
	var err error
	s.once.Do(func() {
		select {
		case <-s.done:
		default:
			close(s.done)
		}
		if !s.local {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_, err = s.c.Call(ctx, "event.unsubscribe", map[string]any{"subscription_id": s.ID})
		}
		s.c.mu.Lock()
		delete(s.c.subs, s.ID)
		s.c.mu.Unlock()
	})
	return err
}

// ---------------------------------------------------------------------------
// long-poll loop
// ---------------------------------------------------------------------------

type pollLoop struct {
	c       *HubClient
	stop    chan struct{}
	cursor  int64
	seen    *dedup
	mu      sync.Mutex         // guards cancel below
	cancel  context.CancelFunc // in-flight long-poll request
	endOnce sync.Once
	started bool
}

// startPoller launches a long-poll loop for event delivery; it is idempotent
// and safely no-ops once the client is closed.
func (c *HubClient) startPoller() {
	c.mu.Lock()
	if c.closed || c.poller != nil {
		c.mu.Unlock()
		return
	}
	pl := &pollLoop{c: c, stop: make(chan struct{}), seen: newDedup()}
	c.poller = pl
	c.mu.Unlock()
	go pl.run()
}

// end cancels the in-flight poll and stops the loop.
func (pl *pollLoop) end() {
	pl.endOnce.Do(func() {
		close(pl.stop)
		pl.mu.Lock()
		if pl.cancel != nil {
			pl.cancel()
		}
		pl.mu.Unlock()
	})
}

// run polls /v1/poll with an increasing cursor, applying backoff on error.
// Events are dispatched to matching subscriptions (deduplicated by event.id).
func (pl *pollLoop) run() {
	retries := 0
	for {
		select {
		case <-pl.stop:
			return
		default:
		}
		events, cursor, err := pl.pollOnce(pl.cursor)
		if err != nil {
			if !pl.c.backoffWait(retries) {
				return
			}
			retries++
			continue
		}
		retries = 0
		if cursor > pl.cursor {
			pl.cursor = cursor
		}
		for _, ev := range events {
			if !pl.seen.add(ev.ID) {
				continue // dedup window (5000)
			}
			for _, s := range pl.c.snapshotSubs() {
				if s.matches(ev) {
					select {
					case s.events <- *ev:
					default:
					}
				}
			}
		}
	}
}

// pollOnce performs one long-poll request, registering its cancel func so the
// loop can be torn down promptly by end().
func (pl *pollLoop) pollOnce(cursor int64) ([]*HubEvent, int64, error) {
	c := pl.c
	u := *c.baseURL
	u.Path = strings.TrimSuffix(u.Path, "/") + "/v1/poll"
	q := u.Query()
	q.Set("cursor", strconv.FormatInt(cursor, 10))
	q.Set("timeout", "30")
	if c.token != "" {
		q.Set("token", c.token)
	}
	u.RawQuery = q.Encode()

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	pl.mu.Lock()
	pl.cancel = func() { cancel() }
	pl.mu.Unlock()
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, cursor, err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	client := &http.Client{Timeout: 40 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, cursor, err
	}
	defer resp.Body.Close()
	var out struct {
		Cursor int64       `json:"cursor"`
		Events []*HubEvent `json:"events"`
	}
	dec := json.NewDecoder(io.LimitReader(resp.Body, 64<<20))
	if err := dec.Decode(&out); err != nil {
		return nil, cursor, err
	}
	return out.Events, out.Cursor, nil
}

// ---------------------------------------------------------------------------
// SSE loop
// ---------------------------------------------------------------------------

type sseLoop struct {
	c         *HubClient
	stop      chan struct{}
	seen      *dedup
	mu        sync.Mutex // guards body between the writer and Close()
	body      io.Closer  // current stream body, closed to unblock reads
	closeOnce sync.Once
}

func (c *HubClient) startSSE() error {
	c.mu.Lock()
	if c.closed || c.sseLoop != nil {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	sl := &sseLoop{c: c, stop: make(chan struct{}), seen: newDedup()}
	resp, err := sl.connect()
	if err != nil {
		return err
	}
	c.mu.Lock()
	if c.sseLoop != nil { // lost the race: another consumer already connected
		c.mu.Unlock()
		resp.Body.Close()
		return nil
	}
	sl.setBody(resp.Body)
	c.sseLoop = sl
	c.mu.Unlock()
	go sl.run(resp) // stream the already-open connection
	return nil
}

// run consumes the open stream and reconnects with backoff on failure until
// the loop is stopped (client close).
func (sl *sseLoop) run(resp *http.Response) {
	defer sl.close()
	retries := 0
	for {
		if resp != nil {
			err := sl.readBody(resp)
			resp.Body.Close()
			resp = nil
			select {
			case <-sl.stop:
				return
			default:
			}
			if err != nil {
				retries++
			} else {
				retries = 1 // clean EOF is still a drop: reconnect
			}
		}
		if !sl.c.backoffWait(retries) {
			return
		}
		next, cerr := sl.connect()
		if cerr != nil {
			continue
		}
		sl.setBody(next.Body)
		resp = next
	}
}

// setBody publishes the current stream body so close() can unblock reads.
func (sl *sseLoop) setBody(b io.Closer) {
	sl.mu.Lock()
	sl.body = b
	sl.mu.Unlock()
}

// close stops the loop and unblocks any pending read.
func (sl *sseLoop) close() {
	sl.closeOnce.Do(func() {
		close(sl.stop)
		sl.mu.Lock()
		if sl.body != nil {
			sl.body.Close()
		}
		sl.mu.Unlock()
	})
}

// connect opens the /v1/events stream; doing it synchronously means no event
// published after EventSubscribe can slip between subscribe and stream.
func (sl *sseLoop) connect() (*http.Response, error) {
	u := *sl.c.baseURL
	u.Path = strings.TrimSuffix(u.Path, "/") + "/v1/events"
	q := u.Query()
	if sl.c.token != "" {
		q.Set("token", sl.c.token)
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	if sl.c.token != "" {
		req.Header.Set("Authorization", "Bearer "+sl.c.token)
	}
	client := &http.Client{Timeout: 0} // no overall timeout for a stream
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("sse: status %s", resp.Status)
	}
	return resp, nil
}

// readBody consumes one SSE response body, dispatching frames as they arrive.
func (sl *sseLoop) readBody(resp *http.Response) error {
	br := bufio.NewReader(resp.Body)
	var data []string
	for {
		line, err := br.ReadString('\n')
		if len(line) > 0 {
			line = strings.TrimRight(line, "\r\n")
			switch {
			case strings.HasPrefix(line, ":"):
				// comment / heartbeat
			case strings.HasPrefix(line, "data:"):
				data = append(data, strings.TrimPrefix(line, "data:"))
			case line == "" && len(data) > 0:
				sl.handleData(strings.Join(data, "\n"))
				data = nil
			}
		}
		if err != nil {
			return err // io.EOF or network error -> reconnect
		}
	}
}

func (sl *sseLoop) handleData(payload string) {
	var env map[string]json.RawMessage
	if err := json.Unmarshal([]byte(payload), &env); err != nil {
		return
	}
	method := ""
	_ = json.Unmarshal(env["method"], &method)
	if method != "event" {
		return // unknown notification types are ignored
	}
	var params struct {
		SubscriptionID string    `json:"subscription_id"`
		Event          *HubEvent `json:"event"`
	}
	if err := json.Unmarshal(env["params"], &params); err != nil || params.Event == nil {
		return
	}
	if !sl.seen.add(params.Event.ID) {
		return
	}
	sl.c.dispatchEvent(params.SubscriptionID, params.Event)
}

// ---------------------------------------------------------------------------
// dedup
// ---------------------------------------------------------------------------

// dedup remembers event ids within a sliding window (PROTOCOL.md §6.2: 5000).
type dedup struct {
	mu    sync.Mutex
	ids   map[string]struct{}
	order []string
	limit int
}

func newDedup() *dedup {
	return &dedup{ids: make(map[string]struct{}), limit: eventDedupWindow}
}

func (d *dedup) add(id string) bool {
	if id == "" {
		return true
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.ids[id]; ok {
		return false
	}
	d.ids[id] = struct{}{}
	d.order = append(d.order, id)
	if len(d.order) > d.limit {
		drop := d.order[:len(d.order)-d.limit]
		for _, old := range drop {
			delete(d.ids, old)
		}
		d.order = d.order[len(d.order)-d.limit:]
	}
	return true
}

// ---------------------------------------------------------------------------
// offline queue
// ---------------------------------------------------------------------------

// QueueWrite enqueues a write operation while offline. This is an optional
// best-effort offline queue (PROTOCOL.md §6.2, limit 1000): when the
// connection recovers, queued operations are replayed in order.
//
// TODO: wire this into Call so command.send / task.create / node.* calls made
// while disconnected are queued and replayed after reconnect. Currently the
// SDK surfaces the transport error to the caller instead, which is the
// simpler "at-most-once" default.
func (c *HubClient) QueueWrite(req map[string]any) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.offline) >= offlineQueueLimit {
		return false
	}
	c.offline = append(c.offline, req)
	return true
}

// Close shuts down the client and all background loops.
func (c *HubClient) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	ws := c.ws
	c.ws = nil
	sse := c.sseLoop
	c.sseLoop = nil
	pl := c.poller
	c.poller = nil
	subs := make([]*Subscription, 0, len(c.subs))
	for _, s := range c.subs {
		subs = append(subs, s)
	}
	c.subs = map[string]*Subscription{}
	c.mu.Unlock()

	for _, s := range subs {
		select {
		case <-s.done:
		default:
			close(s.done)
		}
	}
	if sse != nil {
		sse.close()
	}
	if pl != nil {
		pl.end()
	}
	if ws != nil {
		_ = ws.Close()
	}
	return nil
}

// ParseEvent extracts a HubEvent from a raw notification map (used by tests
// and advanced consumers).
func ParseEvent(notification map[string]any) *HubEvent {
	b, err := json.Marshal(notification)
	if err != nil {
		return nil
	}
	var env struct {
		Params struct {
			SubscriptionID string    `json:"subscription_id"`
			Event          *HubEvent `json:"event"`
		} `json:"params"`
	}
	if err := json.Unmarshal(b, &env); err != nil {
		return nil
	}
	return env.Params.Event
}

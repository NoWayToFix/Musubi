package hub

import (
	"fmt"
	"sync"
	"time"

	"musubi/engine/internal/jsonrpc"
)

// Node statuses (PROTOCOL.md §4.1).
const (
	StatusOnline  = "online"
	StatusBusy    = "busy"
	StatusOffline = "offline"
	StatusUnknown = "unknown"
)

// NodePlatform describes the platform a node runs on (PROTOCOL.md §4.1).
// It is self-reported by the agent/SDK at registration; the core treats it as
// opaque routing/display data and never rejects unknown values.
type NodePlatform struct {
	OS             string            `json:"os,omitempty"`              // linux|darwin|windows|...
	Arch           string            `json:"arch,omitempty"`            // amd64|arm64|x64|...
	Runtime        string            `json:"runtime,omitempty"`         // go|node|deno|bun|browser|electron|python|demo
	RuntimeVersion string            `json:"runtime_version,omitempty"` // runtime version string
	AgentVersion   string            `json:"agent_version,omitempty"`   // SDK/agent version
	Labels         map[string]string `json:"labels,omitempty"`          // free-form platform labels
}

// Node is a controlled node (schema node.json). Unknown fields reported by
// registering clients are ignored; the engine keeps the protocol fields only.
type Node struct {
	ID           string         `json:"id"`
	Name         string         `json:"name"`
	Status       string         `json:"status"`
	Capabilities []string       `json:"capabilities,omitempty"`
	Tags         []string       `json:"tags,omitempty"`
	Metadata     map[string]any `json:"metadata,omitempty"`
	Platform     *NodePlatform  `json:"platform,omitempty"` // self-reported platform descriptor
	LastSeen     string         `json:"last_seen"`
	RegisteredAt string         `json:"registered_at"`
	SeatID       *string        `json:"seat_id,omitempty"` // occupied seat (PROTOCOL.md §4.7)

	demo     bool   // simulated node: never expires, not serialised
	misses   int    // heartbeat watchdog misses
	busyRefs int    // concurrent running commands (busy refcount)
	owner    string // registrar identity (see callerOwner); "" = anonymous
}

// RegisterRequest is the POST /v1/register body and internal demo registration.
type RegisterRequest struct {
	ID           string         `json:"id"`
	Name         string         `json:"name"`
	Capabilities *[]string      `json:"capabilities"`
	Tags         []string       `json:"tags"`
	Metadata     map[string]any `json:"metadata"`
	Platform     *NodePlatform  `json:"platform"`
}

type nodeRegistry struct {
	hub   *Hub
	mu    sync.RWMutex
	nodes map[string]*Node
	order []string // insertion order for stable listing
}

func newNodeRegistry(h *Hub) *nodeRegistry {
	return &nodeRegistry{hub: h, nodes: map[string]*Node{}}
}

// register adds a node or refreshes an existing one (re-register acts as a
// heartbeat). New nodes are online; existing offline nodes come back online.
func (r *nodeRegistry) register(req RegisterRequest, demo bool, owner string) *Node {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := nowTS()
	// Normalise optional pointer fields.
	caps := []string(nil)
	if req.Capabilities != nil {
		caps = *req.Capabilities
	}
	if n, ok := r.nodes[req.ID]; ok {
		if req.Name != "" {
			n.Name = req.Name
		}
		if caps != nil {
			n.Capabilities = append([]string(nil), caps...)
		}
		if req.Tags != nil {
			n.Tags = append([]string(nil), req.Tags...)
		}
		if req.Metadata != nil {
			n.Metadata = req.Metadata
		}
		if req.Platform != nil {
			n.Platform = clonePlatform(req.Platform)
		}
		if owner != "" {
			n.owner = owner
		}
		wasOffline := n.Status == StatusOffline || n.Status == StatusUnknown
		n.LastSeen = now
		n.misses = 0
		n.Status = StatusOnline
		cl := cloneNode(n)
		r.hub.publishNodeUpdated(cl)
		if wasOffline {
			r.hub.publishNodeStatus(n.ID, StatusOnline)
		}
		return cl
	}
	n := &Node{
		ID:           req.ID,
		Name:         req.Name,
		Status:       StatusOnline,
		Capabilities: append([]string(nil), caps...),
		Tags:         append([]string(nil), req.Tags...),
		Metadata:     req.Metadata,
		Platform:     clonePlatform(req.Platform),
		LastSeen:     now,
		RegisteredAt: now,
		demo:         demo,
		owner:        owner,
	}
	r.nodes[req.ID] = n
	r.order = append(r.order, req.ID)
	cl := cloneNode(n)
	r.hub.publishNodeRegistered(cl)
	return cl
}

// heartbeat refreshes a node's last_seen (node.heartbeat extension).
func (r *nodeRegistry) heartbeat(id string) (*Node, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, ok := r.nodes[id]
	if !ok {
		return nil, false
	}
	n.LastSeen = nowTS()
	n.misses = 0
	if n.Status == StatusOffline || n.Status == StatusUnknown {
		n.Status = StatusOnline
		r.hub.publishNodeStatus(id, StatusOnline)
	}
	return cloneNode(n), true
}

// get returns a snapshot of one node.
func (r *nodeRegistry) get(id string) (*Node, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n, ok := r.nodes[id]
	if !ok {
		return nil, false
	}
	return cloneNode(n), true
}

// status returns the live status of one node.
func (r *nodeRegistry) status(id string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n, ok := r.nodes[id]
	if !ok {
		return "", false
	}
	return n.Status, true
}

// exists reports whether a node id is registered.
func (r *nodeRegistry) exists(id string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.nodes[id]
	return ok
}

// isDemo reports whether id belongs to a simulated node (executed in-process).
func (r *nodeRegistry) isDemo(id string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n, ok := r.nodes[id]
	return ok && n.demo
}

// accepting reports whether a node can receive new work: it must exist and not
// be offline/unknown. A busy node still accepts (commands are ref-counted).
func (r *nodeRegistry) accepting(id string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n, ok := r.nodes[id]
	if !ok {
		return false
	}
	return n.Status != StatusOffline && n.Status != StatusUnknown
}

// tryAcquire atomically reserves an online node for execution (online -> busy).
// Concurrent acquisitions are allowed and ref-counted: a node is busy while
// any command is running, and flips back to online when the last releases.
func (r *nodeRegistry) tryAcquire(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, ok := r.nodes[id]
	if !ok || n.Status == StatusOffline || n.Status == StatusUnknown {
		return false
	}
	n.busyRefs++
	if n.Status != StatusBusy {
		n.Status = StatusBusy
		r.hub.publishNodeStatus(id, StatusBusy)
	}
	return true
}

// release restores a busy node to online when the last command finishes.
func (r *nodeRegistry) release(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, ok := r.nodes[id]
	if !ok || n.busyRefs == 0 {
		return
	}
	n.busyRefs--
	if n.busyRefs == 0 && n.Status == StatusBusy {
		n.Status = StatusOnline
		r.hub.publishNodeStatus(id, StatusOnline)
	}
}

// unregister removes a node (node.unregister extension; also fires
// node.unregistered for the event stream).
func (r *nodeRegistry) unregister(id string) (*Node, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, ok := r.nodes[id]
	if !ok {
		return nil, false
	}
	cl := cloneNode(n)
	delete(r.nodes, id)
	for i, oid := range r.order {
		if oid == id {
			r.order = append(r.order[:i], r.order[i+1:]...)
			break
		}
	}
	r.hub.publishNodeUnregistered(id)
	return cl, true
}

// nodeFilter filters node.list results: status exact match, tags "any match",
// runtime/os exact match against the self-reported platform descriptor.
type nodeFilter struct {
	status  string
	tags    []string
	runtime string
	os      string
}

func (f *nodeFilter) match(n *Node) bool {
	if f.status != "" && n.Status != f.status {
		return false
	}
	if f.runtime != "" && (n.Platform == nil || n.Platform.Runtime != f.runtime) {
		return false
	}
	if f.os != "" && (n.Platform == nil || n.Platform.OS != f.os) {
		return false
	}
	if len(f.tags) > 0 {
		hit := false
		for _, t := range f.tags {
			for _, nt := range n.Tags {
				if t == nt {
					hit = true
					break
				}
			}
			if hit {
				break
			}
		}
		if !hit {
			return false
		}
	}
	return true
}

// list returns filtered, paginated node snapshots plus the pre-pagination
// total. Always returns a non-nil slice (JSON [] rather than null).
func (r *nodeRegistry) list(f *nodeFilter, limit, offset int) ([]*Node, int) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Node, 0)
	for _, id := range r.order {
		n := r.nodes[id]
		if f != nil && !f.match(n) {
			continue
		}
		out = append(out, cloneNode(n))
	}
	total := len(out)
	if offset >= len(out) {
		out = []*Node{}
	} else {
		out = out[offset:]
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, total
}

// watchdogTick marks real nodes offline after 3 missed heartbeat intervals.
// Demo nodes are always online. Returns the ids that transitioned to offline
// so callers can run side effects (seat release) outside the registry lock.
func (r *nodeRegistry) watchdogTick(interval time.Duration) []string {
	r.mu.Lock()
	var offlined []string
	defer r.mu.Unlock()
	cutoff := time.Now().UTC().Add(-2 * interval)
	for _, n := range r.nodes {
		if n.demo || n.Status == StatusOffline {
			continue
		}
		last, err := time.Parse(tsLayout, n.LastSeen)
		if err == nil && last.Before(cutoff) {
			n.misses++
		} else {
			n.misses = 0
		}
		if n.misses >= 3 {
			n.Status = StatusOffline
			offlined = append(offlined, n.ID)
			r.hub.publishNodeStatus(n.ID, StatusOffline)
		}
	}
	return offlined
}

func cloneNode(n *Node) *Node {
	c := *n
	c.Capabilities = append([]string(nil), n.Capabilities...)
	c.Tags = append([]string(nil), n.Tags...)
	if n.Metadata != nil {
		c.Metadata = make(map[string]any, len(n.Metadata))
		for k, v := range n.Metadata {
			c.Metadata[k] = v
		}
	}
	c.Platform = clonePlatform(n.Platform)
	return &c
}

// clonePlatform deep-copies a platform descriptor (nil-safe).
func clonePlatform(p *NodePlatform) *NodePlatform {
	if p == nil {
		return nil
	}
	c := *p
	if p.Labels != nil {
		c.Labels = make(map[string]string, len(p.Labels))
		for k, v := range p.Labels {
			c.Labels[k] = v
		}
	}
	return &c
}

// platformParam parses an optional platform object from JSON-RPC params.
// Malformed members are ignored (unknown/odd values must not break register).
func platformParam(v any) *NodePlatform {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	p := &NodePlatform{
		OS:             strParam(m["os"]),
		Arch:           strParam(m["arch"]),
		Runtime:        strParam(m["runtime"]),
		RuntimeVersion: strParam(m["runtime_version"]),
		AgentVersion:   strParam(m["agent_version"]),
	}
	if labels, ok := m["labels"].(map[string]any); ok {
		p.Labels = make(map[string]string, len(labels))
		for k, val := range labels {
			if s, ok := val.(string); ok {
				p.Labels[k] = s
			}
		}
	}
	if p.OS == "" && p.Arch == "" && p.Runtime == "" && p.RuntimeVersion == "" &&
		p.AgentVersion == "" && len(p.Labels) == 0 {
		return nil
	}
	return p
}

// stringSliceParam parses an optional string array. ok=false when absent or
// not an array; present-but-empty clears the field on re-registration.
func stringSliceParam(v any) ([]string, bool) {
	arr, ok := v.([]any)
	if !ok {
		return nil, false
	}
	out := make([]string, 0, len(arr))
	for _, item := range arr {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out, true
}

// ---------------------------------------------------------------------------
// node.* RPC handlers

func (h *Hub) nodeList(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	limit, offset := clampPage(intParam(p["limit"], 100), intParam(p["offset"], 0))
	var f *nodeFilter
	if raw, ok := p["filter"].(map[string]any); ok {
		f = &nodeFilter{
			status:  strParam(raw["status"]),
			runtime: strParam(raw["runtime"]),
			os:      strParam(raw["os"]),
		}
		// "platform" is accepted as an alias for the runtime filter.
		if f.runtime == "" {
			f.runtime = strParam(raw["platform"])
		}
		if tags, ok := raw["tags"].([]any); ok {
			for _, t := range tags {
				if s, ok := t.(string); ok {
					f.tags = append(f.tags, s)
				}
			}
		}
	}
	nodes, total := h.nodes.list(f, limit, offset)
	return map[string]any{"nodes": nodes, "total": total}, nil
}

func (h *Hub) nodeGet(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	id := strParam(p["node_id"])
	if id == "" {
		return nil, jsonrpc.ErrInvalidParams("node_id required")
	}
	n, ok := h.nodes.get(id)
	if !ok {
		return nil, jsonrpc.NewError(jsonrpc.CodeNodeNotFound, "node.not_found",
			map[string]any{"node_id": id})
	}
	return map[string]any{"node": n}, nil
}

// nodeHeartbeat is an additive extension so real nodes can stay online.
func (h *Hub) nodeHeartbeat(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	id := strParam(p["node_id"])
	if id == "" {
		return nil, jsonrpc.ErrInvalidParams("node_id required")
	}
	if _, ok := h.nodes.get(id); !ok {
		return nil, jsonrpc.NewError(jsonrpc.CodeNodeNotFound, "node.not_found",
			map[string]any{"node_id": id})
	}
	if !h.mayActOnNode(cc, id) {
		return nil, errForbidden("node")
	}
	n, ok := h.nodes.heartbeat(id)
	if !ok {
		return nil, jsonrpc.NewError(jsonrpc.CodeNodeNotFound, "node.not_found",
			map[string]any{"node_id": id})
	}
	return map[string]any{"node": n}, nil
}

// nodeUnregister is an additive extension for node lifecycle management.
func (h *Hub) nodeUnregister(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	id := strParam(p["node_id"])
	if id == "" {
		return nil, jsonrpc.ErrInvalidParams("node_id required")
	}
	if _, ok := h.nodes.get(id); !ok {
		return nil, jsonrpc.NewError(jsonrpc.CodeNodeNotFound, "node.not_found",
			map[string]any{"node_id": id})
	}
	if !h.mayActOnNode(cc, id) {
		return nil, errForbidden("node")
	}
	n, ok := h.nodes.unregister(id)
	if !ok {
		return nil, jsonrpc.NewError(jsonrpc.CodeNodeNotFound, "node.not_found",
			map[string]any{"node_id": id})
	}
	// seat lifecycle hooks (PROTOCOL.md §4.7): unregistering releases the
	// occupied seat shell back to vacant.
	if h.seats != nil {
		h.seats.releaseByNode(id)
	}
	// The node is gone: anything queued or running for it cannot complete.
	h.failNodeCommands(id, jsonrpc.CodeNodeOffline, "node.offline")
	return map[string]any{"node": n}, nil
}

// nodeRegister registers or refreshes a real node over JSON-RPC
// (PROTOCOL.md §5.2). Together with node.heartbeat / command.pull /
// command.complete it makes an SDK agent a pure JSON-RPC client, so a
// platform needs no HTTP-specific endpoint to join the control center.
// Registration and update semantics mirror POST /v1/register.
func (h *Hub) nodeRegister(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	id := strParam(p["node_id"])
	if id == "" {
		id = strParam(p["id"]) // alias kept for POST /v1/register compatibility
	}
	req := RegisterRequest{
		ID:       id,
		Name:     strParam(p["name"]),
		Metadata: metadataParam(p["metadata"]),
		Platform: platformParam(p["platform"]),
	}
	if caps, ok := stringSliceParam(p["capabilities"]); ok {
		req.Capabilities = &caps
	}
	if tags, ok := stringSliceParam(p["tags"]); ok {
		req.Tags = tags
	}
	n, aerr := h.RegisterNodeOwned(req, callerOwner(cc))
	if aerr != nil {
		return nil, aerr
	}
	return map[string]any{"node": n}, nil
}

// metadataParam parses an optional metadata object; absent/non-object -> nil.
func metadataParam(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return nil
}

func (h *Hub) nodeSubscribe(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	return h.makeSubscription(cc, nil, strParam(p["node_id"]), "")
}

func (h *Hub) nodeUnsubscribe(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	id := strParam(p["subscription_id"])
	if id == "" {
		return nil, jsonrpc.ErrInvalidParams("subscription_id required")
	}
	sub := h.events.getSub(id)
	if sub == nil {
		return nil, jsonrpc.NewError(jsonrpc.CodeSubNotFound, "subscription.not_found",
			map[string]any{"subscription_id": id})
	}
	if !mayActOn(cc, sub.Owner) {
		return nil, errForbidden("subscription")
	}
	h.events.closeSub(id)
	return map[string]any{}, nil
}

// demoCollision reports whether id already belongs to a demo node. Real
// registrations must not shadow demo nodes: a caller posting id "n-1" to
// /v1/register would otherwise overwrite the simulated node's identity.
func (r *nodeRegistry) demoCollision(id string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n, ok := r.nodes[id]
	return ok && n.demo
}

// assignSeat / clearSeat maintain the node-side seat_id mirror (PROTOCOL.md
// §4.7). Best-effort: unknown nodes are ignored.
func (r *nodeRegistry) assignSeat(nodeID, seatID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n, ok := r.nodes[nodeID]; ok {
		s := seatID
		n.SeatID = &s
	}
}

func (r *nodeRegistry) clearSeat(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n, ok := r.nodes[nodeID]; ok {
		n.SeatID = nil
	}
}

// RegisterNode registers a real node with no owner (tests and internal callers).
func (h *Hub) RegisterNode(req RegisterRequest) (*Node, *jsonrpc.Error) {
	return h.RegisterNodeOwned(req, "")
}

// RegisterNodeOwned registers a real node via POST /v1/register (online on
// arrival). It rejects ids that collide with a demo node (-32602) and refuses
// to let a different session take over a node that already has an owner
// (-32004), so a node id cannot be hijacked by re-registering it.
// After a successful registration, vacant `auto` seats matching the node's
// profile are filled (PROTOCOL.md §4.7 补席).
func (h *Hub) RegisterNodeOwned(req RegisterRequest, owner string) (*Node, *jsonrpc.Error) {
	if req.ID == "" {
		req.ID = fmt.Sprintf("n-%d", h.nodeSeq.Add(1))
	}
	if h.nodes.demoCollision(req.ID) {
		return nil, jsonrpc.NewError(jsonrpc.CodeInvalidParams, "invalid params: node_id is reserved by a demo node",
			map[string]any{"node_id": req.ID})
	}
	if cur, ok := h.nodes.ownerOf(req.ID); ok && cur != owner {
		return nil, errForbidden("node")
	}
	if req.Name == "" {
		req.Name = req.ID
	}
	n := h.nodes.register(req, false, owner)
	h.seats.onNodeRegistered(n, false)
	return n, nil
}

// ownerOf returns the registrar identity of a node ("" = anonymous).
func (r *nodeRegistry) ownerOf(id string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n, ok := r.nodes[id]
	if !ok {
		return "", false
	}
	return n.owner, true
}

// mayActOnNode reports whether the caller may heartbeat/unregister/pull for a
// node. Anonymous-owned nodes stay open (none mode is trust-everything).
func (h *Hub) mayActOnNode(cc *CallCtx, id string) bool {
	owner, ok := h.nodes.ownerOf(id)
	return ok && mayActOn(cc, owner)
}

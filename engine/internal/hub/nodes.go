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

// Node is a controlled node (schema node.json). Unknown fields reported by
// registering clients are ignored; the engine keeps the protocol fields only.
type Node struct {
	ID           string         `json:"id"`
	Name         string         `json:"name"`
	Status       string         `json:"status"`
	Capabilities []string       `json:"capabilities,omitempty"`
	Tags         []string       `json:"tags,omitempty"`
	Metadata     map[string]any `json:"metadata,omitempty"`
	LastSeen     string         `json:"last_seen"`
	RegisteredAt string         `json:"registered_at"`

	demo     bool // simulated node: never expires, not serialised
	misses   int  // heartbeat watchdog misses
	busyRefs int  // concurrent running commands (busy refcount)
}

// RegisterRequest is the POST /v1/register body and internal demo registration.
type RegisterRequest struct {
	ID           string         `json:"id"`
	Name         string         `json:"name"`
	Capabilities *[]string      `json:"capabilities"`
	Tags         []string       `json:"tags"`
	Metadata     map[string]any `json:"metadata"`
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
func (r *nodeRegistry) register(req RegisterRequest, demo bool) *Node {
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
		LastSeen:     now,
		RegisteredAt: now,
		demo:         demo,
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

// nodeFilter filters node.list results: status exact match, tags "any match".
type nodeFilter struct {
	status string
	tags   []string
}

func (f *nodeFilter) match(n *Node) bool {
	if f.status != "" && n.Status != f.status {
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
// Demo nodes are always online.
func (r *nodeRegistry) watchdogTick(interval time.Duration) {
	r.mu.Lock()
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
			r.hub.publishNodeStatus(n.ID, StatusOffline)
		}
	}
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
	return &c
}

// ---------------------------------------------------------------------------
// node.* RPC handlers

func (h *Hub) nodeList(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	limit, offset := clampPage(intParam(p["limit"], 100), intParam(p["offset"], 0))
	var f *nodeFilter
	if raw, ok := p["filter"].(map[string]any); ok {
		f = &nodeFilter{status: strParam(raw["status"])}
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
	n, ok := h.nodes.unregister(id)
	if !ok {
		return nil, jsonrpc.NewError(jsonrpc.CodeNodeNotFound, "node.not_found",
			map[string]any{"node_id": id})
	}
	return map[string]any{"node": n}, nil
}

func (h *Hub) nodeSubscribe(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	return h.makeSubscription(cc, nil, strParam(p["node_id"]), "")
}

func (h *Hub) nodeUnsubscribe(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	id := strParam(p["subscription_id"])
	if id == "" {
		return nil, jsonrpc.ErrInvalidParams("subscription_id required")
	}
	if !h.events.closeSub(id) {
		return nil, jsonrpc.NewError(jsonrpc.CodeSubNotFound, "subscription.not_found",
			map[string]any{"subscription_id": id})
	}
	return map[string]any{}, nil
}

// RegisterNode registers a real node via POST /v1/register (online on arrival).
func (h *Hub) RegisterNode(req RegisterRequest) *Node {
	if req.ID == "" {
		req.ID = fmt.Sprintf("n-%d", h.nodeSeq.Add(1))
	}
	if req.Name == "" {
		req.Name = req.ID
	}
	return h.nodes.register(req, false)
}

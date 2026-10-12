package hub

import (
	"bytes"
	"encoding/json"
	"sync"

	"musubi/engine/internal/jsonrpc"
)

// Seat kinds (PROTOCOL.md §4.7): standing = occupied by a node, vacant =
// reserved and waiting to be filled.
const (
	SeatKindStanding = "standing"
	SeatKindVacant   = "vacant"
)

// Seat bind policies (vacant-seat semantics).
const (
	SeatBindAuto   = "auto"   // fill automatically when a matching node registers
	SeatBindManual = "manual" // fill only via seat.bind
)

// SeatExpect is the expected profile a filling node must match. All fields
// optional; unknown fields ignored (forward compatibility). Matching rules:
// capabilities = all required, tags = any-match (same as node.list
// filter.tags), metadata = key/value equality on listed keys.
type SeatExpect struct {
	Capabilities []string       `json:"capabilities,omitempty"`
	Tags         []string       `json:"tags,omitempty"`
	Metadata     map[string]any `json:"metadata,omitempty"`
}

// matches reports whether node n satisfies the profile.
func (e *SeatExpect) matches(n *Node) bool {
	if e == nil {
		return true
	}
	if len(e.Capabilities) > 0 {
		has := make(map[string]bool, len(n.Capabilities))
		for _, c := range n.Capabilities {
			has[c] = true
		}
		for _, c := range e.Capabilities {
			if !has[c] {
				return false
			}
		}
	}
	if len(e.Tags) > 0 {
		hit := false
		for _, t := range e.Tags {
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
	if len(e.Metadata) > 0 {
		for k, v := range e.Metadata {
			nv, ok := n.Metadata[k]
			if !ok || !jsonEqualValue(nv, v) {
				return false
			}
		}
	}
	return true
}

// jsonEqualValue compares two opaque metadata values by canonical JSON bytes,
// so json.Number("1") from RPC params equals float64(1) from an HTTP body.
func jsonEqualValue(a, b any) bool {
	ab, err1 := json.Marshal(a)
	bb, err2 := json.Marshal(b)
	return err1 == nil && err2 == nil && bytes.Equal(ab, bb)
}

// Seat is a reservable position (schema seat.json). One node occupies at most
// one seat; releasing a seat keeps the shell (and its expect profile) for a
// later refill: the position is durable, the occupant is transient.
type Seat struct {
	ID         string      `json:"id"`
	Kind       string      `json:"kind"`
	Name       string      `json:"name,omitempty"`
	BindPolicy string      `json:"bind_policy,omitempty"`
	Expect     *SeatExpect `json:"expect,omitempty"`
	NodeID     *string     `json:"node_id"`
	CreatedAt  string      `json:"created_at"`
	FilledAt   *string     `json:"filled_at,omitempty"`
	ReleasedAt *string     `json:"released_at,omitempty"`

	owner string // creator identity (see callerOwner); "" = anonymous
}

func cloneSeat(s *Seat) *Seat {
	c := *s
	if s.Expect != nil {
		e := *s.Expect
		e.Capabilities = append([]string(nil), s.Expect.Capabilities...)
		e.Tags = append([]string(nil), s.Expect.Tags...)
		if s.Expect.Metadata != nil {
			e.Metadata = make(map[string]any, len(s.Expect.Metadata))
			for k, v := range s.Expect.Metadata {
				e.Metadata[k] = v
			}
		}
		c.Expect = &e
	}
	return &c
}

type seatRegistry struct {
	hub   *Hub
	mu    sync.RWMutex
	seats map[string]*Seat
	order []string // creation order for stable listing and auto-fill priority
}

func newSeatRegistry(h *Hub) *seatRegistry {
	return &seatRegistry{hub: h, seats: map[string]*Seat{}}
}

// create adds a vacant seat and publishes seat.created.
func (r *seatRegistry) create(name, bindPolicy string, expect *SeatExpect, owner string) *Seat {
	now := nowTS()
	s := &Seat{
		ID:         randomID("s-"),
		Kind:       SeatKindVacant,
		Name:       name,
		BindPolicy: bindPolicy,
		Expect:     expect,
		NodeID:     nil,
		CreatedAt:  now,
		owner:      owner,
	}
	r.mu.Lock()
	r.seats[s.ID] = s
	r.order = append(r.order, s.ID)
	cl := cloneSeat(s)
	r.mu.Unlock()
	r.hub.publishSeatCreated(cl)
	return cl
}

// get returns a snapshot of one seat.
func (r *seatRegistry) get(id string) (*Seat, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.seats[id]
	if !ok {
		return nil, false
	}
	return cloneSeat(s), true
}

// seatFilter filters seat.list: kind and node_id exact match.
type seatFilter struct {
	kind   string
	nodeID string
}

func (f *seatFilter) match(s *Seat) bool {
	if f.kind != "" && s.Kind != f.kind {
		return false
	}
	if f.nodeID != "" && (s.NodeID == nil || *s.NodeID != f.nodeID) {
		return false
	}
	return true
}

// list returns filtered, paginated seat snapshots plus the pre-pagination
// total. Always returns a non-nil slice (JSON [] rather than null).
func (r *seatRegistry) list(f *seatFilter, limit, offset int) ([]*Seat, int) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Seat, 0)
	for _, id := range r.order {
		s := r.seats[id]
		if f != nil && !f.match(s) {
			continue
		}
		out = append(out, cloneSeat(s))
	}
	total := len(out)
	if offset >= len(out) {
		out = []*Seat{}
	} else {
		out = out[offset:]
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, total
}

// del removes a vacant seat (occupied seats must be released first).
func (r *seatRegistry) del(id string) (*Seat, bool, bool) {
	r.mu.Lock()
	s, ok := r.seats[id]
	if !ok {
		r.mu.Unlock()
		return nil, false, false
	}
	if s.Kind != SeatKindVacant {
		r.mu.Unlock()
		return cloneSeat(s), true, false
	}
	cl := cloneSeat(s)
	delete(r.seats, id)
	for i, oid := range r.order {
		if oid == id {
			r.order = append(r.order[:i], r.order[i+1:]...)
			break
		}
	}
	r.mu.Unlock()
	r.hub.publishSeatDeleted()
	return cl, true, true
}

// onNodeRegistered tries to auto-fill vacant `auto` seats after a real node
// registers. Seats are scanned in creation order; the first match wins. Demo
// nodes never take seats. No-op when the node already holds a seat.
func (r *seatRegistry) onNodeRegistered(n *Node, demo bool) {
	if demo || n == nil {
		return
	}
	r.mu.RLock()
	var target *Seat
	if n.SeatID == nil {
		for _, id := range r.order {
			s := r.seats[id]
			if s != nil && s.Kind == SeatKindVacant && s.BindPolicy == SeatBindAuto && s.Expect.matches(n) {
				target = s
				break
			}
		}
	}
	r.mu.RUnlock()
	if target != nil {
		r.hub.seats.fill(target.ID, n.ID)
	}
}

// fill occupies a vacant seat with a node: the seat becomes standing, the
// node gains seat_id, and any previous seat of that node is released first
// (an occupant is transient; a node moves seats by auto-releasing the old
// one). Returns the updated seat snapshot, or nil when the seat or node is
// gone or the seat is already occupied.
func (r *seatRegistry) fill(seatID, nodeID string) *Seat {
	// Release the node's previous seat, if any (idempotent when none).
	if prev := r.seatOfNode(nodeID); prev != "" && prev != seatID {
		r.release(prev)
	}

	r.mu.Lock()
	s, ok := r.seats[seatID]
	if !ok || s.Kind != SeatKindVacant {
		r.mu.Unlock()
		return nil
	}
	if !r.hub.nodes.exists(nodeID) {
		r.mu.Unlock()
		return nil
	}
	now := nowTS()
	nid := nodeID
	s.Kind = SeatKindStanding
	s.NodeID = &nid
	s.FilledAt = &now
	s.ReleasedAt = nil
	cl := cloneSeat(s)
	r.mu.Unlock()

	r.hub.nodes.assignSeat(nodeID, seatID)
	r.hub.publishSeatFilled(cl)
	return cl
}

// release returns a standing seat to vacant, keeping the shell and its expect
// profile for a later refill. Idempotent: a vacant seat is returned unchanged.
func (r *seatRegistry) release(seatID string) *Seat {
	r.mu.Lock()
	s, ok := r.seats[seatID]
	if !ok || s.Kind != SeatKindStanding {
		var cl *Seat
		if ok {
			cl = cloneSeat(s)
		}
		r.mu.Unlock()
		return cl
	}
	exNode := ""
	if s.NodeID != nil {
		exNode = *s.NodeID
	}
	now := nowTS()
	s.Kind = SeatKindVacant
	s.NodeID = nil
	s.ReleasedAt = &now
	cl := cloneSeat(s)
	r.mu.Unlock()

	if exNode != "" {
		r.hub.nodes.clearSeat(exNode)
	}
	r.hub.publishSeatReleased(cl, exNode)
	return cl
}

// seatOfNode returns the seat id currently occupied by nodeID ("" when none).
func (r *seatRegistry) seatOfNode(nodeID string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, id := range r.order {
		s := r.seats[id]
		if s != nil && s.Kind == SeatKindStanding && s.NodeID != nil && *s.NodeID == nodeID {
			return id
		}
	}
	return ""
}

// releaseByNode releases the seat occupied by nodeID, if any. Used when a
// node unregisters or is marked offline by the watchdog: the occupant left,
// the seat shell stays.
func (r *seatRegistry) releaseByNode(nodeID string) {
	if seatID := r.seatOfNode(nodeID); seatID != "" {
		r.release(seatID)
	}
}

// ---------------------------------------------------------------------------
// seat.* RPC handlers

func (h *Hub) seatList(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	limit, offset := clampPage(intParam(p["limit"], 100), intParam(p["offset"], 0))
	var f *seatFilter
	if raw, ok := p["filter"].(map[string]any); ok {
		f = &seatFilter{}
		if kind := strParam(raw["kind"]); kind != "" {
			if kind != SeatKindStanding && kind != SeatKindVacant {
				return nil, jsonrpc.ErrInvalidParams("filter.kind must be \"standing\" or \"vacant\"")
			}
			f.kind = kind
		}
		f.nodeID = strParam(raw["node_id"])
	}
	seats, total := h.seats.list(f, limit, offset)
	return map[string]any{"seats": seats, "total": total}, nil
}

func (h *Hub) seatGet(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	id := strParam(p["seat_id"])
	if id == "" {
		return nil, jsonrpc.ErrInvalidParams("seat_id required")
	}
	s, ok := h.seats.get(id)
	if !ok {
		return nil, jsonrpc.NewError(jsonrpc.CodeSeatNotFound, "seat.not_found",
			map[string]any{"seat_id": id})
	}
	return map[string]any{"seat": s}, nil
}

// parseExpect decodes the expect profile from params. Malformed known fields
// (non-object expect, non-string-array capabilities/tags, non-object metadata)
// are rejected with -32602; unknown fields are ignored.
func parseExpect(v any) (*SeatExpect, *jsonrpc.Error) {
	if v == nil {
		return nil, nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, jsonrpc.ErrInvalidParams("expect must be an object")
	}
	e := &SeatExpect{}
	if raw, ok := m["capabilities"]; ok && raw != nil {
		e.Capabilities = toStringSlice(raw)
		if e.Capabilities == nil {
			return nil, jsonrpc.ErrInvalidParams("expect.capabilities must be an array of strings")
		}
	}
	if raw, ok := m["tags"]; ok && raw != nil {
		e.Tags = toStringSlice(raw)
		if e.Tags == nil {
			return nil, jsonrpc.ErrInvalidParams("expect.tags must be an array of strings")
		}
	}
	if raw, ok := m["metadata"]; ok && raw != nil {
		md, ok := raw.(map[string]any)
		if !ok {
			return nil, jsonrpc.ErrInvalidParams("expect.metadata must be an object")
		}
		e.Metadata = md
	}
	return e, nil
}

// toStringSlice converts a params array into []string. Returns nil when the
// value is not an array of strings (caller maps this to -32602). Accepts both
// []any (decoded JSON-RPC params) and []string (direct Go callers).
func toStringSlice(v any) []string {
	switch arr := v.(type) {
	case []string:
		return append([]string(nil), arr...)
	case []any:
		out := make([]string, 0, len(arr))
		for _, e := range arr {
			s, ok := e.(string)
			if !ok {
				return nil
			}
			out = append(out, s)
		}
		return out
	}
	return nil
}

func (h *Hub) seatCreate(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	name := strParam(p["name"])
	policy := strParam(p["bind_policy"])
	if policy == "" {
		policy = SeatBindManual
	}
	if policy != SeatBindAuto && policy != SeatBindManual {
		return nil, jsonrpc.ErrInvalidParams("bind_policy must be \"auto\" or \"manual\"")
	}
	expect, perr := parseExpect(p["expect"])
	if perr != nil {
		return nil, perr
	}
	s := h.seats.create(name, policy, expect, callerOwner(cc))
	return map[string]any{"seat": s}, nil
}

func (h *Hub) seatDelete(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	id := strParam(p["seat_id"])
	if id == "" {
		return nil, jsonrpc.ErrInvalidParams("seat_id required")
	}
	s, ok := h.seats.get(id)
	if !ok {
		return nil, jsonrpc.NewError(jsonrpc.CodeSeatNotFound, "seat.not_found",
			map[string]any{"seat_id": id})
	}
	if !mayActOn(cc, s.owner) {
		return nil, errForbidden("seat")
	}
	_, exists, deleted := h.seats.del(id)
	if !exists {
		return nil, jsonrpc.NewError(jsonrpc.CodeSeatNotFound, "seat.not_found",
			map[string]any{"seat_id": id})
	}
	if !deleted {
		// Standing seat: release first (PROTOCOL.md §4.7 lifecycle).
		return nil, jsonrpc.NewError(jsonrpc.CodeSeatOccupied, "seat.occupied",
			map[string]any{"seat_id": id})
	}
	return map[string]any{}, nil
}

func (h *Hub) seatBind(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	seatID := strParam(p["seat_id"])
	nodeID := strParam(p["node_id"])
	if seatID == "" || nodeID == "" {
		return nil, jsonrpc.ErrInvalidParams("seat_id and node_id required")
	}
	s, ok := h.seats.get(seatID)
	if !ok {
		return nil, jsonrpc.NewError(jsonrpc.CodeSeatNotFound, "seat.not_found",
			map[string]any{"seat_id": seatID})
	}
	if !mayActOn(cc, s.owner) {
		return nil, errForbidden("seat")
	}
	if s.Kind == SeatKindStanding {
		return nil, jsonrpc.NewError(jsonrpc.CodeSeatOccupied, "seat.occupied",
			map[string]any{"seat_id": seatID})
	}
	n, nok := h.nodes.get(nodeID)
	if !nok {
		return nil, jsonrpc.NewError(jsonrpc.CodeNodeNotFound, "node.not_found",
			map[string]any{"node_id": nodeID})
	}
	if !s.Expect.matches(n) {
		return nil, jsonrpc.NewError(jsonrpc.CodeSeatExpectMismatch, "seat.expect.mismatch",
			map[string]any{"seat_id": seatID, "node_id": nodeID})
	}
	out := h.seats.fill(seatID, nodeID)
	if out == nil {
		// Concurrent fill/delete between the check above and now.
		return nil, jsonrpc.NewError(jsonrpc.CodeSeatOccupied, "seat.occupied",
			map[string]any{"seat_id": seatID})
	}
	return map[string]any{"seat": out}, nil
}

func (h *Hub) seatUnbind(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	id := strParam(p["seat_id"])
	if id == "" {
		return nil, jsonrpc.ErrInvalidParams("seat_id required")
	}
	s, ok := h.seats.get(id)
	if !ok {
		return nil, jsonrpc.NewError(jsonrpc.CodeSeatNotFound, "seat.not_found",
			map[string]any{"seat_id": id})
	}
	if !mayActOn(cc, s.owner) {
		return nil, errForbidden("seat")
	}
	out := h.seats.release(id) // idempotent: vacant seats return unchanged
	if out == nil {
		return nil, jsonrpc.NewError(jsonrpc.CodeSeatNotFound, "seat.not_found",
			map[string]any{"seat_id": id})
	}
	return map[string]any{"seat": out}, nil
}

// ---------------------------------------------------------------------------
// Hub-level seat event publishers

func (h *Hub) publishSeatCreated(s *Seat) {
	data, _ := json.Marshal(s)
	h.events.publish(&Event{Type: "seat.created", Data: data})
}

func (h *Hub) publishSeatFilled(s *Seat) {
	data, _ := json.Marshal(s)
	var nid *string
	if s.NodeID != nil {
		nid = strPtr(*s.NodeID)
	}
	h.events.publish(&Event{Type: "seat.filled", NodeID: nid, Data: data})
	h.nodeSeatsChanged(s)
}

func (h *Hub) publishSeatReleased(s *Seat, exNodeID string) {
	data, _ := json.Marshal(s)
	var nid *string
	if exNodeID != "" {
		nid = strPtr(exNodeID)
	}
	h.events.publish(&Event{Type: "seat.released", NodeID: nid, Data: data})
	if exNodeID != "" {
		h.nodeSeatsChanged(s)
	}
}

func (h *Hub) publishSeatDeleted() {
	h.events.publish(&Event{Type: "seat.deleted", Data: json.RawMessage(`{}`)})
}

// nodeSeatsChanged publishes node.updated for the occupier so subscribers see
// the seat_id change; best-effort (node may be gone).
func (h *Hub) nodeSeatsChanged(s *Seat) {
	if s.NodeID == nil {
		return
	}
	if n, ok := h.nodes.get(*s.NodeID); ok {
		h.publishNodeUpdated(n)
	}
}

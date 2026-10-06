package hub

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"musubi/engine/internal/jsonrpc"
)

// Known event types (PROTOCOL.md §4.5). Unknown types are rejected at
// subscribe time (-32031) and never produced by the engine.
var knownEventTypes = map[string]bool{
	"node.registered":     true,
	"node.updated":        true,
	"node.status.changed": true,
	"node.unregistered":   true,
	"command.accepted":    true,
	"command.started":     true,
	"command.result":      true,
	"command.failed":      true,
	"command.cancelled":   true,
	"task.created":        true,
	"task.progress":       true,
	"task.updated":        true,
	"task.completed":      true,
	"task.failed":         true,
	"task.cancelled":      true,
}

const (
	// eventHistorySize bounds the in-memory cursor history for long-polling.
	eventHistorySize = 10000
	// eventSubBuf is the per-subscription delivery buffer; a full buffer
	// drops events (with a log line) instead of blocking publishers.
	eventSubBuf = 256
)

// Event is a server-pushed event (schema event.json). node_id/task_id/
// command_id are nullable (null when unset). Data is an opaque object.
type Event struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Time      string          `json:"time"`
	NodeID    *string         `json:"node_id"`
	TaskID    *string         `json:"task_id"`
	CommandID *string         `json:"command_id"`
	Data      json.RawMessage `json:"data,omitempty"`

	cursor int64 `json:"-"` // monotonic sequence for long-polling
}

// Subscription delivers matching events to one consumer (SSE connection,
// long-poll session or JSON-RPC subscriber).
type Subscription struct {
	ID         string
	EventTypes map[string]bool // non-empty = allow-list (intersection)
	NodeID     string          // filter: only events for this node
	TaskID     string          // filter: only events for this task

	ch   chan *Event
	done chan struct{}
	once sync.Once
}

func (s *Subscription) matches(ev *Event) bool {
	if len(s.EventTypes) > 0 && !s.EventTypes[ev.Type] {
		return false
	}
	if s.NodeID != "" {
		if ev.NodeID == nil || *ev.NodeID != s.NodeID {
			return false
		}
	}
	if s.TaskID != "" {
		if ev.TaskID == nil || *ev.TaskID != s.TaskID {
			return false
		}
	}
	return true
}

// C exposes the delivery channel for SSE-style consumers.
func (s *Subscription) C() <-chan *Event { return s.ch }

// Done is closed when the subscription is closed (unsubscribe or disconnect).
func (s *Subscription) Done() <-chan struct{} { return s.done }

// Read waits for the next event, returning false when closed or ctx done.
func (s *Subscription) Read(ctx context.Context) (*Event, bool) {
	select {
	case ev := <-s.ch:
		return ev, true
	case <-s.done:
		return nil, false
	case <-ctx.Done():
		return nil, false
	}
}

type eventBus struct {
	log *slog.Logger

	mu      sync.RWMutex
	nextID  atomic.Int64 // ev-N
	cursor  atomic.Int64 // monotonic event cursor for /v1/poll
	history []*Event
	subs    map[string]*Subscription
	notify  chan struct{} // wake-up for long-pollers
}

func newEventBus(log *slog.Logger) *eventBus {
	return &eventBus{
		log:    log,
		subs:   map[string]*Subscription{},
		notify: make(chan struct{}, 1),
	}
}

// newSub registers a subscription with id and filters.
func (b *eventBus) newSub(id string, eventTypes []string, nodeID, taskID string) *Subscription {
	sub := &Subscription{
		ID:         id,
		EventTypes: map[string]bool{},
		NodeID:     nodeID,
		TaskID:     taskID,
		ch:         make(chan *Event, eventSubBuf),
		done:       make(chan struct{}),
	}
	for _, t := range eventTypes {
		sub.EventTypes[t] = true
	}
	b.mu.Lock()
	b.subs[id] = sub
	b.mu.Unlock()
	return sub
}

// closeSub removes and closes a subscription. Returns false when unknown.
func (b *eventBus) closeSub(id string) bool {
	b.mu.Lock()
	s, ok := b.subs[id]
	if ok {
		delete(b.subs, id)
	}
	b.mu.Unlock()
	if !ok {
		return false
	}
	s.once.Do(func() { close(s.done) })
	return true
}

// getSub returns the live subscription for id, or nil when unknown.
func (b *eventBus) getSub(id string) *Subscription {
	b.mu.Lock()
	s := b.subs[id]
	b.mu.Unlock()
	return s
}

// closeAll closes every subscription (used on WS disconnect cleanup paths and
// shutdown).
func (b *eventBus) closeAll() {
	b.mu.Lock()
	subs := make([]*Subscription, 0, len(b.subs))
	for _, s := range b.subs {
		subs = append(subs, s)
	}
	b.subs = map[string]*Subscription{}
	b.mu.Unlock()
	for _, s := range subs {
		s.once.Do(func() { close(s.done) })
	}
}

// publish assigns id/cursor/time, appends to history and delivers
// non-blockingly to matching subscriptions (full buffer drops + logs).
func (b *eventBus) publish(ev *Event) {
	if ev.Time == "" {
		ev.Time = nowTS()
	}
	ev.ID = fmt.Sprintf("ev-%d", b.nextID.Add(1))
	b.mu.Lock()
	ev.cursor = b.cursor.Add(1)
	b.history = append(b.history, ev)
	if len(b.history) > eventHistorySize {
		b.history = b.history[len(b.history)-eventHistorySize:]
	}
	subs := make([]*Subscription, 0, len(b.subs))
	for _, s := range b.subs {
		subs = append(subs, s)
	}
	b.mu.Unlock()
	for _, s := range subs {
		if !s.matches(ev) {
			continue
		}
		select {
		case s.ch <- ev:
		case <-s.done:
		default:
			b.log.Warn("event subscription buffer full, dropping event",
				"subscription", s.ID, "event", ev.ID)
		}
	}
	select {
	case b.notify <- struct{}{}:
	default:
	}
}

// eventsAfter returns events with cursor > cursor (already snapshotted: the
// events are immutable once published). Never nil so JSON renders [] not null.
func (b *eventBus) eventsAfter(cursor int64) []*Event {
	b.mu.RLock()
	defer b.mu.RUnlock()
	var out []*Event
	for _, ev := range b.history {
		if ev.cursor > cursor {
			out = append(out, ev)
		}
	}
	if out == nil {
		out = []*Event{}
	}
	return out
}

// poll blocks until events past cursor exist, the timeout elapses or the
// context is cancelled. Returns the new cursor and events. An empty result is
// always an empty slice (never nil) so JSON renders "events": [] instead of
// null, which would break strict decoders in other languages.
func (b *eventBus) poll(ctx context.Context, cursor int64, timeout time.Duration) (int64, []*Event, error) {
	deadline := time.Now().Add(timeout)
	empty := func() []*Event { return []*Event{} }
	for {
		events := b.eventsAfter(cursor)
		if len(events) > 0 {
			return events[len(events)-1].cursor, events, nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return cursor, empty(), nil
		}
		t := time.NewTimer(remaining)
		select {
		case <-b.notify:
			t.Stop()
		case <-t.C:
			return cursor, empty(), nil
		case <-ctx.Done():
			t.Stop()
			return cursor, empty(), ctx.Err()
		}
	}
}

// ---------------------------------------------------------------------------
// Hub-level event helpers (used by nodes/commands/tasks and transports)

func (h *Hub) NewEventSub(eventTypes []string, nodeID, taskID string) *Subscription {
	return h.events.newSub(h.newSubID(), eventTypes, nodeID, taskID)
}

func (h *Hub) CloseEventSub(sub *Subscription) {
	if sub != nil {
		h.events.closeSub(sub.ID)
	}
}

func (h *Hub) CloseEventSubByID(id string) {
	h.events.closeSub(id)
}

// GetEventSubByID returns an existing subscription by id, or nil. Transports
// that stream an already-established subscription (SSE with ?subscription_id)
// use it instead of creating a second one.
func (h *Hub) GetEventSubByID(id string) *Subscription {
	return h.events.getSub(id)
}

// PollEvents is the long-poll transport entry point (GET /v1/poll).
func (h *Hub) PollEvents(ctx context.Context, cursor int64, timeout time.Duration) (int64, []*Event, error) {
	return h.events.poll(ctx, cursor, timeout)
}

func (h *Hub) publishNodeRegistered(n *Node) {
	data, _ := json.Marshal(n)
	h.events.publish(&Event{Type: "node.registered", NodeID: strPtr(n.ID), Data: data})
}

func (h *Hub) publishNodeUpdated(n *Node) {
	data, _ := json.Marshal(n)
	h.events.publish(&Event{Type: "node.updated", NodeID: strPtr(n.ID), Data: data})
}

func (h *Hub) publishNodeStatus(id, status string) {
	data, _ := json.Marshal(map[string]any{"status": status})
	h.events.publish(&Event{Type: "node.status.changed", NodeID: strPtr(id), Data: data})
}

func (h *Hub) publishNodeUnregistered(id string) {
	h.events.publish(&Event{Type: "node.unregistered", NodeID: strPtr(id), Data: json.RawMessage(`{}`)})
}

func (h *Hub) publishCommandAccepted(c *Command) {
	data, _ := json.Marshal(c)
	h.events.publish(&Event{Type: "command.accepted", NodeID: strPtr(c.NodeID), CommandID: strPtr(c.ID), Data: data})
}

func (h *Hub) publishCommandStarted(c *Command) {
	data, _ := json.Marshal(map[string]any{"command_id": c.ID})
	h.events.publish(&Event{Type: "command.started", NodeID: strPtr(c.NodeID), CommandID: strPtr(c.ID), Data: data})
}

// publishCommandTerminal picks the terminal event type by status:
// succeeded -> command.result, failed/timeout -> command.failed,
// cancelled -> command.cancelled.
func (h *Hub) publishCommandTerminal(c *Command) {
	data, _ := json.Marshal(c)
	ev := &Event{Type: "command.result", NodeID: strPtr(c.NodeID), CommandID: strPtr(c.ID), Data: data}
	switch c.Status {
	case cmdFailed, cmdTimeout:
		ev.Type = "command.failed"
	case cmdCancelled:
		ev.Type = "command.cancelled"
	}
	h.events.publish(ev)
}

func (h *Hub) publishTaskCreated(t *Task) {
	data, _ := json.Marshal(t)
	h.events.publish(&Event{Type: "task.created", TaskID: strPtr(t.ID), Data: data})
}

func (h *Hub) publishTaskProgress(id string, progress float64) {
	data, _ := json.Marshal(map[string]any{"progress": progress})
	h.events.publish(&Event{Type: "task.progress", TaskID: strPtr(id), Data: data})
}

func (h *Hub) publishTaskUpdated(t *Task) {
	data, _ := json.Marshal(t)
	h.events.publish(&Event{Type: "task.updated", TaskID: strPtr(t.ID), Data: data})
}

// publishTaskTerminal: succeeded/partial -> task.completed, failed ->
// task.failed, cancelled -> task.cancelled (status field disambiguates).
func (h *Hub) publishTaskTerminal(t *Task) {
	data, _ := json.Marshal(t)
	ev := &Event{Type: "task.completed", TaskID: strPtr(t.ID), Data: data}
	switch t.Status {
	case taskFailed:
		ev.Type = "task.failed"
	case taskCancelled:
		ev.Type = "task.cancelled"
	}
	h.events.publish(ev)
}

// ---------------------------------------------------------------------------
// event.* RPC handlers

// eventSubscribe subscribes with an optional filter: event_types, node_id
// and task_id intersect. Empty event_types = all events (§5).
func (h *Hub) eventSubscribe(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	var types []string
	if raw, ok := p["event_types"].([]any); ok {
		for _, t := range raw {
			if s, ok := t.(string); ok {
				types = append(types, s)
			}
		}
	}
	return h.makeSubscription(cc, types, strParam(p["node_id"]), strParam(p["task_id"]))
}

func (h *Hub) eventUnsubscribe(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
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

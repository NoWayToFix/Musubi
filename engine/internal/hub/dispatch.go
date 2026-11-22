package hub

import (
	"sync"
	"time"

	"musubi/engine/internal/jsonrpc"
)

// Remote dispatch (PROTOCOL.md §5.3): commands addressed to a real node are
// queued and claimed by that node with command.pull, then completed with
// command.complete. This keeps the engine transport-agnostic and pull-based:
// a node needs nothing but JSON-RPC over any supported transport.
//
// State machine per remote command:
//
//	pending --pull--> running --complete--> succeeded|failed|cancelled|timeout
//	   |                  |
//	   +----- timeout/cancel/node-offline ---> terminal
//
// Busy refcounts are acquired at claim time (when the node actually starts
// work), not at enqueue time, so a queued command does not make a node look
// busy. Exactly one remover (complete, timeout, cancel or drain) releases the
// refcount: dispatcher.remove is the arbiter.
type nodeQueue struct {
	pending []*Command
	running map[string]struct{}
	notify  chan struct{}
}

type dispatcher struct {
	hub   *Hub
	mu    sync.Mutex
	nodes map[string]*nodeQueue
}

func newDispatcher(h *Hub) *dispatcher {
	return &dispatcher{hub: h, nodes: map[string]*nodeQueue{}}
}

// queueLocked returns (creating if needed) the queue for a node.
func (d *dispatcher) queueLocked(nodeID string) *nodeQueue {
	q := d.nodes[nodeID]
	if q == nil {
		q = &nodeQueue{running: map[string]struct{}{}, notify: make(chan struct{}, 1)}
		d.nodes[nodeID] = q
	}
	return q
}

// enqueue appends a pending command (deduplicated by id) and wakes a puller.
func (d *dispatcher) enqueue(c *Command) {
	d.mu.Lock()
	q := d.queueLocked(c.NodeID)
	for _, p := range q.pending {
		if p.ID == c.ID {
			d.mu.Unlock()
			return
		}
	}
	if _, ok := q.running[c.ID]; ok {
		d.mu.Unlock()
		return
	}
	q.pending = append(q.pending, c)
	d.mu.Unlock()
	d.wake(c.NodeID)
}

func (d *dispatcher) wake(nodeID string) {
	d.mu.Lock()
	q := d.nodes[nodeID]
	d.mu.Unlock()
	if q != nil {
		select {
		case q.notify <- struct{}{}:
		default:
		}
	}
}

// claim moves up to limit pending commands to running and returns their
// snapshots. Commands that became terminal while queued are dropped. If the
// node cannot be acquired (it went offline), the command stays queued for the
// offline drain to terminalise it.
func (d *dispatcher) claim(nodeID string, limit int) []*Command {
	if limit <= 0 {
		limit = 1
	}
	d.mu.Lock()
	q := d.queueLocked(nodeID)
	out := make([]*Command, 0, limit)
	keep := q.pending[:0]
	for _, c := range q.pending {
		if len(out) >= limit {
			keep = append(keep, c)
			continue
		}
		snap := d.hub.cmds.snapshot(c.ID)
		if snap == nil || isTerminal(snap.Status) {
			continue
		}
		if !d.hub.nodes.tryAcquire(nodeID) {
			keep = append(keep, c)
			continue
		}
		if !d.hub.cmds.start(c.ID) {
			d.hub.nodes.release(nodeID)
			continue
		}
		q.running[c.ID] = struct{}{}
		out = append(out, d.hub.cmds.snapshot(c.ID))
	}
	q.pending = keep
	d.mu.Unlock()
	return out
}

// wait blocks for at most timeout, or until a puller is woken by enqueue.
func (d *dispatcher) wait(nodeID string, timeout time.Duration) {
	if timeout <= 0 {
		return
	}
	d.mu.Lock()
	q := d.queueLocked(nodeID)
	ch := q.notify
	d.mu.Unlock()
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-ch:
	case <-t.C:
	}
}

// remove takes a command out of the queue. It reports the node and whether the
// command was running (and therefore holds a busy refcount to release). The
// first remover wins; later removers get found=false and must not release.
func (d *dispatcher) remove(id string) (nodeID string, running, found bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for nid, q := range d.nodes {
		for i, c := range q.pending {
			if c.ID == id {
				q.pending = append(q.pending[:i], q.pending[i+1:]...)
				return nid, false, true
			}
		}
		if _, ok := q.running[id]; ok {
			delete(q.running, id)
			return nid, true, true
		}
	}
	return "", false, false
}

// drained is one command removed by drainNode.
type drained struct {
	cmd     *Command
	running bool
}

// drainNode removes all queued work for a node that went offline/unregistered.
// The caller must terminalise each command and release the running refcounts.
func (d *dispatcher) drainNode(nodeID string) []drained {
	d.mu.Lock()
	q := d.nodes[nodeID]
	if q == nil {
		d.mu.Unlock()
		return nil
	}
	out := make([]drained, 0, len(q.pending)+len(q.running))
	for _, c := range q.pending {
		out = append(out, drained{cmd: c, running: false})
	}
	for id := range q.running {
		if c := d.hub.cmds.snapshot(id); c != nil {
			out = append(out, drained{cmd: c, running: true})
		}
	}
	delete(d.nodes, nodeID)
	d.mu.Unlock()
	return out
}

// timeout terminalises a command whose engine-side deadline elapsed.
func (d *dispatcher) timeout(id string) {
	nodeID, running, found := d.remove(id)
	if !found {
		return // completed or cancelled first
	}
	d.hub.cmds.finish(id, cmdTimeout, nil, jsonrpc.NewError(simulatorFailureCode, "command timeout",
		map[string]any{"command_id": id}))
	if running {
		d.hub.nodes.release(nodeID)
	}
}

// ---------------------------------------------------------------------------
// Hub integration

// sendRemote queues a command for a real node. SeatID, when set, records the
// seat that routed the command.
func (h *Hub) sendRemote(nodeID string, seatID *string, method string, params map[string]any, timeout int, ik, owner string) *Command {
	cmd := h.newCommand(nodeID, method, params, timeout, nil)
	cmd.Status = cmdPending
	cmd.SeatID = seatID
	cmd.owner = owner
	cmd.IdempotencyKey = ik
	h.cmds.create(cmd, ik)
	h.startRemoteTimer(cmd)
	h.publishCommandAccepted(cmd)
	h.dispatch.enqueue(cmd)
	return h.cmds.snapshot(cmd.ID)
}

// startRemoteTimer enforces the send-to-terminal deadline. Cancellation and
// completion stop it; the callback re-checks terminal state in finish().
func (h *Hub) startRemoteTimer(c *Command) {
	if c.Timeout <= 0 {
		return
	}
	id := c.ID
	c.timeoutTimer = time.AfterFunc(time.Duration(c.Timeout)*time.Millisecond, func() {
		h.dispatch.timeout(id)
	})
}

// cancelCommand terminalises a command and releases dispatcher ownership. It
// is shared by command.cancel and task.cancel; simulator commands are not in
// the dispatcher and keep their existing execution/cleanup path.
func (h *Hub) cancelCommand(id string) bool {
	nodeID, running, _ := h.dispatch.remove(id)
	if !h.cmds.cancel(id) {
		return false
	}
	if running {
		h.nodes.release(nodeID)
	}
	return true
}

// failNodeCommands terminalises every queued/running command of a node that
// unregistered or was marked offline by the watchdog.
func (h *Hub) failNodeCommands(nodeID string, code int, message string) {
	for _, dc := range h.dispatch.drainNode(nodeID) {
		h.cmds.finish(dc.cmd.ID, cmdFailed, nil, jsonrpc.NewError(code, message,
			map[string]any{"node_id": nodeID}))
		if dc.running {
			h.nodes.release(nodeID)
		}
	}
}

// ---------------------------------------------------------------------------
// command.pull / command.complete

// cmdPull implements command.pull: claim up to limit pending commands for a
// real node, optionally long-polling (wait ms) for work.
func (h *Hub) cmdPull(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	nodeID := strParam(p["node_id"])
	if nodeID == "" {
		return nil, jsonrpc.ErrInvalidParams("node_id required")
	}
	n, ok := h.nodes.get(nodeID)
	if !ok {
		return nil, jsonrpc.NewError(jsonrpc.CodeNodeNotFound, "node.not_found",
			map[string]any{"node_id": nodeID})
	}
	if n.demo {
		return nil, jsonrpc.ErrInvalidParams("demo nodes are executed in-process and cannot pull")
	}
	if !h.mayActOnNode(cc, nodeID) {
		return nil, errForbidden("node")
	}
	if n.Status == StatusOffline || n.Status == StatusUnknown {
		return nil, jsonrpc.NewError(jsonrpc.CodeNodeOffline, "node.offline",
			map[string]any{"node_id": nodeID})
	}
	limit := intParam(p["limit"], 1)
	if limit < 1 {
		limit = 1
	}
	if limit > 32 {
		limit = 32
	}
	wait, werr := msParam(p["wait"], 0)
	if werr != nil {
		return nil, werr
	}
	if wait > 30000 {
		wait = 30000
	}
	deadline := time.Now().Add(time.Duration(wait) * time.Millisecond)
	for {
		cmds := h.dispatch.claim(nodeID, limit)
		if len(cmds) > 0 {
			return map[string]any{"commands": cmds}, nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return map[string]any{"commands": []*Command{}}, nil
		}
		h.dispatch.wait(nodeID, remaining)
	}
}

// cmdComplete implements command.complete: a real node reports the terminal
// state of a command it pulled. Idempotent: completing a terminal command
// returns its current snapshot unchanged.
func (h *Hub) cmdComplete(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	nodeID := strParam(p["node_id"])
	cmdID := strParam(p["command_id"])
	if nodeID == "" || cmdID == "" {
		return nil, jsonrpc.ErrInvalidParams("node_id and command_id are required")
	}
	n, ok := h.nodes.get(nodeID)
	if !ok {
		return nil, jsonrpc.NewError(jsonrpc.CodeNodeNotFound, "node.not_found",
			map[string]any{"node_id": nodeID})
	}
	if n.demo {
		return nil, jsonrpc.ErrInvalidParams("demo nodes are executed in-process and cannot complete commands")
	}
	if !h.mayActOnNode(cc, nodeID) {
		return nil, errForbidden("node")
	}
	c, ok := h.cmds.get(cmdID)
	if !ok || c.NodeID != nodeID {
		// Do not reveal commands belonging to other nodes.
		return nil, jsonrpc.NewError(jsonrpc.CodeCommandNotFound, "command.not_found",
			map[string]any{"command_id": cmdID})
	}
	status := strParam(p["status"])
	switch status {
	case cmdSucceeded, cmdFailed, cmdCancelled, cmdTimeout:
	default:
		return nil, jsonrpc.ErrInvalidParams("status must be succeeded, failed, cancelled or timeout")
	}
	if isTerminal(c.Status) {
		return map[string]any{"command": c}, nil
	}
	_, running, _ := h.dispatch.remove(cmdID)
	var result any
	var rerr *jsonrpc.Error
	if status == cmdSucceeded {
		result = p["result"]
	} else {
		rerr = commandResultError(status, p["error"])
	}
	out := h.cmds.finish(cmdID, status, result, rerr)
	if running {
		h.nodes.release(nodeID)
	}
	return map[string]any{"command": out}, nil
}

// commandResultError builds the error object reported by a node. A well-formed
// {code,message,data} passes through; otherwise a stable default is used.
func commandResultError(status string, v any) *jsonrpc.Error {
	if m, ok := v.(map[string]any); ok {
		msg := strParam(m["message"])
		if msg == "" {
			msg = "command " + status
		}
		return jsonrpc.NewError(intParam(m["code"], simulatorFailureCode), msg, m["data"])
	}
	if status == cmdCancelled {
		return nil
	}
	return jsonrpc.NewError(simulatorFailureCode, "command "+status, nil)
}

package hub

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"musubi/engine/internal/jsonrpc"
)

// Command statuses (PROTOCOL.md §4.2).
const (
	cmdPending   = "pending"
	cmdAccepted  = "accepted"
	cmdRunning   = "running"
	cmdSucceeded = "succeeded"
	cmdFailed    = "failed"
	cmdCancelled = "cancelled"
	cmdTimeout   = "timeout"
)

const (
	defaultCommandTimeoutMs = 30000 // 30s; 0 = no timeout
	idemTTL                 = 5 * time.Minute
	// simulatorFailureCode is the error code for the built-in "fail" method.
	simulatorFailureCode = -32000
)

// Command is a single command sent to a node (schema command.json). result
// and error are mutually exclusive: error non-nil nulls result (and vice
// versa), matching §4.2.
type Command struct {
	ID             string         `json:"id"`
	NodeID         string         `json:"node_id"`
	SeatID         *string        `json:"seat_id,omitempty"` // routing seat, when addressed via seat_id
	Method         string         `json:"method"`
	Params         map[string]any `json:"params,omitempty"`
	Status         string         `json:"status"`
	Result         any            `json:"result"`
	Error          *jsonrpc.Error `json:"error"`
	Timeout        int            `json:"timeout,omitempty"`
	CreatedAt      string         `json:"created_at"`
	StartedAt      string         `json:"started_at,omitempty"`
	FinishedAt     string         `json:"finished_at,omitempty"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
	TaskID         *string        `json:"task_id"`

	cancelCh chan struct{} // closed to cancel (internal)
	done     chan struct{} // closed once terminal (internal; guarded by the registry lock)
	// timeoutTimer enforces the send-to-terminal deadline for remotely
	// dispatched commands (nil for simulator commands, which use contexts).
	timeoutTimer *time.Timer
	// owner is the creator's identity (see callerOwner); empty for
	// anonymous-created commands, which anyone may cancel.
	owner string
}

func isTerminal(s string) bool {
	switch s {
	case cmdSucceeded, cmdFailed, cmdCancelled, cmdTimeout:
		return true
	}
	return false
}

type idemEntry struct {
	commandID string
	expiresAt time.Time
}

type batchEntry struct {
	result    map[string]any
	expiresAt time.Time
}

type commandRegistry struct {
	hub   *Hub
	mu    sync.RWMutex
	cmds  map[string]*Command
	order []string
	idem  map[string]idemEntry  // command-level idempotency key -> command id
	batch map[string]batchEntry // batch-level idempotency key -> first result
}

func newCommandRegistry(h *Hub) *commandRegistry {
	return &commandRegistry{
		hub:   h,
		cmds:  map[string]*Command{},
		idem:  map[string]idemEntry{},
		batch: map[string]batchEntry{},
	}
}

// ---------------------------------------------------------------------------
// Registry operations

func (r *commandRegistry) create(c *Command, idemKey string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cmds[c.ID] = c
	r.order = append(r.order, c.ID)
	if idemKey != "" {
		r.idem[idemKey] = idemEntry{commandID: c.ID, expiresAt: time.Now().Add(idemTTL)}
	}
}

func (r *commandRegistry) get(id string) (*Command, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.cmds[id]
	if !ok {
		return nil, false
	}
	return cloneCommand(c), true
}

// snapshot is get without the ok boolean (for known ids).
func (r *commandRegistry) snapshot(id string) *Command {
	c, _ := r.get(id)
	return c
}

// idempotentReplay returns the first command for a key within its TTL.
func (r *commandRegistry) idempotentReplay(key string) (*Command, bool) {
	r.mu.RLock()
	e, ok := r.idem[key]
	r.mu.RUnlock()
	if !ok {
		return nil, false
	}
	if time.Now().After(e.expiresAt) {
		r.mu.Lock()
		if cur, ok := r.idem[key]; ok && cur.expiresAt.Equal(e.expiresAt) {
			delete(r.idem, key)
		}
		r.mu.Unlock()
		return nil, false
	}
	r.mu.RLock()
	c, ok := r.cmds[e.commandID]
	if ok {
		cl := cloneCommand(c)
		r.mu.RUnlock()
		return cl, true
	}
	r.mu.RUnlock()
	return nil, false
}

func (r *commandRegistry) storeBatchResult(key string, result map[string]any) {
	r.mu.Lock()
	r.batch[key] = batchEntry{result: result, expiresAt: time.Now().Add(idemTTL)}
	r.mu.Unlock()
}

func (r *commandRegistry) batchReplay(key string) (map[string]any, bool) {
	r.mu.RLock()
	e, ok := r.batch[key]
	r.mu.RUnlock()
	if !ok {
		return nil, false
	}
	if time.Now().After(e.expiresAt) {
		r.mu.Lock()
		if cur, ok := r.batch[key]; ok && cur.expiresAt.Equal(e.expiresAt) {
			delete(r.batch, key)
		}
		r.mu.Unlock()
		return nil, false
	}
	return e.result, true
}

func (r *commandRegistry) cleanupIdem() {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	for k, e := range r.idem {
		if now.After(e.expiresAt) {
			delete(r.idem, k)
		}
	}
	for k, e := range r.batch {
		if now.After(e.expiresAt) {
			delete(r.batch, k)
		}
	}
}

// start transitions a command to running and fires command.started. Returns
// false when the command is already terminal (e.g. cancelled pre-execution).
func (r *commandRegistry) start(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.cmds[id]
	if !ok || isTerminal(c.Status) {
		return false
	}
	c.Status = cmdRunning
	c.StartedAt = nowTS()
	cl := cloneCommand(c)
	r.hub.publishCommandStarted(cl)
	return true
}

// finish marks a command terminal. The first terminal status wins, so a
// concurrent cancel cannot be overwritten by a late success.
func (r *commandRegistry) finish(id, status string, result any, rerr *jsonrpc.Error) *Command {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.cmds[id]
	if !ok {
		return nil
	}
	if isTerminal(c.Status) {
		return cloneCommand(c)
	}
	c.Status = status
	c.FinishedAt = nowTS()
	if rerr != nil {
		c.Error = rerr
		c.Result = nil
	} else {
		c.Result = result
		c.Error = nil
	}
	c.stopTimer()
	c.signalDone()
	cl := cloneCommand(c)
	r.hub.publishCommandTerminal(cl)
	return cl
}

// stopTimer releases the remote-dispatch deadline timer, if any.
func (c *Command) stopTimer() {
	if c.timeoutTimer != nil {
		c.timeoutTimer.Stop()
		c.timeoutTimer = nil
	}
}

// cancel aborts a non-terminal command: closes its cancel channel, marks it
// cancelled and fires command.cancelled. Returns false when already terminal.
func (r *commandRegistry) cancel(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.cmds[id]
	if !ok || isTerminal(c.Status) {
		return false
	}
	close(c.cancelCh)
	c.statusCancelledLocked()
	cl := cloneCommand(c)
	r.hub.publishCommandTerminal(cl)
	return true
}

// statusCancelledLocked applies the terminal cancelled status. Caller holds
// the registry write lock.
func (c *Command) statusCancelledLocked() {
	c.Status = cmdCancelled
	c.FinishedAt = nowTS()
	c.stopTimer()
	c.signalDone()
}

// doneChan returns the terminal signal of a command (nil when unknown).
func (r *commandRegistry) doneChan(id string) <-chan struct{} {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if c, ok := r.cmds[id]; ok {
		return c.done
	}
	return nil
}

// waitCommand blocks until the command is terminal (or already was) and
// returns its snapshot.
func (h *Hub) waitCommand(id string) *Command {
	if ch := h.cmds.doneChan(id); ch != nil {
		<-ch
	}
	return h.cmds.snapshot(id)
}

// commandFilter filters command.list: status/node_id/task_id exact match.
type commandFilter struct {
	status string
	nodeID string
	taskID string
}

func (f *commandFilter) match(c *Command) bool {
	if f.status != "" && c.Status != f.status {
		return false
	}
	if f.nodeID != "" && c.NodeID != f.nodeID {
		return false
	}
	if f.taskID != "" && (c.TaskID == nil || *c.TaskID != f.taskID) {
		return false
	}
	return true
}

// list returns filtered, paginated command snapshots plus the pre-pagination
// total (newest first, insertion order is preserved within the page).
func (r *commandRegistry) list(f *commandFilter, limit, offset int) ([]*Command, int) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Command, 0)
	for _, id := range r.order {
		c := r.cmds[id]
		if c == nil || (f != nil && !f.match(c)) {
			continue
		}
		out = append(out, cloneCommand(c))
	}
	total := len(out)
	// Newest first is friendlier for a live console; the protocol leaves
	// ordering unspecified for lists.
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	if offset >= len(out) {
		out = []*Command{}
	} else {
		out = out[offset:]
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, total
}

// statuses snapshots the status of several commands (used by task progress
// and finalisation; takes the registry lock so statuses are consistent).
func (r *commandRegistry) statuses(ids []string) map[string]string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]string, len(ids))
	for _, id := range ids {
		if c, ok := r.cmds[id]; ok {
			out[id] = c.Status
		}
	}
	return out
}

func cloneCommand(c *Command) *Command {
	cp := *c
	if c.Params != nil {
		cp.Params = make(map[string]any, len(c.Params))
		for k, v := range c.Params {
			cp.Params[k] = v
		}
	}
	if c.Error != nil {
		e := *c.Error
		cp.Error = &e
	}
	if c.TaskID != nil {
		t := *c.TaskID
		cp.TaskID = &t
	}
	if c.SeatID != nil {
		s := *c.SeatID
		cp.SeatID = &s
	}
	cp.cancelCh = nil
	cp.done = nil
	cp.timeoutTimer = nil
	return &cp
}

// resolveTarget maps the addressing parameters of a command to a concrete
// node. Exactly one of node_id / seat_id must be given (PROTOCOL.md §4.2):
//   - node_id addresses a node directly;
//   - seat_id addresses the node currently occupying that seat; a vacant seat
//     has no executor and is rejected with -32053.
//
// Both or neither is -32602 (no implicit precedence).
func (h *Hub) resolveTarget(nodeID, seatID string) (string, *string, *jsonrpc.Error) {
	switch {
	case nodeID != "" && seatID != "":
		return "", nil, jsonrpc.ErrInvalidParams("node_id and seat_id are mutually exclusive")
	case nodeID == "" && seatID == "":
		return "", nil, jsonrpc.ErrInvalidParams("node_id or seat_id is required")
	case seatID != "":
		s, ok := h.seats.get(seatID)
		if !ok {
			return "", nil, jsonrpc.NewError(jsonrpc.CodeSeatNotFound, "seat.not_found",
				map[string]any{"seat_id": seatID})
		}
		if s.Kind != SeatKindStanding || s.NodeID == nil {
			return "", &seatID, jsonrpc.NewError(jsonrpc.CodeSeatVacant, "seat.vacant",
				map[string]any{"seat_id": seatID})
		}
		return *s.NodeID, &seatID, nil
	}
	return nodeID, nil, nil
}

// ---------------------------------------------------------------------------
// command.* RPC handlers

// cmdSend implements command.send: async accepted -> running -> terminal.
func (h *Hub) cmdSend(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	method := strParam(p["method"])
	if method == "" {
		return nil, jsonrpc.ErrInvalidParams("method is required")
	}
	timeout, terr := msParam(p["timeout"], defaultCommandTimeoutMs)
	if terr != nil {
		return nil, terr
	}
	params, _ := p["params"].(map[string]any)
	ik := strParam(p["idempotency_key"])

	if ik != "" {
		if c, ok := h.cmds.idempotentReplay(ik); ok {
			return map[string]any{"command": c}, nil
		}
	}
	nodeID, seatID, terr := h.resolveTarget(strParam(p["node_id"]), strParam(p["seat_id"]))
	if terr != nil {
		return nil, terr
	}
	if !h.nodes.accepting(nodeID) {
		return nil, h.nodeBusyOrOfflineError(nodeID)
	}
	owner := callerOwner(cc)
	// Real nodes execute out of process: queue the command for command.pull.
	// Demo nodes keep the in-process simulator path.
	if !h.nodes.isDemo(nodeID) {
		return map[string]any{"command": h.sendRemote(nodeID, seatID, method, params, timeout, ik, owner)}, nil
	}
	// tryAcquire refcounts concurrent commands (nodes.go): it only fails for
	// missing/offline/unknown nodes, never for a busy one.
	if !h.nodes.tryAcquire(nodeID) {
		return nil, h.nodeBusyOrOfflineError(nodeID)
	}
	cmd := h.newCommand(nodeID, method, params, timeout, nil)
	cmd.SeatID = seatID
	cmd.owner = owner
	cmd.IdempotencyKey = ik
	h.cmds.create(cmd, ik)
	h.publishCommandAccepted(cmd)
	snap := h.cmds.snapshot(cmd.ID)
	go func() {
		defer h.nodes.release(nodeID)
		h.runCommand(cmd)
	}()
	return map[string]any{"command": snap}, nil
}

// cmdBatch implements command.batch: synchronous, sequential; failures are
// embedded in each command's error field, never raised as method errors.
func (h *Hub) cmdBatch(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	rawCmds, ok := p["commands"].([]any)
	if !ok {
		return nil, jsonrpc.ErrInvalidParams("commands array required")
	}
	ik := strParam(p["idempotency_key"])
	if ik != "" {
		if cached, ok := h.cmds.batchReplay(ik); ok {
			return cached, nil
		}
	}
	out := make([]any, 0, len(rawCmds))
	for _, rc := range rawCmds {
		pm, ok := rc.(map[string]any)
		if !ok {
			out = append(out, h.failedSpecCommand("", "", nil, 0, "", nil, jsonrpc.ErrInvalidParams("invalid command entry")))
			continue
		}
		method := strParam(pm["method"])
		if method == "" {
			out = append(out, h.failedSpecCommand(strParam(pm["node_id"]), "", nil, 0, "", nil,
				jsonrpc.ErrInvalidParams("method is required")))
			continue
		}
		nodeID, seatID, terr := h.resolveTarget(strParam(pm["node_id"]), strParam(pm["seat_id"]))
		if terr != nil {
			out = append(out, h.failedSpecCommand(strParam(pm["node_id"]), method, nil, 0, "", seatID, terr))
			continue
		}
		timeout, terr := msParam(pm["timeout"], defaultCommandTimeoutMs)
		if terr != nil {
			out = append(out, h.failedSpecCommand(nodeID, method, nil, 0, "", seatID, terr))
			continue
		}
		params, _ := pm["params"].(map[string]any)
		out = append(out, h.sendSync(nodeID, method, params, timeout, nil, seatID, callerOwner(cc)))
	}
	res := map[string]any{"commands": out, "task_id": nil}
	if ik != "" {
		h.cmds.storeBatchResult(ik, res)
	}
	return res, nil
}

// cmdCancel implements command.cancel; terminal commands are rejected -32014.
// A command created by another authenticated session is rejected -32004;
// anonymous-created commands (empty owner) stay cancellable by anyone.
func (h *Hub) cmdCancel(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	id := strParam(p["command_id"])
	if id == "" {
		return nil, jsonrpc.ErrInvalidParams("command_id required")
	}
	c, ok := h.cmds.get(id)
	if !ok {
		return nil, jsonrpc.NewError(jsonrpc.CodeCommandNotFound, "command.not_found",
			map[string]any{"command_id": id})
	}
	if !mayActOn(cc, c.owner) {
		return nil, errForbidden("command")
	}
	if isTerminal(c.Status) {
		return nil, jsonrpc.NewError(jsonrpc.CodeCommandCancelRej, "command.cancel.rejected",
			map[string]any{"command_id": id, "status": c.Status})
	}
	h.cancelCommand(id)
	c, _ = h.cmds.get(id)
	return map[string]any{"command": c}, nil
}

// cmdGet implements command.get (additive read method, mirrors task.get).
func (h *Hub) cmdGet(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	id := strParam(p["command_id"])
	if id == "" {
		return nil, jsonrpc.ErrInvalidParams("command_id required")
	}
	c, ok := h.cmds.get(id)
	if !ok {
		return nil, jsonrpc.NewError(jsonrpc.CodeCommandNotFound, "command.not_found",
			map[string]any{"command_id": id})
	}
	return map[string]any{"command": c}, nil
}

// cmdList implements command.list (additive read method, mirrors task.list).
func (h *Hub) cmdList(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	limit, offset := clampPage(intParam(p["limit"], 100), intParam(p["offset"], 0))
	var f *commandFilter
	if raw, ok := p["filter"].(map[string]any); ok {
		f = &commandFilter{
			status: strParam(raw["status"]),
			nodeID: strParam(raw["node_id"]),
			taskID: strParam(raw["task_id"]),
		}
	}
	cmds, total := h.cmds.list(f, limit, offset)
	return map[string]any{"commands": cmds, "total": total}, nil
}

// ---------------------------------------------------------------------------
// Execution

func (h *Hub) newCommand(nodeID, method string, params map[string]any, timeout int, taskID *string) *Command {
	return &Command{
		ID:        randomID("c-"),
		NodeID:    nodeID,
		Method:    method,
		Params:    params,
		Status:    cmdAccepted,
		Timeout:   timeout,
		CreatedAt: nowTS(),
		TaskID:    taskID,
		cancelCh:  make(chan struct{}),
		done:      make(chan struct{}),
	}
}

// signalDone closes the command's terminal signal. Callers hold the command
// registry write lock (or are single-threaded restore code), so a nil check is
// enough: terminal transitions happen at most once per command.
func (c *Command) signalDone() {
	if c.done != nil {
		close(c.done)
		c.done = nil
	}
}

// runCommand drives a command to its terminal state. The caller owns the
// node busy flag and must release it when this returns.
func (h *Hub) runCommand(cmd *Command) *Command {
	if !h.cmds.start(cmd.ID) {
		return h.cmds.snapshot(cmd.ID)
	}
	done := make(chan struct{})
	var status string
	var result any
	var rerr *jsonrpc.Error
	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// Propagate cancellation and timeout into the executor context.
		go func() {
			select {
			case <-cmd.cancelCh:
				cancel()
			case <-ctx.Done():
			}
		}()
		if cmd.Timeout > 0 {
			t := time.AfterFunc(time.Duration(cmd.Timeout)*time.Millisecond, cancel)
			defer t.Stop()
		}
		status, result, rerr = h.execNodeCommand(ctx, cmd)
		close(done)
	}()
	<-done
	// A concurrent cancel wins: finish() keeps the first terminal status.
	return h.cmds.finish(cmd.ID, status, result, rerr)
}

// sendSync creates and executes one command to completion (command.batch and
// task commands). Node problems are embedded in the command's error field.
func (h *Hub) sendSync(nodeID, method string, params map[string]any, timeout int, taskID *string, seatID *string, owner string) *Command {
	if !h.nodes.accepting(nodeID) {
		code, msg, data := h.nodeBusyOrOfflineCode(nodeID)
		c := h.newCommand(nodeID, method, params, timeout, taskID)
		c.SeatID = seatID
		c.owner = owner
		h.cmds.create(c, "")
		h.cmds.finish(c.ID, cmdFailed, nil, jsonrpc.NewError(code, msg, data))
		return h.cmds.snapshot(c.ID)
	}
	// command.batch is synchronous: for a real node, wait for command.complete
	// (bounded by the command timeout; 0 means no deadline).
	if !h.nodes.isDemo(nodeID) {
		sent := h.sendRemote(nodeID, seatID, method, params, timeout, "", owner)
		return h.waitCommand(sent.ID)
	}
	if !h.nodes.tryAcquire(nodeID) {
		code, msg, data := h.nodeBusyOrOfflineCode(nodeID)
		c := h.newCommand(nodeID, method, params, timeout, taskID)
		c.SeatID = seatID
		c.owner = owner
		h.cmds.create(c, "")
		h.cmds.finish(c.ID, cmdFailed, nil, jsonrpc.NewError(code, msg, data))
		return h.cmds.snapshot(c.ID)
	}
	defer h.nodes.release(nodeID)
	cmd := h.newCommand(nodeID, method, params, timeout, taskID)
	cmd.SeatID = seatID
	cmd.owner = owner
	h.cmds.create(cmd, "")
	h.publishCommandAccepted(cmd)
	return h.runCommand(cmd)
}

// failedSpecCommand builds a command that failed before touching any node
// (malformed batch entries).
func (h *Hub) failedSpecCommand(nodeID, method string, params map[string]any, timeout int, owner string, seatID *string, rerr *jsonrpc.Error) *Command {
	c := h.newCommand(nodeID, method, params, timeout, nil)
	c.SeatID = seatID
	c.owner = owner
	h.cmds.create(c, "")
	h.cmds.finish(c.ID, cmdFailed, nil, rerr)
	return h.cmds.snapshot(c.ID)
}

func (h *Hub) nodeBusyOrOfflineError(id string) *jsonrpc.Error {
	code, msg, data := h.nodeBusyOrOfflineCode(id)
	return jsonrpc.NewError(code, msg, data)
}

// nodeBusyOrOfflineCode resolves the error for a node that cannot accept a
// command: missing -> -32010 node.not_found, offline -> -32011 node.offline
// (PROTOCOL.md §7.2). The final -32012 node.busy return is currently
// unreachable: the only gate is tryAcquire, which refcounts concurrent
// commands and only fails for missing/offline/unknown nodes. It is kept
// defensively in case tryAcquire ever becomes exclusive again.
func (h *Hub) nodeBusyOrOfflineCode(id string) (int, string, map[string]any) {
	st, ok := h.nodes.status(id)
	if !ok {
		return jsonrpc.CodeNodeNotFound, "node.not_found", map[string]any{"node_id": id}
	}
	if st == StatusOffline || st == StatusUnknown {
		return jsonrpc.CodeNodeOffline, "node.offline", map[string]any{"node_id": id}
	}
	return jsonrpc.CodeNodeBusy, "node.busy", map[string]any{"node_id": id}
}

// execNodeCommand runs one node method on the built-in simulated executor.
// The -demo nodes support exactly echo / fail / sleep / shell.
func (h *Hub) execNodeCommand(ctx context.Context, cmd *Command) (string, any, *jsonrpc.Error) {
	switch cmd.Method {
	case "echo":
		// echo returns the params object verbatim.
		return cmdSucceeded, cmd.Params, nil
	case "fail":
		return cmdFailed, nil, jsonrpc.NewError(simulatorFailureCode, "simulated failure", nil)
	case "sleep":
		// msParam clamps to [0, maxMs]: a raw int64 ms would overflow
		// time.Duration arithmetic and fire immediately.
		ms, merr := msParam(cmd.Params["ms"], 0)
		if merr != nil {
			return cmdFailed, nil, merr
		}
		select {
		case <-time.After(time.Duration(ms) * time.Millisecond):
			return cmdSucceeded, map[string]any{"slept": ms}, nil
		case <-ctx.Done():
			return h.abortedStatus(cmd), nil, nil
		}
	case "shell":
		// The built-in executor is a simulator. Running host commands is an
		// explicit operator opt-in (-allow-shell), not a default capability.
		if !h.cfg.AllowShell {
			return cmdFailed, nil, jsonrpc.NewError(jsonrpc.CodeFeatureDisabled,
				"feature.disabled", map[string]any{
					"feature": "shell",
					"hint":    "restart hubd with -allow-shell to enable the demo shell executor",
				})
		}
		return h.execShell(ctx, cmd)
	default:
		return cmdFailed, nil, jsonrpc.NewError(jsonrpc.CodeMethodNotFound, "method not found",
			map[string]any{"method": cmd.Method})
	}
}

// abortedStatus distinguishes cancellation from timeout after ctx.Done.
func (h *Hub) abortedStatus(cmd *Command) string {
	select {
	case <-cmd.cancelCh:
		return cmdCancelled
	default:
		return cmdTimeout
	}
}

// execShell runs params.cmd as a structured argv via os/exec, killing the
// process on cancellation or timeout (CommandContext).
//
// Hardening (on top of the -allow-shell opt-in):
//   - a non-empty executable allowlist is REQUIRED (-shell-allow);
//   - only argv arrays are accepted — a string would be handed to "sh -c",
//     whose contents the allowlist cannot inspect safely;
//   - argv[0]'s basename must be in the allowlist;
//   - every accepted execution is written to the audit log.
func (h *Hub) execShell(ctx context.Context, cmd *Command) (string, any, *jsonrpc.Error) {
	if len(h.cfg.ShellAllowlist) == 0 {
		return cmdFailed, nil, jsonrpc.NewError(jsonrpc.CodeFeatureDisabled,
			"feature.disabled", map[string]any{
				"feature": "shell",
				"reason":  "allowlist_empty",
				"hint":    "restart hubd with -shell-allow <bin>[,<bin>...] to allow specific executables",
			})
	}
	args, rerr := shellArgs(cmd.Params["cmd"])
	if rerr != nil {
		return cmdFailed, nil, rerr
	}
	bin := filepath.Base(args[0])
	allowed := false
	for _, a := range h.cfg.ShellAllowlist {
		if a == bin {
			allowed = true
			break
		}
	}
	if !allowed {
		h.log.Warn("shell.exec denied: executable not allow-listed",
			"node", cmd.NodeID, "command", bin, "argv", args)
		return cmdFailed, nil, jsonrpc.NewError(jsonrpc.CodeFeatureDisabled,
			"feature.disabled", map[string]any{
				"feature":   "shell",
				"reason":    "command_not_allowed",
				"command":   bin,
				"allowlist": h.cfg.ShellAllowlist,
			})
	}
	// Audit trail: who asked for what, before anything runs.
	h.log.Info("shell.exec", "node", cmd.NodeID, "command", bin, "argv", args, "owner", cmd.owner)

	c := exec.CommandContext(ctx, args[0], args[1:]...)
	var stdout, stderr bytes.Buffer
	c.Stdout = &stdout
	c.Stderr = &stderr
	// Bound how long Wait blocks once the context is done. Killing the direct
	// child does not kill its grandchildren, so a script such as
	// `sh -c "sleep 999999"` keeps the inherited stdout/stderr pipes open and
	// would otherwise defeat the command timeout entirely.
	c.WaitDelay = 500 * time.Millisecond
	err := c.Run()
	if err != nil {
		// runCommand implements the deadline as context.WithCancel +
		// time.AfterFunc(cancel), so ctx.Err() is always context.Canceled and
		// cannot tell a timeout from an explicit cancel. abortedStatus reads
		// the command's cancel channel, which is the authoritative
		// discriminator: only an explicit command.cancel closes it.
		if ctx.Err() != nil {
			if h.abortedStatus(cmd) == cmdCancelled {
				return cmdCancelled, nil, nil
			}
			return cmdTimeout, nil, jsonrpc.NewError(simulatorFailureCode, "command timeout", nil)
		}
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return cmdFailed, nil, jsonrpc.NewError(simulatorFailureCode,
				fmt.Sprintf("exit status %d", ee.ExitCode()),
				map[string]any{"stdout": stdout.String(), "stderr": stderr.String()})
		}
		return cmdFailed, nil, jsonrpc.NewError(simulatorFailureCode, err.Error(), nil)
	}
	return cmdSucceeded, map[string]any{"exit_code": 0, "stdout": stdout.String(), "stderr": stderr.String()}, nil
}

// shellArgs validates and normalises params.cmd into a non-empty argv slice.
// String commands are rejected on purpose: the executor never invokes a shell,
// so the allowlist checks the actual binary instead of a script body.
func shellArgs(v any) ([]string, *jsonrpc.Error) {
	var args []string
	switch c := v.(type) {
	case []any:
		args = make([]string, 0, len(c))
		for _, a := range c {
			s, ok := a.(string)
			if !ok {
				return nil, jsonrpc.ErrInvalidParams("cmd array must contain only strings")
			}
			args = append(args, s)
		}
	case []string:
		args = append([]string(nil), c...)
	case string:
		return nil, jsonrpc.NewError(jsonrpc.CodeInvalidParams, "invalid params: shell.cmd must be an argv array",
			map[string]any{"reason": "string_not_allowed", "hint": `use ["program","arg1",...] instead of "program arg1 ..."`})
	default:
		return nil, jsonrpc.ErrInvalidParams("cmd must be an array of strings")
	}
	if len(args) == 0 || args[0] == "" {
		return nil, jsonrpc.ErrInvalidParams("cmd array must start with a non-empty executable")
	}
	return args, nil
}

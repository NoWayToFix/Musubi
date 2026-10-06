package hub

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
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
	cl := cloneCommand(c)
	r.hub.publishCommandTerminal(cl)
	return cl
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
	c.Status = cmdCancelled
	c.FinishedAt = nowTS()
	cl := cloneCommand(c)
	r.hub.publishCommandTerminal(cl)
	return true
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
	cp.cancelCh = nil
	return &cp
}

// ---------------------------------------------------------------------------
// command.* RPC handlers

// cmdSend implements command.send: async accepted -> running -> terminal.
func (h *Hub) cmdSend(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	nodeID := strParam(p["node_id"])
	method := strParam(p["method"])
	if nodeID == "" || method == "" {
		return nil, jsonrpc.ErrInvalidParams("node_id and method are required")
	}
	timeout := intParam(p["timeout"], defaultCommandTimeoutMs)
	if timeout < 0 {
		timeout = defaultCommandTimeoutMs
	}
	params, _ := p["params"].(map[string]any)
	ik := strParam(p["idempotency_key"])

	if ik != "" {
		if c, ok := h.cmds.idempotentReplay(ik); ok {
			return map[string]any{"command": c}, nil
		}
	}
	// Reserve the node atomically so concurrent sends see busy (-32012).
	if !h.nodes.tryAcquire(nodeID) {
		return nil, h.nodeBusyOrOfflineError(nodeID)
	}
	cmd := h.newCommand(nodeID, method, params, timeout, nil)
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
			out = append(out, h.failedSpecCommand("", "", nil, 0, jsonrpc.ErrInvalidParams("invalid command entry")))
			continue
		}
		nodeID := strParam(pm["node_id"])
		method := strParam(pm["method"])
		if nodeID == "" || method == "" {
			out = append(out, h.failedSpecCommand(nodeID, method, nil, 0,
				jsonrpc.ErrInvalidParams("node_id and method are required")))
			continue
		}
		timeout := intParam(pm["timeout"], defaultCommandTimeoutMs)
		if timeout < 0 {
			timeout = defaultCommandTimeoutMs
		}
		params, _ := pm["params"].(map[string]any)
		out = append(out, h.sendSync(nodeID, method, params, timeout, nil))
	}
	res := map[string]any{"commands": out, "task_id": nil}
	if ik != "" {
		h.cmds.storeBatchResult(ik, res)
	}
	return res, nil
}

// cmdCancel implements command.cancel; terminal commands are rejected -32014.
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
	if isTerminal(c.Status) {
		return nil, jsonrpc.NewError(jsonrpc.CodeCommandCancelRej, "command.cancel.rejected",
			map[string]any{"command_id": id, "status": c.Status})
	}
	h.cmds.cancel(id)
	c, _ = h.cmds.get(id)
	return map[string]any{"command": c}, nil
}

// ---------------------------------------------------------------------------
// Execution

func (h *Hub) newCommand(nodeID, method string, params map[string]any, timeout int, taskID *string) *Command {
	return &Command{
		ID:        fmt.Sprintf("c-%d", h.cmdSeq.Add(1)),
		NodeID:    nodeID,
		Method:    method,
		Params:    params,
		Status:    cmdAccepted,
		Timeout:   timeout,
		CreatedAt: nowTS(),
		TaskID:    taskID,
		cancelCh:  make(chan struct{}),
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
func (h *Hub) sendSync(nodeID, method string, params map[string]any, timeout int, taskID *string) *Command {
	if !h.nodes.tryAcquire(nodeID) {
		code, msg, data := h.nodeBusyOrOfflineCode(nodeID)
		c := h.newCommand(nodeID, method, params, timeout, taskID)
		h.cmds.create(c, "")
		h.cmds.finish(c.ID, cmdFailed, nil, jsonrpc.NewError(code, msg, data))
		return h.cmds.snapshot(c.ID)
	}
	defer h.nodes.release(nodeID)
	cmd := h.newCommand(nodeID, method, params, timeout, taskID)
	h.cmds.create(cmd, "")
	h.publishCommandAccepted(cmd)
	return h.runCommand(cmd)
}

// failedSpecCommand builds a command that failed before touching any node
// (malformed batch entries).
func (h *Hub) failedSpecCommand(nodeID, method string, params map[string]any, timeout int, rerr *jsonrpc.Error) *Command {
	c := h.newCommand(nodeID, method, params, timeout, nil)
	h.cmds.create(c, "")
	h.cmds.finish(c.ID, cmdFailed, nil, rerr)
	return h.cmds.snapshot(c.ID)
}

func (h *Hub) nodeBusyOrOfflineError(id string) *jsonrpc.Error {
	code, msg, data := h.nodeBusyOrOfflineCode(id)
	return jsonrpc.NewError(code, msg, data)
}

// nodeBusyOrOfflineCode resolves the error for a node that cannot accept a
// command: missing -> -32010 node.not_found, offline -> -32011 node.offline,
// busy -> -32012 node.busy (PROTOCOL.md §7.2).
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
		ms := intParam(cmd.Params["ms"], 0)
		select {
		case <-time.After(time.Duration(ms) * time.Millisecond):
			return cmdSucceeded, map[string]any{"slept": ms}, nil
		case <-ctx.Done():
			return h.abortedStatus(cmd), nil, nil
		}
	case "shell":
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

// execShell runs params.cmd via os/exec (string -> sh -c, array -> argv),
// killing the process on cancellation or timeout (CommandContext).
func (h *Hub) execShell(ctx context.Context, cmd *Command) (string, any, *jsonrpc.Error) {
	var c *exec.Cmd
	var stdout, stderr bytes.Buffer
	switch v := cmd.Params["cmd"].(type) {
	case string:
		c = exec.CommandContext(ctx, "sh", "-c", v)
	case []any:
		args := make([]string, 0, len(v))
		for _, a := range v {
			if s, ok := a.(string); ok {
				args = append(args, s)
			}
		}
		if len(args) == 0 {
			return cmdFailed, nil, jsonrpc.NewError(jsonrpc.CodeInvalidParams, "invalid params: empty command array", nil)
		}
		c = exec.CommandContext(ctx, args[0], args[1:]...)
	default:
		return cmdFailed, nil, jsonrpc.NewError(jsonrpc.CodeInvalidParams, "invalid params: cmd must be a string or array", nil)
	}
	c.Stdout = &stdout
	c.Stderr = &stderr
	err := c.Run()
	if err != nil {
		switch {
		case ctx.Err() == context.DeadlineExceeded:
			return cmdTimeout, nil, jsonrpc.NewError(simulatorFailureCode, "command timeout", nil)
		case ctx.Err() == context.Canceled:
			return cmdCancelled, nil, nil
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

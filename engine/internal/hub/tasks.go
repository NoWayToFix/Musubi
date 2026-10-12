package hub

import (
	"sync"
	"time"

	"musubi/engine/internal/jsonrpc"
)

// Task statuses (PROTOCOL.md §4.3).
const (
	taskRunning   = "running"
	taskSucceeded = "succeeded"
	taskFailed    = "failed"
	taskCancelled = "cancelled"
	taskPartial   = "partial"
)

func isTaskTerminal(s string) bool {
	switch s {
	case taskSucceeded, taskFailed, taskCancelled, taskPartial:
		return true
	}
	return false
}

// Task is a collection of commands with aggregate status and progress
// (schema task.json).
type Task struct {
	ID         string         `json:"id"`
	Name       string         `json:"name"`
	Status     string         `json:"status"`
	Progress   float64        `json:"progress"`
	CommandIDs []string       `json:"command_ids"`
	Error      *jsonrpc.Error `json:"error,omitempty"`
	CreatedAt  string         `json:"created_at"`
	UpdatedAt  string         `json:"updated_at"`

	Parallel  bool       `json:"-"`
	commands  []*Command `json:"-"`
	cancelled bool       `json:"-"`
	// owner is the creator's identity (see callerOwner); empty for
	// anonymous-created tasks, which anyone may cancel.
	owner    string
	done     chan struct{}
	doneOnce sync.Once
}

type taskRegistry struct {
	hub   *Hub
	mu    sync.RWMutex
	tasks map[string]*Task
	order []string // insertion order for stable listing
}

func newTaskRegistry(h *Hub) *taskRegistry {
	return &taskRegistry{hub: h, tasks: map[string]*Task{}}
}

// ---------------------------------------------------------------------------
// Registry operations

func (r *taskRegistry) create(t *Task) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tasks[t.ID] = t
	r.order = append(r.order, t.ID)
}

func (r *taskRegistry) get(id string) (*Task, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.tasks[id]
	if !ok {
		return nil, false
	}
	return cloneTask(t), true
}

func (r *taskRegistry) snapshot(id string) *Task {
	t, _ := r.get(id)
	return t
}

func (r *taskRegistry) isCancelled(id string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.tasks[id]
	return ok && t.cancelled
}

func (r *taskRegistry) cancel(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if t, ok := r.tasks[id]; ok {
		t.cancelled = true
		t.UpdatedAt = nowTS()
	}
}

func (r *taskRegistry) doneCh(id string) <-chan struct{} {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if t, ok := r.tasks[id]; ok && t.done != nil {
		return t.done
	}
	ch := make(chan struct{})
	close(ch) // already gone: treat as done
	return ch
}

type taskFilter struct{ status string }

func (r *taskRegistry) list(f *taskFilter, limit, offset int) ([]*Task, int) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Task, 0)
	for _, id := range r.order {
		t := r.tasks[id]
		if f != nil && f.status != "" && t.Status != f.status {
			continue
		}
		out = append(out, cloneTask(t))
	}
	total := len(out)
	if offset >= len(out) {
		out = []*Task{}
	} else {
		out = out[offset:]
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, total
}

// progress recomputes progress = terminal / total and publishes task.progress.
func (r *taskRegistry) progress(id string) *Task {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.tasks[id]
	if !ok {
		return nil
	}
	st := r.hub.cmds.statuses(t.CommandIDs)
	var term int
	for _, s := range st {
		if isTerminal(s) {
			term++
		}
	}
	p := 0.0
	if len(t.CommandIDs) > 0 {
		p = float64(term) / float64(len(t.CommandIDs))
	}
	t.Progress = p
	t.UpdatedAt = nowTS()
	cl := cloneTask(t)
	r.hub.publishTaskProgress(t.ID, p)
	return cl
}

// finalize computes the aggregate terminal status once (idempotent):
//
//	succeeded - all commands succeeded (progress 1)
//	failed    - at least one failed/timeout and none succeeded
//	cancelled - all commands cancelled
//	partial   - mix of succeeded and failed/cancelled
func (r *taskRegistry) finalize(id string) *Task {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.tasks[id]
	if !ok {
		return nil
	}
	if isTaskTerminal(t.Status) {
		return cloneTask(t)
	}
	st := r.hub.cmds.statuses(t.CommandIDs)
	total := len(t.CommandIDs)
	var succ, failed, cancelled int
	for _, s := range st {
		switch s {
		case cmdSucceeded:
			succ++
		case cmdFailed, cmdTimeout:
			failed++
		case cmdCancelled:
			cancelled++
		}
	}
	var status string
	switch {
	case succ == total:
		status = taskSucceeded
	case failed > 0 && succ == 0 && cancelled == 0:
		status = taskFailed
	case cancelled == total:
		status = taskCancelled
	default:
		status = taskPartial
	}
	progress := 0.0
	if total > 0 {
		progress = float64(succ) / float64(total)
	}
	if status == taskSucceeded {
		progress = 1.0
	}
	t.Status = status
	t.Progress = progress
	t.UpdatedAt = nowTS()
	cl := cloneTask(t)
	r.hub.publishTaskUpdated(cl)
	r.hub.publishTaskTerminal(cl)
	t.doneOnce.Do(func() { close(t.done) })
	return cl
}

func cloneTask(t *Task) *Task {
	cp := &Task{
		ID:         t.ID,
		Name:       t.Name,
		Status:     t.Status,
		Progress:   t.Progress,
		CommandIDs: append([]string(nil), t.CommandIDs...),
		CreatedAt:  t.CreatedAt,
		UpdatedAt:  t.UpdatedAt,
		owner:      t.owner,
	}
	if t.Error != nil {
		e := *t.Error
		cp.Error = &e
	}
	// Internal fields must not be shared with live snapshots.
	cp.commands = nil
	cp.done = nil
	return cp
}

// ---------------------------------------------------------------------------
// task.* RPC handlers

// taskCreate implements task.create: validates commands, creates the task and
// starts execution immediately (serial by default, parallel when requested).
func (h *Hub) taskCreate(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	name := strParam(p["name"])
	rawCmds, ok := p["commands"].([]any)
	if !ok || len(rawCmds) == 0 {
		return nil, jsonrpc.ErrInvalidParams("commands array required")
	}
	parallel, _ := p["parallel"].(bool)

	var cmds []*Command
	var ids []string
	for _, rc := range rawCmds {
		pm, ok := rc.(map[string]any)
		if !ok {
			return nil, jsonrpc.ErrInvalidParams("invalid command entry")
		}
		nodeID := strParam(pm["node_id"])
		method := strParam(pm["method"])
		if nodeID == "" || method == "" {
			return nil, jsonrpc.ErrInvalidParams("node_id and method are required per command")
		}
		timeout, terr := msParam(pm["timeout"], defaultCommandTimeoutMs)
		if terr != nil {
			return nil, terr
		}
		params, _ := pm["params"].(map[string]any)
		cmds = append(cmds, h.newCommand(nodeID, method, params, timeout, nil))
		ids = append(ids, cmds[len(cmds)-1].ID)
	}
	owner := callerOwner(cc)
	for _, c := range cmds {
		c.owner = owner
	}
	t := &Task{
		ID:         randomID("t-"),
		Name:       name,
		Status:     taskRunning,
		CommandIDs: ids,
		CreatedAt:  nowTS(),
		UpdatedAt:  nowTS(),
		Parallel:   parallel,
		commands:   cmds,
		owner:      owner,
		done:       make(chan struct{}),
	}
	taskID := t.ID
	for _, c := range cmds {
		c.TaskID = &taskID
		h.cmds.create(c, "")
	}
	h.tasks.create(t)
	h.publishTaskCreated(t)
	go h.runTask(t)
	return map[string]any{"task": h.tasks.snapshot(t.ID)}, nil
}

func (h *Hub) taskGet(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	id := strParam(p["task_id"])
	if id == "" {
		return nil, jsonrpc.ErrInvalidParams("task_id required")
	}
	t, ok := h.tasks.get(id)
	if !ok {
		return nil, jsonrpc.NewError(jsonrpc.CodeTaskNotFound, "task.not_found",
			map[string]any{"task_id": id})
	}
	return map[string]any{"task": t}, nil
}

func (h *Hub) taskList(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	limit, offset := clampPage(intParam(p["limit"], 100), intParam(p["offset"], 0))
	var f *taskFilter
	if raw, ok := p["filter"].(map[string]any); ok {
		f = &taskFilter{status: strParam(raw["status"])}
	}
	tasks, total := h.tasks.list(f, limit, offset)
	return map[string]any{"tasks": tasks, "total": total}, nil
}

// taskCancel cancels all non-terminal commands and waits briefly for the
// runner to finalise. Terminal tasks are rejected -32021. A task created by
// another authenticated session is rejected -32004; anonymous-created tasks
// (empty owner) stay cancellable by anyone.
func (h *Hub) taskCancel(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	id := strParam(p["task_id"])
	if id == "" {
		return nil, jsonrpc.ErrInvalidParams("task_id required")
	}
	t, ok := h.tasks.get(id)
	if !ok {
		return nil, jsonrpc.NewError(jsonrpc.CodeTaskNotFound, "task.not_found",
			map[string]any{"task_id": id})
	}
	if !mayActOn(cc, t.owner) {
		return nil, errForbidden("task")
	}
	if isTaskTerminal(t.Status) {
		return nil, jsonrpc.NewError(jsonrpc.CodeTaskCancelRej, "task.cancel.rejected",
			map[string]any{"task_id": id, "status": t.Status})
	}
	for _, cid := range t.CommandIDs {
		h.cmds.cancel(cid)
	}
	h.tasks.cancel(id)
	select {
	case <-h.tasks.doneCh(id):
	case <-time.After(2 * time.Second):
	}
	t, _ = h.tasks.get(id)
	return map[string]any{"task": t}, nil
}

func (h *Hub) taskSubscribe(cc *CallCtx, p map[string]any) (any, *jsonrpc.Error) {
	return h.makeSubscription(cc, nil, "", strParam(p["task_id"]))
}

func (h *Hub) runTask(t *Task) {
	if t.Parallel {
		var wg sync.WaitGroup
		for _, c := range t.commands {
			wg.Add(1)
			go func(c *Command) {
				defer wg.Done()
				h.executeTaskCommand(t, c)
			}(c)
		}
		wg.Wait()
	} else {
		for _, c := range t.commands {
			if h.tasks.isCancelled(t.ID) {
				break
			}
			h.executeTaskCommand(t, c)
		}
	}
	h.tasks.finalize(t.ID)
}

// executeTaskCommand runs one task command to completion and updates progress.
func (h *Hub) executeTaskCommand(t *Task, c *Command) {
	if !h.nodes.tryAcquire(c.NodeID) {
		// Already terminal (cancelled before execution) or node unavailable.
		if snap, ok := h.cmds.get(c.ID); ok && isTerminal(snap.Status) {
			h.taskProgress(t.ID)
			return
		}
		code, msg, data := h.nodeBusyOrOfflineCode(c.NodeID)
		h.cmds.finish(c.ID, cmdFailed, nil, jsonrpc.NewError(code, msg, data))
		h.taskProgress(t.ID)
		return
	}
	defer h.nodes.release(c.NodeID)
	// Cancelled between the check and the acquire.
	if snap, ok := h.cmds.get(c.ID); ok && isTerminal(snap.Status) {
		h.taskProgress(t.ID)
		return
	}
	h.publishCommandAccepted(c)
	h.runCommand(c)
	h.taskProgress(t.ID)
}

func (h *Hub) taskProgress(id string) {
	h.tasks.progress(id)
}

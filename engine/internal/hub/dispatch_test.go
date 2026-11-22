package hub

import (
	"testing"
	"time"

	"musubi/engine/internal/jsonrpc"
)

func anonCC() *CallCtx { return &CallCtx{Session: &Session{Anonymous: true}} }

// realNodeHub registers w-1 as a real (worker) node next to the demo node.
func realNodeHub(t *testing.T) *Hub {
	t.Helper()
	h := newSeatHub(t)
	caps := []string{"echo"}
	if _, rerr := h.RegisterNode(RegisterRequest{ID: "w-1", Name: "w-1", Capabilities: &caps}); rerr != nil {
		t.Fatalf("RegisterNode failed: %+v", rerr)
	}
	return h
}

func sendTo(t *testing.T, h *Hub, nodeID, method string, params map[string]any, timeout int) *Command {
	t.Helper()
	p := map[string]any{"node_id": nodeID, "method": method}
	if params != nil {
		p["params"] = params
	}
	if timeout > 0 {
		p["timeout"] = timeout
	}
	res, rerr := h.cmdSend(anonCC(), p)
	if rerr != nil {
		t.Fatalf("command.send failed: %+v", rerr)
	}
	return res.(map[string]any)["command"].(*Command)
}

func pullOne(t *testing.T, h *Hub, nodeID string) *Command {
	t.Helper()
	res, rerr := h.cmdPull(anonCC(), map[string]any{"node_id": nodeID, "wait": 1000})
	if rerr != nil {
		t.Fatalf("command.pull failed: %+v", rerr)
	}
	cmds := res.(map[string]any)["commands"].([]*Command)
	if len(cmds) != 1 {
		t.Fatalf("pull returned %d commands, want 1", len(cmds))
	}
	return cmds[0]
}

func complete(t *testing.T, h *Hub, nodeID, cmdID, status string, result any) *Command {
	t.Helper()
	p := map[string]any{"node_id": nodeID, "command_id": cmdID, "status": status}
	if result != nil {
		p["result"] = result
	}
	res, rerr := h.cmdComplete(anonCC(), p)
	if rerr != nil {
		t.Fatalf("command.complete failed: %+v", rerr)
	}
	return res.(map[string]any)["command"].(*Command)
}

func TestRemoteDispatchPullComplete(t *testing.T) {
	h := realNodeHub(t)
	sent := sendTo(t, h, "w-1", "echo", map[string]any{"k": "v"}, 0)
	if sent.Status != cmdPending {
		t.Fatalf("sent status = %q, want pending", sent.Status)
	}

	claimed := pullOne(t, h, "w-1")
	if claimed.ID != sent.ID || claimed.Status != cmdRunning {
		t.Fatalf("claimed = %s/%s, want %s/running", claimed.ID, claimed.Status, sent.ID)
	}
	if st, _ := h.nodes.status("w-1"); st != StatusBusy {
		t.Fatalf("node status after pull = %q, want busy", st)
	}

	done := complete(t, h, "w-1", sent.ID, cmdSucceeded, map[string]any{"echoed": true})
	if done.Status != cmdSucceeded || done.Result == nil {
		t.Fatalf("completed = %+v, want succeeded with result", done)
	}
	if st, _ := h.nodes.status("w-1"); st != StatusOnline {
		t.Fatalf("node status after complete = %q, want online", st)
	}
	// Idempotent replay.
	again := complete(t, h, "w-1", sent.ID, cmdFailed, nil)
	if again.Status != cmdSucceeded {
		t.Fatalf("second complete changed status to %q", again.Status)
	}
}

func TestRemoteDispatchPullLongPoll(t *testing.T) {
	h := realNodeHub(t)
	got := make(chan []*Command, 1)
	go func() {
		res, rerr := h.cmdPull(anonCC(), map[string]any{"node_id": "w-1", "wait": 2000})
		if rerr != nil {
			got <- nil
			return
		}
		got <- res.(map[string]any)["commands"].([]*Command)
	}()
	time.Sleep(30 * time.Millisecond)
	sendTo(t, h, "w-1", "echo", nil, 0)
	select {
	case cmds := <-got:
		if len(cmds) != 1 {
			t.Fatalf("long poll returned %d commands, want 1", len(cmds))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("long poll was not woken by command.send")
	}
}

func TestRemoteDispatchTimeout(t *testing.T) {
	h := realNodeHub(t)
	sent := sendTo(t, h, "w-1", "echo", nil, 40)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if snap, ok := h.cmds.get(sent.ID); ok && isTerminal(snap.Status) {
			if snap.Status != cmdTimeout {
				t.Fatalf("timed-out command status = %q, want timeout", snap.Status)
			}
			if res, _ := h.cmdPull(anonCC(), map[string]any{"node_id": "w-1"}); len(res.(map[string]any)["commands"].([]*Command)) != 0 {
				t.Fatal("timed-out command still queued")
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("command did not time out")
}

func TestRemoteDispatchCancelWhileQueued(t *testing.T) {
	h := realNodeHub(t)
	sent := sendTo(t, h, "w-1", "echo", nil, 0)
	if _, rerr := h.cmdCancel(anonCC(), map[string]any{"command_id": sent.ID}); rerr != nil {
		t.Fatalf("command.cancel failed: %+v", rerr)
	}
	snap, _ := h.cmds.get(sent.ID)
	if snap.Status != cmdCancelled {
		t.Fatalf("status = %q, want cancelled", snap.Status)
	}
	res, _ := h.cmdPull(anonCC(), map[string]any{"node_id": "w-1"})
	if cmds := res.(map[string]any)["commands"].([]*Command); len(cmds) != 0 {
		t.Fatalf("cancelled command still pullable: %+v", cmds)
	}
}

func TestRemoteDispatchNodeUnregisterFailsQueued(t *testing.T) {
	h := realNodeHub(t)
	sent := sendTo(t, h, "w-1", "echo", nil, 0)
	claimed := pullOne(t, h, "w-1")
	if _, rerr := h.nodeUnregister(anonCC(), map[string]any{"node_id": "w-1"}); rerr != nil {
		t.Fatalf("node.unregister failed: %+v", rerr)
	}
	snap, _ := h.cmds.get(sent.ID)
	if snap.Status != cmdFailed || snap.Error == nil || snap.Error.Code != jsonrpc.CodeNodeOffline {
		t.Fatalf("queued/running command after unregister = %+v, want failed -32011", snap)
	}
	_ = claimed
}

func TestRemoteTaskCompletesAfterPull(t *testing.T) {
	h := realNodeHub(t)
	res, rerr := h.taskCreate(anonCC(), map[string]any{
		"name":     "remote-task",
		"commands": []any{map[string]any{"node_id": "w-1", "method": "echo"}},
	})
	if rerr != nil {
		t.Fatalf("task.create failed: %+v", rerr)
	}
	task := res.(map[string]any)["task"].(*Task)

	claimed := pullOne(t, h, "w-1")
	complete(t, h, "w-1", claimed.ID, cmdSucceeded, map[string]any{"ok": true})
	final := waitTaskTerminal(t, h, task.ID)
	if final.Status != taskSucceeded || final.Progress != 1 {
		t.Fatalf("task = %s progress=%v, want succeeded/1", final.Status, final.Progress)
	}
}

func TestRemoteBatchWaitsForWorker(t *testing.T) {
	h := realNodeHub(t)
	go func() {
		res, rerr := h.cmdPull(anonCC(), map[string]any{"node_id": "w-1", "wait": 3000})
		if rerr != nil {
			return
		}
		for _, c := range res.(map[string]any)["commands"].([]*Command) {
			_, _ = h.cmdComplete(anonCC(), map[string]any{
				"node_id": "w-1", "command_id": c.ID, "status": cmdSucceeded, "result": map[string]any{"ok": true},
			})
		}
	}()
	res, rerr := h.cmdBatch(anonCC(), map[string]any{
		"commands": []any{map[string]any{"node_id": "w-1", "method": "echo"}},
	})
	if rerr != nil {
		t.Fatalf("command.batch failed: %+v", rerr)
	}
	cmd := res.(map[string]any)["commands"].([]any)[0].(*Command)
	if cmd.Status != cmdSucceeded {
		t.Fatalf("batch command status = %q, want succeeded", cmd.Status)
	}
}

func TestRemoteDispatchOwnership(t *testing.T) {
	h := newSeatHub(t)
	caps := []string{"echo"}
	if _, rerr := h.RegisterNodeOwned(RegisterRequest{ID: "w-owned", Capabilities: &caps}, "owner-A"); rerr != nil {
		t.Fatalf("register failed: %+v", rerr)
	}
	ownerB := &CallCtx{Session: &Session{Token: &Token{AccessToken: "owner-B"}}}
	_, rerr := h.cmdPull(ownerB, map[string]any{"node_id": "w-owned"})
	expectErrCode(t, rerr, jsonrpc.CodeAuthForbidden, "pull by non-owner")

	ownerA := &CallCtx{Session: &Session{Token: &Token{AccessToken: "owner-A"}}}
	if _, rerr := h.cmdPull(ownerA, map[string]any{"node_id": "w-owned"}); rerr != nil {
		t.Fatalf("owner pull failed: %+v", rerr)
	}
}

func TestRemoteDispatchRejectsDemoNode(t *testing.T) {
	h := realNodeHub(t)
	_, rerr := h.cmdPull(anonCC(), map[string]any{"node_id": "n-1"})
	expectErrCode(t, rerr, jsonrpc.CodeInvalidParams, "pull on demo node")
}

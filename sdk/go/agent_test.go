package sdk

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// fakeHub is a minimal JSON-RPC peer implementing the agent lifecycle.
type fakeHub struct {
	mu           sync.Mutex
	firstCommand map[string]any
	registerCall map[string]any
	registers    int
	pulls        int
	completed    []CommandCompleteParams
	unregistered bool
	done         chan struct{}
	doneOnce     sync.Once
}

func (f *fakeHub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID     any            `json:"id"`
		Method string         `json:"method"`
		Params map[string]any `json:"params"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var result any
	switch req.Method {
	case "node.register":
		f.mu.Lock()
		f.registers++
		f.registerCall = req.Params
		f.mu.Unlock()
		result = map[string]any{"node": map[string]any{
			"id": "w-test", "name": req.Params["name"], "status": "online",
			"platform": req.Params["platform"],
		}}
	case "node.heartbeat":
		result = map[string]any{"node": map[string]any{"id": "w-test", "status": "online"}}
	case "command.pull":
		f.mu.Lock()
		f.pulls++
		first := f.pulls == 1
		cmd := f.firstCommand
		f.mu.Unlock()
		if first && cmd != nil {
			result = map[string]any{"commands": []any{cmd}}
		} else {
			result = map[string]any{"commands": []any{}}
		}
	case "command.complete":
		var p CommandCompleteParams
		b, _ := json.Marshal(req.Params)
		_ = json.Unmarshal(b, &p)
		f.mu.Lock()
		f.completed = append(f.completed, p)
		f.mu.Unlock()
		f.doneOnce.Do(func() { close(f.done) })
		result = map[string]any{"command": map[string]any{"id": p.CommandID, "status": p.Status}}
	case "node.unregister":
		f.mu.Lock()
		f.unregistered = true
		f.mu.Unlock()
		result = map[string]any{"node": map[string]any{"id": "w-test", "status": "offline"}}
	default:
		writeRPC(w, req.ID, nil, &HubError{Code: ErrMethodNotFound, Message: "method not found"})
		return
	}
	writeRPC(w, req.ID, result, nil)
}

func writeRPC(w http.ResponseWriter, id any, result any, rerr *HubError) {
	resp := map[string]any{"jsonrpc": "2.0", "id": id}
	if rerr != nil {
		resp["error"] = rerr
	} else {
		resp["result"] = result
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func runAgent(t *testing.T, f *fakeHub, opts AgentOptions) *Agent {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	opts.URL = srv.URL
	opts.Transport = "http"
	opts.PullWait = 100
	opts.Heartbeat = 50 * time.Millisecond
	a := NewAgent(opts)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- a.Run(ctx) }()

	select {
	case <-f.done:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("command was never completed")
	}
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("agent did not stop")
	}
	return a
}

func TestAgentLifecycle(t *testing.T) {
	f := &fakeHub{
		done: make(chan struct{}),
		firstCommand: map[string]any{
			"id": "c-1", "node_id": "w-test", "method": "echo",
			"params": map[string]any{"text": "hi"}, "status": "accepted", "timeout": 30000,
		},
	}
	runAgent(t, f, AgentOptions{NodeID: "w-test", Name: "tester"})

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.registers == 0 {
		t.Fatal("node.register was never called")
	}
	plat, _ := f.registerCall["platform"].(map[string]any)
	if plat["runtime"] != "go" || plat["os"] == "" || plat["agent_version"] != Version {
		t.Fatalf("unexpected platform descriptor: %#v", plat)
	}
	if len(f.completed) != 1 {
		t.Fatalf("completed %d commands, want 1", len(f.completed))
	}
	got := f.completed[0]
	if got.Status != "succeeded" || got.CommandID != "c-1" || got.NodeID != "w-test" {
		t.Fatalf("unexpected completion: %#v", got)
	}
	res, _ := got.Result.(map[string]any)
	if res["text"] != "hi" {
		t.Fatalf("unexpected result: %#v", got.Result)
	}
	if !f.unregistered {
		t.Fatal("node.unregister was never called on shutdown")
	}
}

func TestAgentUnknownMethodFailsWithMethodNotFound(t *testing.T) {
	f := &fakeHub{
		done: make(chan struct{}),
		firstCommand: map[string]any{
			"id": "c-2", "node_id": "w-test", "method": "no.such.method",
			"params": map[string]any{}, "status": "accepted", "timeout": 30000,
		},
	}
	runAgent(t, f, AgentOptions{NodeID: "w-test"})

	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.completed) != 1 {
		t.Fatalf("completed %d commands, want 1", len(f.completed))
	}
	got := f.completed[0]
	if got.Status != "failed" || got.Error == nil || got.Error.Code != ErrMethodNotFound {
		t.Fatalf("unexpected completion: %#v", got)
	}
}

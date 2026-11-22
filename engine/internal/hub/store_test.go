package hub

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func waitTaskTerminal(t *testing.T, h *Hub, id string) *Task {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if task, ok := h.tasks.get(id); ok && isTaskTerminal(task.Status) {
			return task
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("task %s did not reach a terminal state", id)
	return nil
}

// Seats, real nodes, tasks and terminal commands survive a restart; a standing
// seat comes back as a vacant shell because its occupant is gone.
func TestPersistenceRoundTrip(t *testing.T) {
	dir := t.TempDir()
	h := NewHub(Config{AuthMode: "none", DemoNodes: 1, DataDir: dir, Logger: discardLogger()})

	seat := createSeat(t, h, map[string]any{
		"name": "durable", "bind_policy": "manual",
		"expect": map[string]any{"tags": []string{"linux"}},
	})
	registerReal(t, h, "w-1", []string{"echo"}, []string{"linux"})
	if _, rerr := seatCall(t, h, "seat.bind", map[string]any{"seat_id": seat.ID, "node_id": "w-1"}); rerr != nil {
		t.Fatalf("seat.bind failed: %+v", rerr)
	}

	res, rerr := h.taskCreate(&CallCtx{Session: &Session{Anonymous: true}}, map[string]any{
		"name":     "durable-task",
		"commands": []any{map[string]any{"node_id": "n-1", "method": "echo", "params": map[string]any{"k": "v"}}},
	})
	if rerr != nil {
		t.Fatalf("task.create failed: %+v", rerr)
	}
	task := res.(map[string]any)["task"].(*Task)
	waitTaskTerminal(t, h, task.ID)
	h.Stop()

	if _, err := os.Stat(filepath.Join(dir, stateFileName)); err != nil {
		t.Fatalf("state file not written: %v", err)
	}

	h2 := NewHub(Config{AuthMode: "none", DemoNodes: 1, DataDir: dir, Logger: discardLogger()})

	restoredSeat, ok := h2.seats.get(seat.ID)
	if !ok {
		t.Fatalf("seat %s was not restored", seat.ID)
	}
	if restoredSeat.Kind != SeatKindVacant || restoredSeat.NodeID != nil {
		t.Fatalf("restored seat = %+v, want vacant shell with no occupant", restoredSeat)
	}
	if restoredSeat.Expect == nil || len(restoredSeat.Expect.Tags) != 1 {
		t.Fatalf("restored seat lost its expect profile: %+v", restoredSeat.Expect)
	}

	node, ok := h2.nodes.get("w-1")
	if !ok {
		t.Fatalf("real node w-1 was not restored")
	}
	if node.Status != StatusOffline {
		t.Fatalf("restored node status = %q, want offline until it re-registers", node.Status)
	}

	restoredTask, ok := h2.tasks.get(task.ID)
	if !ok {
		t.Fatalf("task %s was not restored", task.ID)
	}
	if restoredTask.Status != taskSucceeded {
		t.Fatalf("restored task status = %q, want succeeded", restoredTask.Status)
	}
	cmd := h2.cmds.snapshot(restoredTask.CommandIDs[0])
	if cmd == nil || cmd.Status != cmdSucceeded || cmd.Result == nil {
		t.Fatalf("restored command = %+v, want a succeeded command with its result", cmd)
	}
}

// Work that was in flight at shutdown is terminalised, never replayed.
func TestPersistenceInFlightWorkFailsClosed(t *testing.T) {
	dir := t.TempDir()
	h := NewHub(Config{AuthMode: "none", DemoNodes: 1, DataDir: dir, Logger: discardLogger()})
	cmd := h.newCommand("n-1", "echo", map[string]any{"k": "v"}, 0, nil)
	h.cmds.create(cmd, "")
	h.publishCommandAccepted(cmd) // accepted but never executed
	h.Stop()

	h2 := NewHub(Config{AuthMode: "none", DemoNodes: 1, DataDir: dir, Logger: discardLogger()})
	got := h2.cmds.snapshot(cmd.ID)
	if got == nil {
		t.Fatalf("command %s was not restored", cmd.ID)
	}
	if got.Status != cmdFailed || got.Error == nil || got.Error.Message != "command interrupted by engine restart" {
		t.Fatalf("restored in-flight command = %+v, want failed with restart error", got)
	}
}

// Issued tokens survive a restart (within their TTL) so resource ownership
// checks keep working.
func TestPersistenceKeepsUnexpiredTokens(t *testing.T) {
	dir := t.TempDir()
	h := NewHub(Config{AuthMode: "token", Token: "secret", DemoNodes: 0, DataDir: dir, Logger: discardLogger()})
	res, lerr := h.auth.login(map[string]any{"token": "secret"})
	if lerr != nil {
		t.Fatalf("auth.login failed: %+v", lerr)
	}
	access := res["access_token"].(string)
	h.Stop()

	h2 := NewHub(Config{AuthMode: "token", Token: "secret", DemoNodes: 0, DataDir: dir, Logger: discardLogger()})
	sess, aerr := h2.auth.authenticate(access)
	if aerr != nil || sess == nil || !sess.Authenticated {
		t.Fatalf("restored token not accepted: err=%+v sess=%+v", aerr, sess)
	}
}

// A memory-only hub must not touch the filesystem.
func TestPersistenceDisabledByDefault(t *testing.T) {
	h := NewHub(Config{AuthMode: "none", DemoNodes: 1, Logger: discardLogger()})
	if h.store != nil {
		t.Fatal("store initialised without DataDir")
	}
	h.markDirty()
	h.Stop() // must be a no-op
}

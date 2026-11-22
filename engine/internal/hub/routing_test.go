package hub

import (
	"testing"

	"musubi/engine/internal/jsonrpc"
)

// seatRoutedHub returns a hub with one demo node bound to a standing seat.
func seatRoutedHub(t *testing.T) (*Hub, string) {
	t.Helper()
	h := newSeatHub(t)
	s := createSeat(t, h, map[string]any{"name": "route-me", "bind_policy": "manual"})
	if _, rerr := seatCall(t, h, "seat.bind", map[string]any{"seat_id": s.ID, "node_id": "n-1"}); rerr != nil {
		t.Fatalf("seat.bind failed: %+v", rerr)
	}
	return h, s.ID
}

func TestCommandSendRoutesThroughStandingSeat(t *testing.T) {
	h, seatID := seatRoutedHub(t)
	res, rerr := h.cmdSend(&CallCtx{Session: &Session{Anonymous: true}},
		map[string]any{"seat_id": seatID, "method": "echo", "params": map[string]any{"k": "v"}})
	if rerr != nil {
		t.Fatalf("command.send by seat failed: %+v", rerr)
	}
	cmd := res.(map[string]any)["command"].(*Command)
	if cmd.NodeID != "n-1" {
		t.Fatalf("node_id = %q, want the seat occupant n-1", cmd.NodeID)
	}
	if cmd.SeatID == nil || *cmd.SeatID != seatID {
		t.Fatalf("seat_id = %v, want %q", cmd.SeatID, seatID)
	}
}

func TestCommandSendVacantSeatRejected(t *testing.T) {
	h := newSeatHub(t)
	s := createSeat(t, h, map[string]any{"name": "empty"})
	_, rerr := h.cmdSend(&CallCtx{Session: &Session{Anonymous: true}},
		map[string]any{"seat_id": s.ID, "method": "echo"})
	expectErrCode(t, rerr, jsonrpc.CodeSeatVacant, "command.send to vacant seat")
}

func TestCommandSendUnknownSeatRejected(t *testing.T) {
	h := newSeatHub(t)
	_, rerr := h.cmdSend(&CallCtx{Session: &Session{Anonymous: true}},
		map[string]any{"seat_id": "s-nope", "method": "echo"})
	expectErrCode(t, rerr, jsonrpc.CodeSeatNotFound, "command.send to unknown seat")
}

func TestCommandSendAddressingIsExclusive(t *testing.T) {
	h, seatID := seatRoutedHub(t)
	cc := &CallCtx{Session: &Session{Anonymous: true}}
	_, rerr := h.cmdSend(cc, map[string]any{"node_id": "n-1", "seat_id": seatID, "method": "echo"})
	expectErrCode(t, rerr, jsonrpc.CodeInvalidParams, "both node_id and seat_id")
	_, rerr = h.cmdSend(cc, map[string]any{"method": "echo"})
	expectErrCode(t, rerr, jsonrpc.CodeInvalidParams, "neither node_id nor seat_id")
}

func TestCommandBatchRoutesThroughSeat(t *testing.T) {
	h, seatID := seatRoutedHub(t)
	res, rerr := h.cmdBatch(&CallCtx{Session: &Session{Anonymous: true}}, map[string]any{
		"commands": []any{
			map[string]any{"seat_id": seatID, "method": "echo", "params": map[string]any{"n": 1}},
			map[string]any{"seat_id": "s-nope", "method": "echo"},
		},
	})
	if rerr != nil {
		t.Fatalf("command.batch failed: %+v", rerr)
	}
	cmds := res.(map[string]any)["commands"].([]any)
	first := cmds[0].(*Command)
	if first.NodeID != "n-1" || first.SeatID == nil || *first.SeatID != seatID {
		t.Fatalf("first command routed to %q seat %v, want n-1/%s", first.NodeID, first.SeatID, seatID)
	}
	second := cmds[1].(*Command)
	if second.Error == nil || second.Error.Code != jsonrpc.CodeSeatNotFound {
		t.Fatalf("second command error = %+v, want -32050 embedded", second.Error)
	}
}

func TestTaskCreateRoutesThroughSeat(t *testing.T) {
	h, seatID := seatRoutedHub(t)
	res, rerr := h.taskCreate(&CallCtx{Session: &Session{Anonymous: true}}, map[string]any{
		"name": "seat-task",
		"commands": []any{
			map[string]any{"seat_id": seatID, "method": "echo", "params": map[string]any{"n": 1}},
		},
	})
	if rerr != nil {
		t.Fatalf("task.create failed: %+v", rerr)
	}
	task := res.(map[string]any)["task"].(*Task)
	cmd := h.cmds.snapshot(task.CommandIDs[0])
	if cmd.NodeID != "n-1" || cmd.SeatID == nil || *cmd.SeatID != seatID {
		t.Fatalf("task command routed to %q seat %v, want n-1/%s", cmd.NodeID, cmd.SeatID, seatID)
	}
}

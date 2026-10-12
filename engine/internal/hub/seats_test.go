package hub

import (
	"encoding/json"
	"testing"

	"musubi/engine/internal/jsonrpc"
)

// seatCall invokes a seat/node handler directly (anonymous none-mode session).
func seatCall(t *testing.T, h *Hub, method string, params map[string]any) (any, *jsonrpc.Error) {
	t.Helper()
	cc := &CallCtx{Session: &Session{Anonymous: true}}
	var res any
	var rerr *jsonrpc.Error
	switch method {
	case "seat.create":
		res, rerr = h.seatCreate(cc, params)
	case "seat.get":
		res, rerr = h.seatGet(cc, params)
	case "seat.list":
		res, rerr = h.seatList(cc, params)
	case "seat.bind":
		res, rerr = h.seatBind(cc, params)
	case "seat.unbind":
		res, rerr = h.seatUnbind(cc, params)
	case "seat.delete":
		res, rerr = h.seatDelete(cc, params)
	case "node.get":
		res, rerr = h.nodeGet(cc, params)
	case "node.unregister":
		res, rerr = h.nodeUnregister(cc, params)
	default:
		t.Fatalf("unknown test method %s", method)
	}
	return res, rerr
}

func newSeatHub(t *testing.T) *Hub {
	t.Helper()
	return NewHub(Config{AuthMode: "none", DemoNodes: 1, Logger: discardLogger()})
}

// registerReal registers a real node (as POST /v1/register would) so it can
// take seats; demo nodes never do.
func registerReal(t *testing.T, h *Hub, id string, caps []string, tags []string) *Node {
	t.Helper()
	c := caps
	n, rerr := h.RegisterNode(RegisterRequest{ID: id, Name: id, Capabilities: &c, Tags: tags})
	if rerr != nil {
		t.Fatalf("RegisterNode(%s) failed: %+v", id, rerr)
	}
	return n
}

func expectErrCode(t *testing.T, err *jsonrpc.Error, want int, what string) {
	t.Helper()
	if err == nil || err.Code != want {
		t.Fatalf("%s: want error code %d, got %+v", what, want, err)
	}
}

// createSeat is a helper returning the created *Seat.
func createSeat(t *testing.T, h *Hub, params map[string]any) *Seat {
	t.Helper()
	res, rerr := seatCall(t, h, "seat.create", params)
	if rerr != nil {
		t.Fatalf("seat.create failed: %+v", rerr)
	}
	return res.(map[string]any)["seat"].(*Seat)
}

func getSeat(t *testing.T, h *Hub, sid string) *Seat {
	t.Helper()
	res, rerr := seatCall(t, h, "seat.get", map[string]any{"seat_id": sid})
	if rerr != nil {
		t.Fatalf("seat.get failed: %+v", rerr)
	}
	return res.(map[string]any)["seat"].(*Seat)
}

func TestSeatCreateDefaultsAndList(t *testing.T) {
	h := newSeatHub(t)
	s := createSeat(t, h, map[string]any{"name": "slot-a"})
	if s.Kind != SeatKindVacant {
		t.Fatalf("new seat kind = %v, want vacant", s.Kind)
	}
	if s.BindPolicy != SeatBindManual {
		t.Fatalf("default bind_policy = %v, want manual", s.BindPolicy)
	}
	if s.NodeID != nil {
		t.Fatalf("new seat node_id = %v, want nil", s.NodeID)
	}
	if len(s.ID) < 3 || s.ID[:2] != "s-" {
		t.Fatalf("seat id %q lacks s- prefix", s.ID)
	}
	res, rerr := seatCall(t, h, "seat.list", map[string]any{})
	if rerr != nil {
		t.Fatalf("seat.list failed: %+v", rerr)
	}
	if got := res.(map[string]any)["total"].(int); got != 1 {
		t.Fatalf("seat.list total = %d, want 1", got)
	}
}

func TestSeatManualBindUnbindLifecycle(t *testing.T) {
	h := newSeatHub(t)
	s := createSeat(t, h, map[string]any{})
	sid := s.ID
	registerReal(t, h, "n-real-1", []string{"shell.exec"}, []string{"linux"})

	res, rerr := seatCall(t, h, "seat.bind", map[string]any{"seat_id": sid, "node_id": "n-real-1"})
	if rerr != nil {
		t.Fatalf("seat.bind failed: %+v", rerr)
	}
	bound := res.(map[string]any)["seat"].(*Seat)
	if bound.Kind != SeatKindStanding || bound.NodeID == nil || *bound.NodeID != "n-real-1" {
		t.Fatalf("after bind: %+v", bound)
	}
	// Node mirrors seat_id.
	ng, _ := seatCall(t, h, "node.get", map[string]any{"node_id": "n-real-1"})
	n := ng.(map[string]any)["node"].(*Node)
	if n.SeatID == nil || *n.SeatID != sid {
		t.Fatalf("node.SeatID = %v, want %s", n.SeatID, sid)
	}
	// Rebinding an occupied seat fails with -32051.
	_, rerr = seatCall(t, h, "seat.bind", map[string]any{"seat_id": sid, "node_id": "n-real-1"})
	expectErrCode(t, rerr, jsonrpc.CodeSeatOccupied, "bind on occupied seat")

	// Deleting a standing seat fails; unbind returns the shell to vacant.
	_, rerr = seatCall(t, h, "seat.delete", map[string]any{"seat_id": sid})
	expectErrCode(t, rerr, jsonrpc.CodeSeatOccupied, "delete standing seat")

	res, rerr = seatCall(t, h, "seat.unbind", map[string]any{"seat_id": sid})
	if rerr != nil {
		t.Fatalf("seat.unbind failed: %+v", rerr)
	}
	if res.(map[string]any)["seat"].(*Seat).Kind != SeatKindVacant {
		t.Fatal("after unbind kind = standing, want vacant")
	}
	// Unbind is idempotent on a vacant seat.
	if _, rerr = seatCall(t, h, "seat.unbind", map[string]any{"seat_id": sid}); rerr != nil {
		t.Fatalf("idempotent unbind failed: %+v", rerr)
	}
	// Now deletion succeeds.
	if _, rerr = seatCall(t, h, "seat.delete", map[string]any{"seat_id": sid}); rerr != nil {
		t.Fatalf("seat.delete failed: %+v", rerr)
	}
	_, rerr = seatCall(t, h, "seat.get", map[string]any{"seat_id": sid})
	expectErrCode(t, rerr, jsonrpc.CodeSeatNotFound, "get deleted seat")
}

func TestSeatAutoFillOnRegistration(t *testing.T) {
	h := newSeatHub(t)
	s := createSeat(t, h, map[string]any{
		"name":        "auto-slot",
		"bind_policy": "auto",
		"expect":      map[string]any{"capabilities": []string{"shell.exec"}, "tags": []string{"linux"}},
	})
	sid := s.ID
	// A non-matching registration must not fill the seat.
	registerReal(t, h, "n-win", []string{"file.read"}, []string{"windows"})
	if got := getSeat(t, h, sid).Kind; got != SeatKindVacant {
		t.Fatalf("non-matching node filled an auto seat: %s", got)
	}
	// A matching registration fills it and mirrors seat_id on the node.
	registerReal(t, h, "n-lin", []string{"shell.exec", "file.read"}, []string{"linux", "x64"})
	seat := getSeat(t, h, sid)
	if seat.Kind != SeatKindStanding || seat.NodeID == nil || *seat.NodeID != "n-lin" {
		t.Fatalf("auto fill failed: %+v", seat)
	}
	ng, _ := seatCall(t, h, "node.get", map[string]any{"node_id": "n-lin"})
	n := ng.(map[string]any)["node"].(*Node)
	if n.SeatID == nil || *n.SeatID != sid {
		t.Fatalf("node.SeatID = %v, want %s", n.SeatID, sid)
	}
}

func TestSeatBindExpectMismatch(t *testing.T) {
	h := newSeatHub(t)
	s := createSeat(t, h, map[string]any{
		"expect": map[string]any{"capabilities": []string{"gpu.exec"}},
	})
	registerReal(t, h, "n-cpu", []string{"shell.exec"}, nil)
	_, rerr := seatCall(t, h, "seat.bind", map[string]any{"seat_id": s.ID, "node_id": "n-cpu"})
	expectErrCode(t, rerr, jsonrpc.CodeSeatExpectMismatch, "bind without required capability")
}

func TestSeatReleasesOnUnregisterAndRebinds(t *testing.T) {
	h := newSeatHub(t)
	s := createSeat(t, h, map[string]any{})
	sid := s.ID
	registerReal(t, h, "n-x", nil, nil)
	if _, rerr := seatCall(t, h, "seat.bind", map[string]any{"seat_id": sid, "node_id": "n-x"}); rerr != nil {
		t.Fatalf("seat.bind failed: %+v", rerr)
	}
	// Unregistering the occupant releases the shell, keeping the seat.
	if _, rerr := seatCall(t, h, "node.unregister", map[string]any{"node_id": "n-x"}); rerr != nil {
		t.Fatalf("node.unregister failed: %+v", rerr)
	}
	seat := getSeat(t, h, sid)
	if seat.Kind != SeatKindVacant || seat.NodeID != nil {
		t.Fatalf("seat after unregister = %+v, want vacant shell", seat)
	}
	// The shell can be filled again by a new node.
	registerReal(t, h, "n-y", nil, nil)
	if _, rerr := seatCall(t, h, "seat.bind", map[string]any{"seat_id": sid, "node_id": "n-y"}); rerr != nil {
		t.Fatalf("re-bind failed: %+v", rerr)
	}
}

func TestSeatOneSeatPerNodeAutoReleaseOld(t *testing.T) {
	h := newSeatHub(t)
	s1 := createSeat(t, h, map[string]any{"name": "one"})
	s2 := createSeat(t, h, map[string]any{"name": "two"})
	registerReal(t, h, "n-m", nil, nil)
	if _, rerr := seatCall(t, h, "seat.bind", map[string]any{"seat_id": s1.ID, "node_id": "n-m"}); rerr != nil {
		t.Fatalf("seat.bind failed: %+v", rerr)
	}
	// Binding the same node to another seat auto-releases the old seat.
	if _, rerr := seatCall(t, h, "seat.bind", map[string]any{"seat_id": s2.ID, "node_id": "n-m"}); rerr != nil {
		t.Fatalf("seat.bind failed: %+v", rerr)
	}
	if got := getSeat(t, h, s1.ID).Kind; got != SeatKindVacant {
		t.Fatalf("old seat was not auto-released on rebind: %s", got)
	}
}

func TestSeatOwnershipEnforced(t *testing.T) {
	h := NewHub(Config{AuthMode: "token", Token: "secret-token", DemoNodes: 0, Logger: discardLogger()})
	sess := loginSession(t, h)
	cc := &CallCtx{Session: sess}
	res, rerr := h.seatCreate(cc, map[string]any{})
	if rerr != nil {
		t.Fatalf("seat.create failed: %+v", rerr)
	}
	sid := res.(map[string]any)["seat"].(*Seat).ID
	// Anonymous caller may not bind/unbind/delete an owned seat.
	anon := &CallCtx{Session: &Session{Anonymous: true}}
	_, rerr = h.seatBind(anon, map[string]any{"seat_id": sid, "node_id": "n-1"})
	expectErrCode(t, rerr, jsonrpc.CodeAuthForbidden, "anonymous bind on owned seat")
	_, rerr = h.seatUnbind(anon, map[string]any{"seat_id": sid})
	expectErrCode(t, rerr, jsonrpc.CodeAuthForbidden, "anonymous unbind on owned seat")
	_, rerr = h.seatDelete(anon, map[string]any{"seat_id": sid})
	expectErrCode(t, rerr, jsonrpc.CodeAuthForbidden, "anonymous delete on owned seat")
}

func TestSeatEventsPublished(t *testing.T) {
	h := newSeatHub(t)
	s := createSeat(t, h, map[string]any{})
	sid := s.ID
	registerReal(t, h, "n-ev", nil, nil)
	seatCall(t, h, "seat.bind", map[string]any{"seat_id": sid, "node_id": "n-ev"})
	seatCall(t, h, "seat.unbind", map[string]any{"seat_id": sid})
	seatCall(t, h, "seat.delete", map[string]any{"seat_id": sid})

	seen := map[string]bool{}
	for _, ev := range h.events.history {
		seen[ev.Type] = true
	}
	for _, want := range []string{"seat.created", "seat.filled", "seat.released", "seat.deleted"} {
		if !seen[want] {
			t.Fatalf("missing event %s; seen=%v", want, keysOf(seen))
		}
	}
	// The seat object round-trips as valid JSON event data.
	for _, ev := range h.events.history {
		if ev.Type == "seat.filled" {
			var m map[string]any
			if err := json.Unmarshal(ev.Data, &m); err != nil {
				t.Fatalf("seat.filled data not an object: %v", err)
			}
		}
	}
}

func keysOf(m map[string]bool) []string {
	out := []string{}
	for k := range m {
		out = append(out, k)
	}
	return out
}

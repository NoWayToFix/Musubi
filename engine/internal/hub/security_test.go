package hub

import (
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"musubi/engine/internal/jsonrpc"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// tokenHub is a token-mode hub with one demo node for ownership tests.
func tokenHub() *Hub {
	return NewHub(Config{
		AuthMode:   "token",
		Token:      "secret-token",
		DemoNodes:  1,
		AllowShell: true,
		Logger:     discardLogger(),
	})
}

func loginSession(t *testing.T, h *Hub) *Session {
	t.Helper()
	res, lerr := h.auth.login(map[string]any{"token": "secret-token"})
	if lerr != nil {
		t.Fatalf("auth.login failed: %+v", lerr)
	}
	sess, aerr := h.auth.authenticate(res["access_token"].(string))
	if aerr != nil {
		t.Fatalf("authenticate failed: %+v", aerr)
	}
	return sess
}

// ---------------------------------------------------------------------------
// auth.refresh rotation

func TestRefreshRotatesBothTokensAndInvalidatesOldPair(t *testing.T) {
	h := tokenHub()
	res, lerr := h.auth.login(map[string]any{"token": "secret-token"})
	if lerr != nil {
		t.Fatalf("auth.login failed: %+v", lerr)
	}
	oldAccess := res["access_token"].(string)
	oldRefresh := res["refresh_token"].(string)

	r, rerr := h.auth.refresh(oldRefresh)
	if rerr != nil {
		t.Fatalf("auth.refresh failed: %+v", rerr)
	}
	newAccess := r["access_token"].(string)
	newRefresh := r["refresh_token"].(string)
	if newAccess == oldAccess {
		t.Fatal("auth.refresh returned the same access_token, want a new one")
	}
	if newRefresh == oldRefresh {
		t.Fatal("auth.refresh returned the same refresh_token, want a rotated one")
	}

	// The old pair must be dead the instant the new pair is issued.
	if _, ok := h.auth.lookup(oldAccess); ok {
		t.Fatal("old access token still resolves after refresh")
	}
	if _, aerr := h.auth.authenticate(oldAccess); aerr == nil || aerr.Code != jsonrpc.CodeAuthInvalid {
		t.Fatalf("old access token after refresh: err=%+v, want -32002", aerr)
	}
	if _, rerr := h.auth.refresh(oldRefresh); rerr == nil || rerr.Code != jsonrpc.CodeAuthInvalid {
		t.Fatalf("replayed old refresh token: err=%+v, want -32002", rerr)
	}

	// The new pair works for both authenticate and a further refresh.
	if _, aerr := h.auth.authenticate(newAccess); aerr != nil {
		t.Fatalf("new access token rejected: %+v", aerr)
	}
	if _, rerr := h.auth.refresh(newRefresh); rerr != nil {
		t.Fatalf("new refresh token rejected: %+v", rerr)
	}
}

// ---------------------------------------------------------------------------
// ownership

func TestOwnershipSecondSessionForbiddenOwnerAllowed(t *testing.T) {
	h := tokenHub()
	ccA := &CallCtx{Session: loginSession(t, h)}
	ccB := &CallCtx{Session: loginSession(t, h)}

	// -- subscription: B cannot unsubscribe A's subscription, A can.
	res, serr := h.eventSubscribe(ccA, map[string]any{
		"event_types": []any{"node.registered"},
	})
	if serr != nil {
		t.Fatalf("eventSubscribe failed: %+v", serr)
	}
	subID := res.(map[string]any)["subscription_id"].(string)
	if _, err := h.eventUnsubscribe(ccB, map[string]any{"subscription_id": subID}); err == nil ||
		err.Code != jsonrpc.CodeAuthForbidden {
		t.Fatalf("cross-session event.unsubscribe: err=%+v, want -32004 auth.forbidden", err)
	}
	if _, err := h.eventUnsubscribe(ccA, map[string]any{"subscription_id": subID}); err != nil {
		t.Fatalf("owner cannot unsubscribe own subscription: %+v", err)
	}

	// -- command: B cannot cancel A's running command, A can.
	send, cerr := h.cmdSend(ccA, map[string]any{
		"node_id": "n-1", "method": "sleep",
		"params": map[string]any{"ms": json.Number("30000")},
	})
	if cerr != nil {
		t.Fatalf("command.send failed: %+v", cerr)
	}
	cmdID := send.(map[string]any)["command"].(*Command).ID
	if _, err := h.cmdCancel(ccB, map[string]any{"command_id": cmdID}); err == nil ||
		err.Code != jsonrpc.CodeAuthForbidden {
		t.Fatalf("cross-session command.cancel: err=%+v, want -32004 auth.forbidden", err)
	}
	if _, err := h.cmdCancel(ccA, map[string]any{"command_id": cmdID}); err != nil {
		t.Fatalf("owner cannot cancel own command: %+v", err)
	}

	// -- task: B cannot cancel A's task, A can.
	tc, terr := h.taskCreate(ccA, map[string]any{
		"name": "owned",
		"commands": []any{map[string]any{
			"node_id": "n-1", "method": "sleep",
			"params": map[string]any{"ms": json.Number("30000")},
		}},
	})
	if terr != nil {
		t.Fatalf("task.create failed: %+v", terr)
	}
	taskID := tc.(map[string]any)["task"].(*Task).ID
	if _, err := h.taskCancel(ccB, map[string]any{"task_id": taskID}); err == nil ||
		err.Code != jsonrpc.CodeAuthForbidden {
		t.Fatalf("cross-session task.cancel: err=%+v, want -32004 auth.forbidden", err)
	}
	if _, err := h.taskCancel(ccA, map[string]any{"task_id": taskID}); err != nil {
		t.Fatalf("owner cannot cancel own task: %+v", err)
	}
}

// Anonymous sessions (none mode) keep today's behavior: no owner, anyone may
// act on the resources.
func TestOwnershipAnonymousUnchanged(t *testing.T) {
	h := NewHub(Config{AuthMode: "none", DemoNodes: 1, Logger: discardLogger()})
	anon1 := &CallCtx{Session: &Session{Anonymous: true}}
	anon2 := &CallCtx{Session: &Session{Anonymous: true}}

	res, serr := h.eventSubscribe(anon1, nil)
	if serr != nil {
		t.Fatalf("eventSubscribe failed: %+v", serr)
	}
	subID := res.(map[string]any)["subscription_id"].(string)
	if _, err := h.eventUnsubscribe(anon2, map[string]any{"subscription_id": subID}); err != nil {
		t.Fatalf("anonymous cross-call unsubscribe must stay allowed: %+v", err)
	}

	send, cerr := h.cmdSend(anon1, map[string]any{
		"node_id": "n-1", "method": "sleep",
		"params": map[string]any{"ms": json.Number("30000")},
	})
	if cerr != nil {
		t.Fatalf("command.send failed: %+v", cerr)
	}
	cmdID := send.(map[string]any)["command"].(*Command).ID
	if _, err := h.cmdCancel(anon2, map[string]any{"command_id": cmdID}); err != nil {
		t.Fatalf("anonymous cross-call command.cancel must stay allowed: %+v", err)
	}
}

// Same-token access must keep working: the contract runner authenticates once
// and uses that token for every subsequent call.
func TestOwnershipSameTokenAllowed(t *testing.T) {
	h := tokenHub()
	cc := &CallCtx{Session: loginSession(t, h)}
	ccAgain := &CallCtx{Session: cc.Session}

	res, serr := h.eventSubscribe(cc, nil)
	if serr != nil {
		t.Fatalf("eventSubscribe failed: %+v", serr)
	}
	subID := res.(map[string]any)["subscription_id"].(string)
	if _, err := h.eventUnsubscribe(ccAgain, map[string]any{"subscription_id": subID}); err != nil {
		t.Fatalf("same-token unsubscribe rejected: %+v", err)
	}
}

// ---------------------------------------------------------------------------
// /v1/register demo-node shadowing

func TestRegisterCannotShadowDemoNode(t *testing.T) {
	h := NewHub(Config{DemoNodes: 1, Logger: discardLogger()})

	if _, nerr := h.RegisterNode(RegisterRequest{ID: "n-1", Name: "evil"}); nerr == nil ||
		nerr.Code != jsonrpc.CodeInvalidParams {
		t.Fatalf("registering demo id n-1: err=%+v, want -32602 invalid params", nerr)
	}
	if n, ok := h.nodes.get("n-1"); !ok || n.Name != "worker-1" || !n.demo {
		t.Fatalf("demo node was shadowed: node=%+v ok=%v", n, ok)
	}

	// Real nodes register and re-register (update) as before.
	if _, nerr := h.RegisterNode(RegisterRequest{ID: "n-9", Name: "real"}); nerr != nil {
		t.Fatalf("registering a real node failed: %+v", nerr)
	}
	n, nerr := h.RegisterNode(RegisterRequest{ID: "n-9", Name: "real-2"})
	if nerr != nil || n.Name != "real-2" {
		t.Fatalf("real re-registration must update: node=%+v err=%+v", n, nerr)
	}
}

// ---------------------------------------------------------------------------
// event_types validation

func TestEventSubscribeRejectsMalformedEventTypes(t *testing.T) {
	h := tokenHub()
	cc := &CallCtx{Session: loginSession(t, h)}

	for name, p := range map[string]map[string]any{
		"non-string entry": {"event_types": []any{"node.registered", 42}},
		"non-array value":  {"event_types": "node.registered"},
	} {
		if _, err := h.eventSubscribe(cc, p); err == nil || err.Code != jsonrpc.CodeInvalidParams {
			t.Fatalf("%s: err=%+v, want -32602", name, err)
		}
	}
	// A rejected filter must not leave a live allow-all subscription behind.
	if len(h.events.subs) != 0 {
		t.Fatalf("rejected subscribe created %d subscriptions, want 0", len(h.events.subs))
	}

	// A valid filter still subscribes.
	res, err := h.eventSubscribe(cc, map[string]any{"event_types": []any{"node.registered"}})
	if err != nil {
		t.Fatalf("valid event_types rejected: %+v", err)
	}
	if len(h.events.subs) != 1 {
		t.Fatalf("valid subscribe created %d subscriptions, want 1", len(h.events.subs))
	}
	sub := h.events.subs[res.(map[string]any)["subscription_id"].(string)]
	if !sub.EventTypes["node.registered"] || len(sub.EventTypes) != 1 {
		t.Fatalf("event_types filter not applied: %+v", sub.EventTypes)
	}
}

// ---------------------------------------------------------------------------
// millisecond overflow / truncation footguns

func TestMsParamClampsAndRejectsFloats(t *testing.T) {
	if v, err := msParam(json.Number("9223372036854775807"), 0); err != nil || v != maxMs {
		t.Fatalf("huge ms: got (%d, %+v), want (%d, nil)", v, err, maxMs)
	}
	if v, err := msParam(json.Number("-5"), 0); err != nil || v != 0 {
		t.Fatalf("negative ms: got (%d, %+v), want (0, nil)", v, err)
	}
	if v, err := msParam(nil, 30000); err != nil || v != 30000 {
		t.Fatalf("absent ms: got (%d, %+v), want default 30000", v, err)
	}
	if v, err := msParam(json.Number("0"), 30000); err != nil || v != 0 {
		t.Fatalf("integer 0: got (%d, %+v), want 0 (documented no-timeout)", v, err)
	}
	if _, err := msParam(json.Number("0.9"), 0); err == nil || err.Code != jsonrpc.CodeInvalidParams {
		t.Fatalf("float ms 0.9: err=%+v, want -32602", err)
	}
}

// Regression: sleep ms=9223372036854775807 used to overflow
// time.Duration(ms)*time.Millisecond into a negative duration and fire
// immediately. Clamped, it must keep sleeping until the command timeout.
func TestHugeSleepMsDoesNotFireImmediately(t *testing.T) {
	h := tokenHub()
	cc := &CallCtx{Session: loginSession(t, h)}

	res, err := h.cmdSend(cc, map[string]any{
		"node_id": "n-1", "method": "sleep",
		"params":  map[string]any{"ms": json.Number("9223372036854775807")},
		"timeout": json.Number("150"),
	})
	if err != nil {
		t.Fatalf("command.send failed: %+v", err)
	}
	id := res.(map[string]any)["command"].(*Command).ID

	deadline := time.Now().Add(3 * time.Second)
	for {
		c, _ := h.cmds.get(id)
		if c != nil && isTerminal(c.Status) {
			if c.Status != cmdTimeout {
				t.Fatalf("huge-ms sleep ended as %q (err=%+v), want %q: it fired before the 150ms timeout",
					c.Status, c.Error, cmdTimeout)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("huge-ms sleep neither terminated nor timed out within 3s")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Regression: timeout 0.9 truncated to 0 = "no timeout" (unbounded execution).
// It must be rejected; integer 0 keeps meaning "no timeout".
func TestCommandTimeoutFloatRejectedIntegerZeroAllowed(t *testing.T) {
	h := tokenHub()
	cc := &CallCtx{Session: loginSession(t, h)}

	if _, err := h.cmdSend(cc, map[string]any{
		"node_id": "n-1", "method": "echo", "timeout": json.Number("0.9"),
	}); err == nil || err.Code != jsonrpc.CodeInvalidParams {
		t.Fatalf("timeout 0.9: err=%+v, want -32602", err)
	}
	if _, err := h.cmdSend(cc, map[string]any{
		"node_id": "n-1", "method": "echo", "timeout": json.Number("0"),
	}); err != nil {
		t.Fatalf("integer timeout 0 (no timeout) rejected: %+v", err)
	}
}

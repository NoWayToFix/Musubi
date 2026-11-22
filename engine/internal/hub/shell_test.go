package hub

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"musubi/engine/internal/jsonrpc"
)

func testHub(allowShell bool) *Hub {
	cfg := Config{
		DemoNodes:  1,
		AllowShell: allowShell,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if allowShell {
		cfg.ShellAllowlist = []string{"echo", "sleep", "false", "true", "sh"}
	}
	return NewHub(cfg)
}

func shellCmd(h *Hub, argv ...string) *Command {
	args := make([]any, len(argv))
	for i, a := range argv {
		args[i] = a
	}
	return h.newCommand("n-1", "shell", map[string]any{"cmd": args}, 0, nil)
}

func errDataString(t *testing.T, rerr *jsonrpc.Error, key string) string {
	t.Helper()
	if rerr == nil || len(rerr.Data) == 0 {
		t.Fatalf("error/data missing: %+v", rerr)
	}
	var m map[string]any
	if err := json.Unmarshal(rerr.Data, &m); err != nil {
		t.Fatalf("error data is not an object: %v", err)
	}
	s, _ := m[key].(string)
	return s
}

// A stock hubd (no -allow-shell) must never run caller-supplied host commands.
func TestShellDisabledByDefault(t *testing.T) {
	h := testHub(false)
	status, _, rerr := h.execNodeCommand(context.Background(), shellCmd(h, "echo", "pwned"))
	if status != cmdFailed {
		t.Fatalf("status = %q, want %q", status, cmdFailed)
	}
	if rerr == nil || rerr.Code != jsonrpc.CodeFeatureDisabled {
		t.Fatalf("err = %+v, want code %d (feature.disabled)", rerr, jsonrpc.CodeFeatureDisabled)
	}
}

// -allow-shell alone is not enough: without an executable allowlist the
// feature stays disabled, so a single flag mistake cannot expose the host.
func TestShellRequiresAllowlist(t *testing.T) {
	h := NewHub(Config{
		DemoNodes:  1,
		AllowShell: true,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	status, _, rerr := h.execNodeCommand(context.Background(), shellCmd(h, "echo", "musubi"))
	if status != cmdFailed {
		t.Fatalf("status = %q, want %q", status, cmdFailed)
	}
	if rerr == nil || rerr.Code != jsonrpc.CodeFeatureDisabled {
		t.Fatalf("err = %+v, want feature.disabled", rerr)
	}
	if got := errDataString(t, rerr, "reason"); got != "allowlist_empty" {
		t.Fatalf("reason = %q, want allowlist_empty", got)
	}
}

func TestShellEnabledRunsCommand(t *testing.T) {
	h := testHub(true)
	status, result, rerr := h.execNodeCommand(context.Background(), shellCmd(h, "echo", "musubi"))
	if status != cmdSucceeded {
		t.Fatalf("status = %q (err=%+v), want succeeded", status, rerr)
	}
	res, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T, want map", result)
	}
	if !strings.Contains(strParam(res["stdout"]), "musubi") {
		t.Fatalf("stdout = %q, want it to contain %q", res["stdout"], "musubi")
	}
}

// A string command would be executed through "sh -c", whose script body the
// allowlist cannot inspect; it is rejected even when shell is enabled.
func TestShellRejectsStringForm(t *testing.T) {
	h := testHub(true)
	cmd := h.newCommand("n-1", "shell", map[string]any{"cmd": "echo musubi"}, 0, nil)
	status, _, rerr := h.execNodeCommand(context.Background(), cmd)
	if status != cmdFailed {
		t.Fatalf("status = %q, want %q", status, cmdFailed)
	}
	if rerr == nil || rerr.Code != jsonrpc.CodeInvalidParams {
		t.Fatalf("err = %+v, want -32602 invalid params", rerr)
	}
}

// Executables outside the allowlist are denied, including paths whose
// basename is not listed.
func TestShellRejectsUnlistedBinary(t *testing.T) {
	h := testHub(true)
	status, _, rerr := h.execNodeCommand(context.Background(), shellCmd(h, "/usr/bin/curl", "http://example.com"))
	if status != cmdFailed {
		t.Fatalf("status = %q, want %q", status, cmdFailed)
	}
	if rerr == nil || rerr.Code != jsonrpc.CodeFeatureDisabled {
		t.Fatalf("err = %+v, want feature.disabled", rerr)
	}
	if got := errDataString(t, rerr, "reason"); got != "command_not_allowed" {
		t.Fatalf("reason = %q, want command_not_allowed", got)
	}
}

// Regression: runCommand implements the deadline as WithCancel + AfterFunc, so
// ctx.Err() is context.Canceled and cannot identify a timeout. Without the
// abortedStatus discriminator this reported "cancelled".
//
// The command outlives its own cancellation on purpose: a child process may
// keep the inherited pipes open, so this also pins that a cancelled command
// returns promptly instead of blocking until the child exits.
func TestShellTimeoutReportedAsTimeout(t *testing.T) {
	h := testHub(true)
	cmd := shellCmd(h, "sleep", "30")
	ctx, cancel := context.WithCancel(context.Background())
	timer := time.AfterFunc(50*time.Millisecond, cancel)
	defer timer.Stop()

	start := time.Now()
	status, _, _ := h.execNodeCommand(ctx, cmd)
	elapsed := time.Since(start)
	if status != cmdTimeout {
		t.Fatalf("status = %q, want %q", status, cmdTimeout)
	}
	if elapsed > 10*time.Second {
		t.Fatalf("cancelled command took %s: the timeout did not bound execution", elapsed)
	}
}

func TestShellExplicitCancelReportedAsCancelled(t *testing.T) {
	h := testHub(true)
	cmd := shellCmd(h, "sleep", "30")
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(cmd.cancelCh) // what command.cancel does
		cancel()
	}()

	status, _, _ := h.execNodeCommand(ctx, cmd)
	if status != cmdCancelled {
		t.Fatalf("status = %q, want %q", status, cmdCancelled)
	}
}

// A non-cancellation failure must still be a failure, not a timeout/cancel.
func TestShellExitFailureReportedAsFailed(t *testing.T) {
	h := testHub(true)
	status, _, rerr := h.execNodeCommand(context.Background(), shellCmd(h, "false"))
	if status != cmdFailed {
		t.Fatalf("status = %q, want %q", status, cmdFailed)
	}
	if rerr == nil || rerr.Code != simulatorFailureCode {
		t.Fatalf("err = %+v, want code %d", rerr, simulatorFailureCode)
	}
}

// The demo executor's other methods must be unaffected by the shell gate.
func TestNonShellMethodsUnaffected(t *testing.T) {
	h := testHub(false)
	cmd := h.newCommand("n-1", "echo", map[string]any{"k": "v"}, 0, nil)
	status, result, rerr := h.execNodeCommand(context.Background(), cmd)
	if status != cmdSucceeded || rerr != nil {
		t.Fatalf("echo: status=%q err=%+v, want succeeded", status, rerr)
	}
	if m, ok := result.(map[string]any); !ok || m["k"] != "v" {
		t.Fatalf("echo result = %#v, want the params verbatim", result)
	}
}

package hub

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"musubi/engine/internal/jsonrpc"
)

func testHub(allowShell bool) *Hub {
	return NewHub(Config{
		DemoNodes:  1,
		AllowShell: allowShell,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
}

func shellCmd(h *Hub, script string) *Command {
	return h.newCommand("n-1", "shell", map[string]any{"cmd": script}, 0, nil)
}

// A stock hubd (no -allow-shell) must never run caller-supplied host commands.
func TestShellDisabledByDefault(t *testing.T) {
	h := testHub(false)
	status, _, rerr := h.execNodeCommand(context.Background(), shellCmd(h, "echo pwned > /tmp/pwned"))
	if status != cmdFailed {
		t.Fatalf("status = %q, want %q", status, cmdFailed)
	}
	if rerr == nil || rerr.Code != jsonrpc.CodeFeatureDisabled {
		t.Fatalf("err = %+v, want code %d (feature.disabled)", rerr, jsonrpc.CodeFeatureDisabled)
	}
}

func TestShellEnabledRunsCommand(t *testing.T) {
	h := testHub(true)
	status, result, rerr := h.execNodeCommand(context.Background(), shellCmd(h, "echo musubi"))
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

// Regression: runCommand implements the deadline as WithCancel + AfterFunc, so
// ctx.Err() is context.Canceled and cannot identify a timeout. Without the
// abortedStatus discriminator this reported "cancelled".
//
// The script outlives its own cancellation on purpose: the grandchild `sleep`
// inherits the pipes, so this also pins that a cancelled command actually
// returns promptly instead of blocking until the grandchild exits.
func TestShellTimeoutReportedAsTimeout(t *testing.T) {
	h := testHub(true)
	cmd := shellCmd(h, "sleep 30")
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
	cmd := shellCmd(h, "sleep 30")
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
	status, _, rerr := h.execNodeCommand(context.Background(), shellCmd(h, "exit 3"))
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

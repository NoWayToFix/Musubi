package sdk

// Tests for the offline write queue (PROTOCOL.md §6.2): writes that fail at
// transport level are buffered (bounded FIFO) and replayed in order once the
// connection works again.

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// A write that fails with a transport error is queued (ErrQueued surfaced)
// and replayed in order by the next successful call.
func TestOfflineQueueDrainsOnReconnect(t *testing.T) {
	var mu sync.Mutex
	var methods []string
	failFirstCommand := true

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/rpc", func(w http.ResponseWriter, r *http.Request) {
		rec := decodeRPC(r)
		mu.Lock()
		methods = append(methods, rec.Method)
		fail := rec.Method == "command.send" && failFirstCommand
		if fail {
			failFirstCommand = false
		}
		mu.Unlock()
		if fail {
			// Transport-level outage (body undecodable -> transport error).
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		writeRPCResult(w, rec.ID, map[string]any{"pong": true})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New(Options{URL: srv.URL, Transport: "http"})
	defer c.Close()

	// 1. Write while the transport is down: buffered, not executed.
	_, err := c.Call(context.Background(), "command.send", map[string]any{"node_id": "n1", "method": "x"})
	if !errors.Is(err, ErrQueued) {
		t.Fatalf("want ErrQueued, got %v", err)
	}
	c.mu.Lock()
	qlen := len(c.offline)
	c.mu.Unlock()
	if qlen != 1 {
		t.Fatalf("queue length = %d, want 1", qlen)
	}

	// 2. First successful call drains the queue FIFO before returning.
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := c.Call(context.Background(), "engine.ping", nil); err != nil {
			t.Errorf("engine.ping failed: %v", err)
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("flush did not complete")
	}

	mu.Lock()
	defer mu.Unlock()
	want := []string{"command.send", "engine.ping", "command.send"}
	if len(methods) != len(want) {
		t.Fatalf("server saw %v, want %v", methods, want)
	}
	for i := range want {
		if methods[i] != want[i] {
			t.Fatalf("server saw %v, want %v (replay must be in FIFO order)", methods, want)
		}
	}
}

// QueueWrite manually buffers and is bounded at offlineQueueLimit; engine.*
// is never eligible.
func TestQueueWriteBounded(t *testing.T) {
	// Unreachable URL: nothing here ever drains.
	c := New(Options{URL: "http://127.0.0.1:1", Transport: "http"})
	defer c.Close()

	for i := 0; i < offlineQueueLimit; i++ {
		if !c.QueueWrite("command.send", map[string]any{"i": i}) {
			t.Fatalf("enqueue %d refused before the limit", i)
		}
	}
	if c.QueueWrite("command.send", nil) {
		t.Fatalf("queue accepted more than %d items", offlineQueueLimit)
	}
	if c.QueueWrite("engine.ping", nil) {
		t.Fatal("engine.* calls must not be queued")
	}
	if c.QueueWrite("auth.login", nil) {
		t.Fatal("auth.* calls must not be queued")
	}

	// DisableOfflineQueue turns auto-buffering off: the transport error is
	// surfaced instead of ErrQueued.
	c2 := New(Options{URL: srvUnreachable(), Transport: "http", DisableOfflineQueue: true})
	defer c2.Close()
	_, err := c2.Call(context.Background(), "command.send", map[string]any{"node_id": "n1", "method": "x"})
	if err == nil || errors.Is(err, ErrQueued) {
		t.Fatalf("DisableOfflineQueue must surface the transport error, got %v", err)
	}
}

func srvUnreachable() string {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close() // grab a free port, then take it away
	return srv.URL
}

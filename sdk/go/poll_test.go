package sdk

// Tests for the long-poll loop: fatal auth statuses stop the loop and are
// surfaced, and the cursor resets to the server's initial value after a
// reconnect (engine restarts reset the in-memory cursor).

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// A persistent 401 is fatal: the loop stops, the error is surfaced via
// Options.OnError, and there is no infinite silent retry storm.
func TestPollLoopStopsOn401(t *testing.T) {
	var mu sync.Mutex
	hits := 0
	errCh := make(chan error, 8)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/poll", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
		w.WriteHeader(http.StatusUnauthorized)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New(Options{
		URL:           srv.URL,
		Transport:     "poll",
		Token:         "stale-token",
		ReconnectBase: 10 * time.Millisecond,
		OnError:       func(err error) { errCh <- err },
	})
	defer c.Close()

	sub, err := c.EventSubscribe(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	select {
	case err := <-errCh:
		if err == nil || !strings.Contains(err.Error(), "401") {
			t.Fatalf("unexpected surfaced error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("poll 401 was not surfaced via OnError")
	}

	// The loop must have stopped: no retry storm.
	time.Sleep(300 * time.Millisecond)
	mu.Lock()
	n := hits
	mu.Unlock()
	if n != 1 {
		t.Fatalf("poll endpoint hit %d times after fatal 401, want 1", n)
	}

	// A later EventSubscribe can restart the loop after re-auth.
	c.mu.Lock()
	cleared := c.poller == nil
	c.mu.Unlock()
	if !cleared {
		t.Fatal("poller was not cleared after the fatal error")
	}
}

// 5xx responses stay retryable (backoff), so the loop must keep polling.
func TestPollLoopRetriesOn5xx(t *testing.T) {
	var mu sync.Mutex
	hits := 0

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/poll", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
		w.WriteHeader(http.StatusInternalServerError)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New(Options{URL: srv.URL, Transport: "poll", ReconnectBase: 5 * time.Millisecond})
	defer c.Close()

	sub, err := c.EventSubscribe(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		n := hits
		mu.Unlock()
		if n >= 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("poll loop stopped retrying after 5xx (hits=%d)", n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// After a reconnect (polls failing, then succeeding again) the cursor resets
// to 0 so a reset server-side history (engine restart) is re-read instead of
// starving the client forever.
func TestPollCursorResetsOnReconnect(t *testing.T) {
	var mu sync.Mutex
	var cursors []int64
	step := 0

	ev := func(n int) map[string]any {
		return map[string]any{"id": "ev-" + strconv.Itoa(n), "type": "node.updated"}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/poll", func(w http.ResponseWriter, r *http.Request) {
		cur, _ := strconv.ParseInt(r.URL.Query().Get("cursor"), 10, 64)
		mu.Lock()
		cursors = append(cursors, cur)
		step++
		s := step
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch s {
		case 1, 3:
			w.WriteHeader(http.StatusInternalServerError) // network/server blip
		case 2:
			_ = json.NewEncoder(w).Encode(map[string]any{"cursor": 100, "events": []any{ev(1)}})
		case 4:
			// Simulate an engine restart: history/cursor was reset
			// server-side, so the response cursor REGRESSES below the
			// stale cursor the client still holds.
			_ = json.NewEncoder(w).Encode(map[string]any{"cursor": 2, "events": []any{ev(2)}})
		default:
			_ = json.NewEncoder(w).Encode(map[string]any{"cursor": 3, "events": []any{}})
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New(Options{URL: srv.URL, Transport: "poll", ReconnectBase: 5 * time.Millisecond})
	defer c.Close()

	sub, err := c.EventSubscribe(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	// Expected request cursors:
	//   req1 (init)  = 0    -> 500
	//   req2 (retry) = 0    -> success, cursor 100 (reconnect: reset before advancing)
	//   req3         = 100  -> 500
	//   req4 (retry) = 100  -> "engine restart": response cursor regresses to 2;
	//                          the reconnect reset lets the client adopt it
	//   req5         = 2    -> without the reset the stale 100 would be sent forever
	want := []int64{0, 0, 100, 100, 2}
	deadline := time.Now().Add(3 * time.Second)
	for {
		mu.Lock()
		got := append([]int64(nil), cursors...)
		mu.Unlock()
		if len(got) >= len(want) {
			for i, w := range want {
				if got[i] != w {
					t.Fatalf("cursor sequence = %v, want prefix %v", got, want)
				}
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out; cursor sequence so far = %v, want %v", got, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

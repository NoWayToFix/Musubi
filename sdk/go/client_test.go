package sdk

// Tests for the Call path: transport fallback classification (a server
// returned *HubError must never be resent over HTTP), the caller-configured
// WS timeout, and the minimal RFC6455 test server used by the suite.

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// WebSocket test server helpers (stdlib only; reuses the SDK frame codec)
// ---------------------------------------------------------------------------

// acceptWS performs the RFC6455 handshake on a hijacked connection and
// returns it together with the buffered reader positioned after the request.
func acceptWS(w http.ResponseWriter, r *http.Request) (net.Conn, *bufio.Reader, bool) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "no hijack support", http.StatusInternalServerError)
		return nil, nil, false
	}
	conn, rw, err := hj.Hijack()
	if err != nil {
		return nil, nil, false
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	sum := sha1.Sum([]byte(key + wsGUID))
	accept := base64.StdEncoding.EncodeToString(sum[:])
	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
	if _, err := conn.Write([]byte(resp)); err != nil {
		conn.Close()
		return nil, nil, false
	}
	return conn, rw.Reader, true
}

// wsTestHandler serves /v1/ws. handle receives each JSON-RPC request and
// returns the result object or a non-nil rpcErr for an error response.
// The server reuses the SDK's frame codec, so it emits masked frames (a real
// server emits unmasked); the client accepts both.
func wsTestHandler(handle func(method string, id int64) (any, *rpcTestErr)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, br, ok := acceptWS(w, r)
		if !ok {
			return
		}
		defer conn.Close()
		wsc := &WSConn{conn: conn, r: br}
		for {
			msg, err := wsc.ReadMessage()
			if err != nil {
				return
			}
			var env struct {
				ID     int64  `json:"id"`
				Method string `json:"method"`
			}
			if err := json.Unmarshal(msg, &env); err != nil {
				return
			}
			res, rerr := handle(env.Method, env.ID)
			out := map[string]any{"jsonrpc": "2.0", "id": env.ID}
			if rerr != nil {
				out["error"] = rerr
			} else {
				out["result"] = res
			}
			b, _ := json.Marshal(out)
			if err := wsc.WriteText(b); err != nil {
				return
			}
		}
	})
}

// wsDropHandler accepts the handshake and immediately closes the connection
// without answering anything: a pure transport-level failure.
func wsDropHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, ok := acceptWS(w, r)
		if !ok {
			return
		}
		conn.Close()
	})
}

// wsSilentHandler accepts the handshake and never answers a request; the
// hijacked connection stays open.
func wsSilentHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, ok := acceptWS(w, r)
		if !ok {
			return
		}
		// Keep the conn open until the client closes it.
		buf := make([]byte, 1)
		for {
			if _, err := conn.Read(buf); err != nil {
				return
			}
		}
	})
}

// rpcTestErr is a JSON-RPC error object served by the test servers.
type rpcTestErr struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func writeRPCResult(w http.ResponseWriter, id int64, result any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}

func writeRPCTestErr(w http.ResponseWriter, id int64, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"jsonrpc": "2.0", "id": id,
		"error": map[string]any{"code": code, "message": msg},
	})
}

type rpcRecord struct {
	ID     int64           `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

func decodeRPC(r *http.Request) rpcRecord {
	var rec rpcRecord
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return rec
	}
	_ = json.Unmarshal(body, &rec)
	return rec
}

func countString(xs []string, s string) int {
	n := 0
	for _, x := range xs {
		if x == s {
			n++
		}
	}
	return n
}

// ---------------------------------------------------------------------------
// Fallback classification
// ---------------------------------------------------------------------------

// A server-returned *HubError over WS is authoritative: the request was
// already executed, so the SDK must NOT resend it over HTTP.
func TestCallNoHTTPResendAfterHubError(t *testing.T) {
	var mu sync.Mutex
	var wsMethods []string
	rpcHits := 0

	mux := http.NewServeMux()
	mux.Handle("/v1/ws", wsTestHandler(func(method string, id int64) (any, *rpcTestErr) {
		mu.Lock()
		wsMethods = append(wsMethods, method)
		mu.Unlock()
		if method == "command.send" {
			return nil, &rpcTestErr{Code: ErrNodeOffline, Message: "node.offline"}
		}
		return map[string]any{"pong": true}, nil
	}))
	mux.HandleFunc("/v1/rpc", func(w http.ResponseWriter, r *http.Request) {
		rec := decodeRPC(r)
		mu.Lock()
		rpcHits++
		mu.Unlock()
		writeRPCResult(w, rec.ID, map[string]any{"pong": true})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New(Options{URL: srv.URL, Transport: "ws", MaxRetries: 1, ReconnectBase: 10 * time.Millisecond})
	defer c.Close()

	_, err := c.Call(context.Background(), "command.send", map[string]any{"node_id": "n1", "method": "x"})
	var he *HubError
	if !errors.As(err, &he) || he.Code != ErrNodeOffline {
		t.Fatalf("want HubError -32011, got %v", err)
	}
	if errors.Is(err, ErrQueued) {
		t.Fatal("HubError must not be queued offline")
	}
	// Give any (wrong) HTTP fallback plenty of time to show up.
	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if n := countString(wsMethods, "command.send"); n != 1 {
		t.Fatalf("server saw command.send %d times over WS, want exactly 1", n)
	}
	if rpcHits != 0 {
		t.Fatalf("HTTP fallback fired %d times after a server HubError, want 0", rpcHits)
	}
}

// A transport-level WS failure (connection dropped) still degrades to HTTP.
func TestCallResendsOverHTTPAfterTransportError(t *testing.T) {
	var mu sync.Mutex
	rpcHits := 0

	mux := http.NewServeMux()
	mux.Handle("/v1/ws", wsDropHandler())
	mux.HandleFunc("/v1/rpc", func(w http.ResponseWriter, r *http.Request) {
		rec := decodeRPC(r)
		mu.Lock()
		rpcHits++
		mu.Unlock()
		writeRPCResult(w, rec.ID, map[string]any{"pong": true})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New(Options{URL: srv.URL, Transport: "ws", MaxRetries: 1, ReconnectBase: 10 * time.Millisecond})
	defer c.Close()

	res, err := c.Call(context.Background(), "engine.ping", map[string]any{})
	if err != nil {
		t.Fatalf("call should succeed via HTTP fallback, got %v", err)
	}
	if res["pong"] != true {
		t.Fatalf("unexpected result %v", res)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		hits := rpcHits
		mu.Unlock()
		if hits >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("HTTP fallback never happened after WS transport failure")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A caller-configured timeout bounds WS calls too (not only HTTP calls): the
// WS read-deadline safety net is 90s, but the shorter option must win, and
// the timed-out WS call degrades to HTTP.
func TestCallWSAppliesConfiguredTimeout(t *testing.T) {
	var mu sync.Mutex
	rpcHits := 0

	mux := http.NewServeMux()
	mux.Handle("/v1/ws", wsSilentHandler()) // handshake OK, never responds
	mux.HandleFunc("/v1/rpc", func(w http.ResponseWriter, r *http.Request) {
		rec := decodeRPC(r)
		mu.Lock()
		rpcHits++
		mu.Unlock()
		writeRPCResult(w, rec.ID, map[string]any{"pong": true})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New(Options{URL: srv.URL, Transport: "ws", Timeout: 300 * time.Millisecond, MaxRetries: 1, ReconnectBase: 10 * time.Millisecond})
	defer c.Close()

	start := time.Now()
	if _, err := c.Call(context.Background(), "engine.ping", nil); err != nil {
		t.Fatalf("call should fall back to HTTP after the WS timeout, got %v", err)
	}
	if el := time.Since(start); el > 5*time.Second {
		t.Fatalf("call took %s; the 300ms WS timeout should have fired", el)
	}

	// Direct WS call surfaces the timeout error itself.
	_, err := c.callWS(context.Background(), "engine.ping", nil)
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("want ws timeout error, got %v", err)
	}
	if err != context.DeadlineExceeded && strings.Contains(err.Error(), "context") {
		t.Fatalf("timeout should be its own error, got %v", err)
	}
}

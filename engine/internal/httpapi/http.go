// Package httpapi exposes the Hub over the HTTP transports (PROTOCOL.md §6):
// JSON-RPC over POST, SSE, long-poll, health and node registration, plus the
// WebSocket endpoint at /v1/ws.
package httpapi

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"musubi/engine/internal/hub"
	"musubi/engine/internal/jsonrpc"
	"musubi/engine/internal/ws"
)

// API wires a Hub to HTTP handlers.
type API struct {
	hub *hub.Hub
	log *slog.Logger
}

// New creates the HTTP API layer for a Hub.
func New(h *hub.Hub, log *slog.Logger) *API {
	if log == nil {
		log = slog.Default()
	}
	return &API{hub: h, log: log}
}

// Handler returns the HTTP mux with all /v1 endpoints.
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/rpc", a.handleRPC)
	mux.HandleFunc("/v1/events", a.handleSSE)
	mux.HandleFunc("/v1/poll", a.handlePoll)
	mux.HandleFunc("/v1/health", a.handleHealth)
	mux.HandleFunc("/v1/register", a.handleRegister)
	mux.HandleFunc("/v1/ws", a.handleWS)
	return mux
}

// handleRPC serves POST /v1/rpc: JSON-RPC single envelope or batch.
func (a *API) handleRPC(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}
	out := a.hub.HandleRaw(body, r.Header.Get("Authorization"))
	if out == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(out)
}

// handleSSE serves GET /v1/events: server-pushed notifications as
// "data: <JSON-RPC notification>\n\n" with a 15s heartbeat comment line.
func (a *API) handleSSE(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	if _, aerr := a.authFromRequest(r); aerr != nil {
		http.Error(w, aerr.Message, http.StatusUnauthorized)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	fl.Flush()

	sub := a.hub.NewEventSub(nil, "", "")
	defer a.hub.CloseEventSub(sub)

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	ctx := r.Context()

	for {
		select {
		case ev, ok := <-sub.C():
			if !ok {
				return
			}
			msg := map[string]any{
				"jsonrpc": "2.0",
				"method":  "event",
				"params": map[string]any{
					"subscription_id": sub.ID,
					"event":           ev,
				},
			}
			b, err := json.Marshal(msg)
			if err != nil {
				return
			}
			if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
				return
			}
			fl.Flush()
		case <-heartbeat.C:
			if _, err := io.WriteString(w, ": heartbeat\n\n"); err != nil {
				return
			}
			fl.Flush()
		case <-ctx.Done():
			return
		}
	}
}

// handlePoll serves GET /v1/poll?cursor=N&timeout=30: long-polling over the
// event history (timeout max 60s, 0 = immediate).
func (a *API) handlePoll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if _, aerr := a.authFromRequest(r); aerr != nil {
		http.Error(w, aerr.Message, http.StatusUnauthorized)
		return
	}
	cursor, _ := strconv.ParseInt(r.URL.Query().Get("cursor"), 10, 64)
	if cursor < 0 {
		cursor = 0
	}
	timeout := 30
	if v := r.URL.Query().Get("timeout"); v != "" {
		if t, err := strconv.Atoi(v); err == nil {
			timeout = t
		}
	}
	if timeout < 0 {
		timeout = 0
	}
	if timeout > 60 {
		timeout = 60
	}
	ncursor, events, _ := a.hub.PollEvents(r.Context(), cursor, time.Duration(timeout)*time.Second)
	writeJSON(w, http.StatusOK, map[string]any{"cursor": ncursor, "events": events})
}

// handleHealth serves GET /v1/health (never requires authentication).
func (a *API) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, a.hub.Health())
}

// handleRegister serves POST /v1/register: real-node registration (online on
// arrival). Body: {id?, name?, capabilities?, tags?, metadata?}.
func (a *API) handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if _, aerr := a.hub.ResolveHeaderAuth(r.Header.Get("Authorization")); aerr != nil {
		writeJSON(w, http.StatusUnauthorized, jsonrpc.ErrorResponse(nil, aerr))
		return
	}
	var reg hub.RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&reg); err != nil {
		writeJSON(w, http.StatusBadRequest, jsonrpc.ErrorResponse(nil, jsonrpc.ErrParse()))
		return
	}
	n := a.hub.RegisterNode(reg)
	writeJSON(w, http.StatusOK, n)
}

// handleWS serves GET /v1/ws: the WebSocket transport. Each text frame is a
// JSON-RPC message; binary frames are rejected with close 1003; ping/pong and
// close are handled at the protocol layer.
func (a *API) handleWS(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	conn, err := ws.Upgrade(w, r)
	if err != nil {
		return // Upgrade already wrote the error response
	}
	defer conn.Close()

	authHeader := r.Header.Get("Authorization")
	var subs []string
	conn.KeepAlive(30*time.Second, 10*time.Second)

	for {
		op, payload, err := conn.ReadMessage()
		if err != nil {
			break
		}
		switch op {
		case ws.OpText:
			cc := &hub.CallCtx{
				TrackSub: func(id string) {
					subs = append(subs, id)
					// Forward the subscription's events to this socket as
					// notifications until either side closes (PROTOCOL §5.2).
					if sub := a.hub.GetEventSubByID(id); sub != nil {
						go pumpWSSub(conn, sub)
					}
				},
			}
			out := a.hub.HandleRawCtx(payload, authHeader, cc)
			if len(out) > 0 {
				if err := conn.WriteMessage(ws.OpText, out); err != nil {
					return
				}
			}
		case ws.OpBinary:
			_ = conn.WriteClose(1003, "binary frames not supported")
			return
		case ws.OpPing:
			if err := conn.WriteMessage(ws.OpPong, payload); err != nil {
				return
			}
		case ws.OpPong:
			conn.NotePong()
		case ws.OpClose:
			_ = conn.WriteClose(0, "")
			return
		}
	}
	// Clean up subscriptions created on this connection.
	for _, id := range subs {
		a.hub.CloseEventSubByID(id)
	}
}

// pumpWSSub streams one subscription's events to a WebSocket connection as
// JSON-RPC notifications, exiting when the subscription is closed or the
// socket drops. WriteMessage serializes concurrent writers internally.
func pumpWSSub(conn *ws.Conn, sub *hub.Subscription) {
	for {
		select {
		case ev, ok := <-sub.C():
			if !ok {
				return
			}
			msg, err := json.Marshal(map[string]any{
				"jsonrpc": "2.0",
				"method":  "event",
				"params": map[string]any{
					"subscription_id": sub.ID,
					"event":           ev,
				},
			})
			if err != nil {
				return
			}
			if err := conn.WriteMessage(ws.OpText, msg); err != nil {
				return
			}
		case <-sub.Done():
			return
		case <-conn.Done():
			return
		}
	}
}

// authFromRequest resolves ?token= or Authorization: Bearer for SSE/poll.
func (a *API) authFromRequest(r *http.Request) (*hub.Session, *jsonrpc.Error) {
	token := r.URL.Query().Get("token")
	if token == "" {
		token = strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	}
	return a.hub.AuthenticateToken(token)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
